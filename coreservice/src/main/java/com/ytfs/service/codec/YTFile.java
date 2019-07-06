package com.ytfs.service.codec;

import com.ytfs.service.Function;
import static com.ytfs.service.UserConfig.Compress_Reserve_Size;
import static com.ytfs.service.UserConfig.Default_Block_Size;
import static com.ytfs.service.UserConfig.Max_Memory_Usage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import org.bson.Document;

public class YTFile {

    private File blockDir = null;
    private File parent = null;
    private final InputStream is;
    private final byte[] buf = new byte[16];
    private final List<Block> blockList = new ArrayList();
    private long length;
    private byte[] VHW;
    private boolean finished = false;
    private boolean inMemory;

    public YTFile(byte[] bs) throws IOException {
        if (bs.length >= Max_Memory_Usage) {
            throw new IOException("");
        }
        ByteArrayInputStream bi = new ByteArrayInputStream(bs);
        digest(bi);
        is = new BackableBytesInputSteam(bs);
        inMemory = true;
        length = bs.length;
    }

    private void digest(InputStream is) throws IOException {
        byte[] data = new byte[8192];
        int len;
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            while ((len = is.read(data)) != -1) {
                sha256.update(data, 0, len);
            }
            this.VHW = sha256.digest();
        } catch (NoSuchAlgorithmException ex) {
            is.close();
            throw new IOException(ex);
        } finally {
            is.close();
        }
    }

    public YTFile(String path) throws IOException {
        File file = new File(path);
        digest(new FileInputStream(file));
        is = new BackableBufferedInputSteam(new FileInputStream(file), Default_Block_Size);
        length = file.length();
        if (file.length() >= Max_Memory_Usage) {
            inMemory = false;
            parent = file.getParentFile();
        } else {
            inMemory = true;
        }
    }

    public void init(String vnu) throws IOException {
        if (!inMemory) {
            blockDir = new File(parent, vnu);
            File end = new File(blockDir, "end");
            if (!end.exists()) {
                clear();
                int retry = 0;
                while (!blockDir.mkdir()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ex) {
                    }
                    if (retry++ > 10) {
                        throw new IOException();
                    }
                }
            } else {
                finished = true;
                readEnd(end);
            }
        }
    }

    public void clear() {
        if (inMemory == false && blockDir.exists()) {
            if (blockDir.isFile()) {
                blockDir.delete();
            } else {
                File[] files = blockDir.listFiles();
                for (File file : files) {
                    file.delete();
                }
                blockDir.delete();
            }
        }
    }

    public void handle() throws IOException {
        if (finished) {
            return;
        }
        while (!finished) {
            try {
                long readTotal = deflate(is);
                if (readTotal > 0) {
                    is.skip(readTotal * -1);
                    pack(is);
                }
            } catch (IOException e) {
                is.close();
                clear();
                throw e;
            }
        }
        writeEnd();
    }

    /**
     * 不压缩,打包
     *
     * @param is
     * @throws IOException
     */
    private void pack(InputStream is) throws IOException {
        byte[] data = new byte[Default_Block_Size];
        int len = -1;//不压缩,head置-1
        Function.short2bytes((short) len, data, 0);
        len = is.read(data, 2, data.length - 2);
        if (len < data.length - 2) {
            byte[] newdata = new byte[2 + len];
            System.arraycopy(data, 0, newdata, 0, newdata.length);
            write(newdata, len);
            finished = true;
        } else {
            write(data, Default_Block_Size - 2);
        }
    }

    /**
     * 压缩
     *
     * @param is
     * @return >0 压缩失败返回totalIn(读取了多少字节,需要撤回) 0 OK
     * @throws IOException
     */
    private long deflate(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(new byte[]{0, 0});
        DeflaterOutputStream dos = new DeflaterOutputStream(bos);
        long totalIn = 0;
        int len;
        while ((len = is.read(buf)) != -1) {
            dos.write(buf, 0, len);
            totalIn = totalIn + len;
            int remainSize = Default_Block_Size - bos.size();
            if (remainSize < 0) {
                return totalIn;
            }
            if (remainSize < Compress_Reserve_Size) {
                dos.close();
                if (totalIn - bos.size() <= 0) {
                    return totalIn;
                }
                remainSize = Default_Block_Size - bos.size();
                if (remainSize < 0) {//失败
                    return totalIn;
                } else {
                    byte[] bs = new byte[remainSize];
                    len = is.read(bs);
                    if (len == -1) {
                        byte[] data = bos.toByteArray();//后续无原文字节,head置0
                        write(data, totalIn);
                        finished = true;
                    } else {
                        totalIn = totalIn + len;
                        bos.write(bs, 0, len);
                        byte[] data = bos.toByteArray();//后续有原文字节,head置len
                        Function.short2bytes((short) len, data, 0);
                        write(data, totalIn);
                        if (len < remainSize) {
                            finished = true;
                        }
                    }
                    return 0;
                }
            }
        }
        dos.close();
        if (totalIn - bos.size() <= 0) {
            return totalIn; //没必要压缩
        }
        if (bos.size() > Default_Block_Size) {
            return totalIn; //失败
        }
        if (totalIn > 0) {
            byte[] data = bos.toByteArray();//后续无原文字节,head置0
            write(data, totalIn);
        }
        finished = true;
        return 0;
    }

    /**
     * 如果不超过最大内存使用量,不写磁盘
     *
     * @param data
     * @param originalSize
     */
    private void write(byte[] data, long originalSize) throws IOException {
        Block b = new Block(data);
        b.setOriginalSize(originalSize);
        if (!inMemory) {
            File f = new File(blockDir, Integer.toString(getBlockList().size()));
            FileOutputStream os = new FileOutputStream(f);
            try {
                os.write(data);
            } finally {
                os.close();
            }
            b.setPath(f.getAbsolutePath());
            b.setData(null);
        } else {
            b.setData(data);
        }
        getBlockList().add(b);
    }

    /**
     * 将block信息写入end文件
     *
     * @throws IOException
     */
    private void writeEnd() throws IOException {
        if (!inMemory) {
            File f = new File(blockDir, "end");
            BufferedWriter writer = new BufferedWriter(new FileWriter(f));
            try {
                for (Block b : blockList) {
                    writer.write(b.toJson() + "\r\n");
                }
            } finally {
                writer.close();
            }
        }
    }

    /**
     * 从end文件读取block信息
     *
     * @param f
     * @throws IOException
     */
    private void readEnd(File f) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(f));
        try {
            for (;;) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.isEmpty()) {
                    continue;
                }
                Document doc = Document.parse(line);
                Block b = new Block(doc);
                getBlockList().add(b);
            }
        } catch (Exception e) {
            throw e instanceof IOException ? (IOException) e : new IOException(e.getMessage());
        } finally {
            reader.close();
        }
    }

    /**
     * @return the blockList
     */
    public List<Block> getBlockList() {
        return blockList;
    }

    /**
     * @return the length
     */
    public long getLength() {
        return length;
    }

    /**
     * @return the VHW
     */
    public byte[] getVHW() {
        return VHW;
    }
}
