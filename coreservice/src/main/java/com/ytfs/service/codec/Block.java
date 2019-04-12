package com.ytfs.service.codec;

import com.ytfs.service.UserConfig;
import static com.ytfs.service.UserConfig.Default_PND;
import static com.ytfs.service.UserConfig.Default_Shard_Size;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.bson.Document;

public class Block {

    private long originalSize;  //编码前长度
    private int realSize;  //实际长度
    private byte[] data = null;//数据块内容
    private String path = null;//存储在磁盘上的路径
    private byte[] VHP = null;//该数据块的明文SHA256摘要
    private byte[] KD = null;//去重用密钥KD

    public String toJson() {
        Document doc = new Document();
        doc.append("path", path);
        doc.append("originalSize", originalSize);
        doc.append("realSize", realSize);
        return doc.toJson();
    }

    public Block(Document doc) {
        this.path = doc.getString("path");
        this.originalSize = doc.getLong("originalSize");
        this.realSize = doc.getInteger("realSize");
    }

    public Block(byte[] data) {
        this.data = data;
        this.realSize = data.length;
    }

    public Block(String path) {
        this.path = path;
    }

    public void load() throws IOException {
        if (data == null) {
            File file = new File(path);
            InputStream is = new FileInputStream(file);
            int len = (int) file.length();
            this.data = new byte[len];
            try {
                is.read(data);
            } finally {
                is.close();
            }
            this.realSize = data.length;
        }
    }

    public void save(String path) throws IOException {
        if (data == null) {
            throw new IOException();
        }
        File file = new File(path);
        OutputStream os = new FileOutputStream(file);
        os.write(data);
        os.close();
        this.path = path;
    }

    /**
     * 计算VHP,KD的方式
     *
     * @throws IOException
     */
    public void calculate() throws IOException {
        load();
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bs = md5.digest(data);
            setVHP(sha256.digest(data));
            sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(data);
            setKD(sha256.digest(bs));
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * @return the VHP
     */
    public byte[] getVHP() {
        return VHP;
    }

    /**
     * @return the KD
     */
    public byte[] getKD() {
        return KD;
    }

    /**
     * @return the originalSize
     */
    public long getOriginalSize() {
        return originalSize;
    }

    /**
     * @param originalSize the originalSize to set
     */
    public void setOriginalSize(long originalSize) {
        this.originalSize = originalSize;
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @param VHP the VHP to set
     */
    public void setVHP(byte[] VHP) {
        this.VHP = VHP;
    }

    /**
     * @param KD the KD to set
     */
    public void setKD(byte[] KD) {
        this.KD = KD;
    }

    /**
     * @return the realSize
     */
    public int getRealSize() {
        return realSize;
    }

    /**
     * @param realSize the realSize to set
     */
    public void setRealSize(int realSize) {
        this.realSize = realSize;
    }
 
}
