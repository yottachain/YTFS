package com.ytfs.service.codec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class BlockReader {

    private final File blockDir;
    private final FileOutputStream out;
    private final byte[] buf = new byte[8192];

    public BlockReader(String path, String outpath) throws IOException {
        this.blockDir = new File(path);
        if (!blockDir.exists() || !blockDir.isDirectory()) {
            throw new FileNotFoundException();
        }
        File outFile = new File(outpath);
        this.out = new FileOutputStream(outFile);
    }

    public void read() throws IOException {
        int ii = 0;
        while (true) {
            File file = new File(blockDir, Integer.toString(ii));
            if (!file.exists()) {
                break;
            }
            BlockInputStream is = new BlockInputStream(file.getAbsolutePath());
            readBlock(is);
            ii++;
        }
        out.close();
    }

    public void readBlock(BlockInputStream is) throws IOException {
        int len;
        while ((len = is.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
    }

}
