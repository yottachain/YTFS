package com.ytfs.service.codec;

import java.io.ByteArrayInputStream;

public class BackableBytesInputSteam extends ByteArrayInputStream {

    public BackableBytesInputSteam(byte buf[]) {
        super(buf);
    }

    /**
     * 在进行数据块编码时,可能需要回读,ByteArrayInputStream不支持 skip(-n)
     *
     * @param n
     * @return
     */
    @Override
    public synchronized long skip(long n) {
        if (n < 0) {
            long backnum = n + pos;
            if (backnum >= 0) {
                pos = (int) backnum;
                return n;
            } else {
                backnum = pos;
                pos = 0;
                return backnum;
            }
        } else {
            return super.skip(n);
        }
    }
}
