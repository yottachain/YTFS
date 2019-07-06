package com.ytfs.service.codec;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BackableBufferedInputSteam extends BufferedInputStream {

    public BackableBufferedInputSteam(InputStream in, int size) {
        super(in, size);
    }

    public BackableBufferedInputSteam(InputStream in) {
        super(in);
    }

    /**
     * 在进行数据块编码时,可能需要回读,BufferedInputStream不支持 skip(-n)
     *
     * @param n
     * @return
     * @throws IOException
     */
    @Override
    public synchronized long skip(long n) throws IOException {
        if (n < 0) {
            long backnum = n + pos;
            if (backnum >= 0) {
                pos = (int) backnum;
                return n;
            } else {
                backnum = in.skip(backnum - count) - pos + count;
                pos = 0;
                count = 0;
                return backnum;
            }
        } else {
            return super.skip(n);
        }
    }
}
