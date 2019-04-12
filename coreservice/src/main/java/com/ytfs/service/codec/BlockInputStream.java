package com.ytfs.service.codec;

import com.ytfs.service.Function;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

public class BlockInputStream extends InputStream {

    private final byte[] data;
    private int head = 0;
    private InputStream in;

    public BlockInputStream(byte[] data) {
        this.data = data;
        readHead();
    }

    public BlockInputStream(Block block) throws IOException {
        block.load();
        this.data = block.getData();
        readHead();
    }

    public BlockInputStream(String path) throws IOException {
        Block b = new Block(path);
        b.load();
        this.data = b.getData();
        readHead();
    }

    private void readHead() {
        head = (short) Function.bytes2Integer(data, 0, 2);
        if (head == 0) {
            in = new InflaterInputStream(new ByteArrayInputStream(data, 2, data.length - 2));
        } else if (head < 0) {
            in = new ByteArrayInputStream(data, 2, data.length - 2);
        } else {
            in = new InflaterInputStream(new ByteArrayInputStream(data, 2, data.length - 2 - head));
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (in == null) {
            throw new IOException("Stream closed");
        }
        if (in instanceof ByteArrayInputStream) {
            return in.skip(n);
        } else {
            long count = 0;
            long rc;
            while (true) {
                rc = in.skip(n - count);
                count = count + rc;
                if (count == n) {
                    return n;
                }
                if (in.available() == 0) {
                    in = new ByteArrayInputStream(data, data.length - head, head);
                    head = 0;
                    break;
                }
            }
            in.skip(n - count);
            return count;
        }
    }

    @Override
    public int read() throws IOException {
        InputStream input = in;
        if (input == null) {
            throw new IOException("Stream closed");
        }
        int r = in.read();
        if (r == -1) {
            if (head > 0) {
                in = new ByteArrayInputStream(data, data.length - head, head);
                head = 0;
                return in.read();
            }
        }
        return r;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        InputStream input = in;
        if (input == null) {
            throw new IOException("Stream closed");
        }
        int rc = in.read(b, off, len);
        if (rc == -1) {
            if (head > 0) {
                in = new ByteArrayInputStream(data, data.length - head, head);
                head = 0;
                return in.read(b, off, len);
            }
        }
        return rc;
    }

}
