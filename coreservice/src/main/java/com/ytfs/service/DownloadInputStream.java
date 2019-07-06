package com.ytfs.service;

import com.ytfs.service.codec.Block;
import com.ytfs.service.codec.BlockInputStream;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.packet.ServiceException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DownloadInputStream extends InputStream {

    private Map<Integer, ObjectRefer> refers = new HashMap();
    private final long end;
    private BlockInputStream bin;
    private long readpos;
    private long pos = 0;
    private int referIndex = 0;

    public DownloadInputStream(List<ObjectRefer> refs, long start, long end) {
        for (ObjectRefer refer : refs) {
            int id = refer.getId() & 0xFFFF;
            this.refers.put(id, refer);
        }
        this.readpos = start;
        this.end = end;
    }

    private void readBlock() throws IOException, ServiceException {
        bin = null;
        while (bin == null) {
            ObjectRefer refer = refers.get(referIndex);
            if (refer == null) {
                return;
            }
            pos = pos + refer.getOriginalSize();
            if (readpos < pos) {
                DownloadBlock db = new DownloadBlock(refer);
                db.load();
                Block block = new Block(db.getData());
                bin = new BlockInputStream(block);
                long skip = pos - readpos;
                if (skip > 0) {
                    bin.skip(skip);
                }
            }
            referIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        refers = null;
    }

    @Override
    public int read() throws IOException {
        if (refers == null) {
            throw new IOException("Stream closed");
        }
        if (end == readpos) {
            return -1;
        }
        try {
            if (bin == null) {
                readBlock();
                if (bin == null) {
                    return -1;
                }
            }
            int r = bin.read();
            if (r == -1) {
                readBlock();
                if (bin == null) {
                    return -1;
                } else {
                    r = bin.read();
                    if (r != -1) {
                        readpos++;
                    }
                }
            } else {
                readpos++;
            }
            return r;
        } catch (ServiceException se) {
            throw new IOException(se);
        }
    }

}
