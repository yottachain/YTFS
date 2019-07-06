package com.ytfs.service.packet;

import com.ytfs.service.codec.ObjectRefer;
import java.util.List;

public class QueryObjectMetaResp {

    private short[] blocknums;
    private long length;

    public QueryObjectMetaResp() {
    }

    public QueryObjectMetaResp(byte[] bs, long length) {
        this.length = length;
        if (bs == null) {
            blocknums = new short[0];
        } else {
            List<ObjectRefer> refers = ObjectRefer.parse(bs);
            int ii = 0;
            blocknums = new short[refers.size()];
            for (ObjectRefer refer : refers) {
                blocknums[ii++] = refer.getId();
            }
        }
    }

    /**
     * @return the blocknums
     */
    public short[] getBlocknums() {
        return blocknums;
    }

    /**
     * @param blocknums the blocknums to set
     */
    public void setBlocknums(short[] blocknums) {
        this.blocknums = blocknums;
    }

    /**
     * @return the length
     */
    public long getLength() {
        return length;
    }

    /**
     * @param length the length to set
     */
    public void setLength(long length) {
        this.length = length;
    }
}
