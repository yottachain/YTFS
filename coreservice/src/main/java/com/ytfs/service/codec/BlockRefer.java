package com.ytfs.service.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BlockRefer {

    public static List<BlockRefer> parse(byte[] bs) {
        List<BlockRefer> ls = new ArrayList();
        int len = bs.length / 40;
        ByteBuffer buf = ByteBuffer.wrap(bs);
        for (int ii = 0; ii < len; ii++) {
            BlockRefer refer = new BlockRefer();
            refer.shardid = buf.getInt();
            refer.nodeid = buf.getInt();
            refer.VHF = new byte[32];
            buf.get(refer.VHF);
            ls.add(refer);
        }
        return ls;
    }

    public static byte[] merge(List<BlockRefer> ls) {
        ByteBuffer buf = ByteBuffer.allocate(40 * ls.size());
        for (BlockRefer refer : ls) {
            buf.putInt(refer.shardid);
            buf.putInt(refer.nodeid);
            buf.put(refer.VHF);
        }
        buf.flip();
        return buf.array();
    }

    /**
     * @return the shardid
     */
    public int getShardid() {
        return shardid;
    }

    /**
     * @param shardid the shardid to set
     */
    public void setShardid(int shardid) {
        this.shardid = shardid;
    }

    /**
     * @return the VHF
     */
    public byte[] getVHF() {
        return VHF;
    }

    /**
     * @param VHF the VHF to set
     */
    public void setVHF(byte[] VHF) {
        this.VHF = VHF;
    }

    /**
     * @return the nodeid
     */
    public int getNodeid() {
        return nodeid;
    }

    /**
     * @param nodeid the nodeid to set
     */
    public void setNodeid(int nodeid) {
        this.nodeid = nodeid;
    }

    private int shardid;  //序号
    private byte[] VHF;
    private int nodeid;

}
