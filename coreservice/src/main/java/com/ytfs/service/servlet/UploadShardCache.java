package com.ytfs.service.servlet;

import java.nio.ByteBuffer;

public class UploadShardCache {

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

    public byte[] toByte() {
        ByteBuffer buf = ByteBuffer.allocate(44);
        buf.put(VHF);
        buf.putInt(res);
        buf.putInt(nodeid);
        buf.putInt(shardid);
        buf.flip();
        return buf.array();
    }

    public void fill(ByteBuffer buf) {
        this.VHF = new byte[32];
        buf.get(this.VHF);
        this.res = buf.getInt();
        this.nodeid = buf.getInt();
        this.shardid = buf.getInt();
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
     * @return the res
     */
    public int getRes() {
        return res;
    }

    /**
     * @param res the res to set
     */
    public void setRes(int res) {
        this.res = res;
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

    private byte[] VHF;//已上传的,未上传或失败的==null
    private int res;    //从存储节点返回的存储结果
    private int nodeid; //从存储节点返回节点ID 
    private int shardid;
}
