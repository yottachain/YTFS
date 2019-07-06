package com.ytfs.service.packet;

import org.bson.types.ObjectId;

public class UploadBlockInitReq {

    private byte[] VHP;//该数据块的明文SHA256摘要
    private ObjectId VNU; //upload id
    private int shardCount = 0;//需要返回存储分片的节点个数
    private short id;

    public UploadBlockInitReq() {
    }

    public UploadBlockInitReq(ObjectId VNU, byte[] VHP, int shardcount, short id) {
        this.VNU = VNU;
        this.VHP = VHP;
        this.id = id;
        this.shardCount = shardcount;
    }

    /**
     * @return the VHP
     */
    public byte[] getVHP() {
        return VHP;
    }

    /**
     * @param VHP the VHP to set
     */
    public final void setVHP(byte[] VHP) {
        this.VHP = VHP;
    }

    /**
     * @return the shardCount
     */
    public int getShardCount() {
        return shardCount;
    }

    /**
     * @param shardCount the shardCount to set
     */
    public final void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    /**
     * @return the VNU
     */
    public ObjectId getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public final void setVNU(ObjectId VNU) {
        this.VNU = VNU;
    }

    /**
     * @return the id
     */
    public short getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public final void setId(short id) {
        this.id = id;
    }
}
