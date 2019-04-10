package com.ytfs.service.packet;

import com.ytfs.service.UserConfig;
import com.ytfs.service.codec.KeyStoreCoder;
import java.nio.ByteBuffer;
import java.security.Key;
import java.security.PrivateKey;
import java.security.Signature;

public class UploadShardReq {

    private int SHARDID; //分片索引
    private int BPDID;   //超级节点ID
    private long VBI;    //上传块生成的流水号
    private byte[] BPDSIGN;  //超级节点对节点ID和VBI的签名
    private byte[] DAT;      //分片数据
    private byte[] VHF;      //分片hash
    private byte[] USERSIGN; //用户对VHF和SHARDID,NODEID,VBI的签名

    public void sign(int nodeid) {
        Key key = KeyStoreCoder.rsaPrivateKey(UserConfig.KUSp);
        try {
            Signature signet = java.security.Signature.getInstance("DSA");
            signet.initSign((PrivateKey) key);
            ByteBuffer bs = ByteBuffer.allocate(48);
            bs.put(VHF);
            bs.putInt(SHARDID);
            bs.putInt(nodeid);
            bs.putLong(VBI);
            bs.flip();
            signet.update(bs.array());
            this.USERSIGN = signet.sign();
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the BPDID
     */
    public int getBPDID() {
        return BPDID;
    }

    /**
     * @param BPDID the BPDID to set
     */
    public void setBPDID(int BPDID) {
        this.BPDID = BPDID;
    }

    /**
     * @return the VBI
     */
    public long getVBI() {
        return VBI;
    }

    /**
     * @param VBI the VBI to set
     */
    public void setVBI(long VBI) {
        this.VBI = VBI;
    }

    /**
     * @return the BPDSIGN
     */
    public byte[] getBPDSIGN() {
        return BPDSIGN;
    }

    /**
     * @param BPDSIGN the BPDSIGN to set
     */
    public void setBPDSIGN(byte[] BPDSIGN) {
        this.BPDSIGN = BPDSIGN;
    }

    /**
     * @return the data
     */
    public byte[] getDAT() {
        return DAT;
    }

    /**
     * @param data the data to set
     */
    public void setDAT(byte[] data) {
        this.DAT = data;
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
     * @return the USERSIGN
     */
    public byte[] getUSERSIGN() {
        return USERSIGN;
    }

    /**
     * @param USERSIGN the USERSIGN to set
     */
    public void setUSERSIGN(byte[] USERSIGN) {
        this.USERSIGN = USERSIGN;
    }

    /**
     * @return the SHARDID
     */
    public int getSHARDID() {
        return SHARDID;
    }

    /**
     * @param SHARDID the SHARDID to set
     */
    public void setSHARDID(int SHARDID) {
        this.SHARDID = SHARDID;
    }
}
