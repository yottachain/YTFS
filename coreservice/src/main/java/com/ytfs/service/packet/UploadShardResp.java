package com.ytfs.service.packet;

import com.ytfs.service.Function;
import com.ytfs.service.codec.KeyStoreCoder;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_SIGNATURE;
import static com.ytfs.service.packet.ServiceErrorCode.TOO_MANY_SHARDS;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;

public class UploadShardResp {

    private int RES;  //0成功  100 bad request  101  空间不足 
    private int SHARDID; //分片索引
    private long VBI;    //上传块生成的流水号
    private byte[] VHF;      //分片hash
    private byte[] USERSIGN; //用户对VHF和SHARDID,NODEID,VBI的签名

    public void verify(byte[] key, int maxshardCount, int nodeid) throws ServiceException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
        if (SHARDID >= maxshardCount) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }//使用用户公钥验签
        Key pkey = KeyStoreCoder.rsaPublicKey(key);
        java.security.Signature signetcheck = java.security.Signature.getInstance("DSA");
        signetcheck.initVerify((PublicKey) pkey);
        signetcheck.update(VHF);
        signetcheck.update(Function.int2bytes(SHARDID));
        signetcheck.update(Function.int2bytes(nodeid));
        signetcheck.update(Function.long2bytes(VBI));
        if (!signetcheck.verify(USERSIGN)) {
            throw new ServiceException(INVALID_SIGNATURE);
        }
    }

    /**
     * @return the RES
     */
    public int getRES() {
        return RES;
    }

    /**
     * @param RES the RES to set
     */
    public void setRES(int RES) {
        this.RES = RES;
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
}
