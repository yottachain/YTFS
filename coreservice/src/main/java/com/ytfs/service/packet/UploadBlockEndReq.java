package com.ytfs.service.packet;

import com.ytfs.service.servlet.UploadShardCache;
import com.ytfs.service.ServerConfig;
import com.ytfs.service.UploadShardRes;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.dao.BlockMeta;
import com.ytfs.service.dao.ShardMeta;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_KED;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_SHARD;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_VHP;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.ObjectId;

public class UploadBlockEndReq {

    private short id;
    private byte[] VHP;
    private byte[] VHB;
    private byte[] KEU;
    private byte[] KED;
    private long originalSize;  //编码前长度  6字节
    private int realSize;
    private long VBI;
    private boolean rsShard;

    public List<Document> verify(Map<Integer, UploadShardCache> caches, int shardCount, long vbi) throws ServiceException {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] vhf = null;
            List<Document> ls = new ArrayList();
            for (int ii = 0; ii < shardCount; ii++) {
                UploadShardCache cache = caches.get(ii);
                if (cache == null || cache.getRes() != UploadShardRes.RES_OK) {
                    throw new ServiceException(INVALID_SHARD);
                }
                if (rsShard) {
                    md5.update(cache.getVHF());
                } else {
                    if (ii == 0) {
                        md5.update(cache.getVHF());
                        vhf = cache.getVHF();
                    } else {
                        if (!Arrays.equals(cache.getVHF(), vhf)) {
                            throw new ServiceException(INVALID_SHARD);
                        }
                    }
                }
                ShardMeta meta = new ShardMeta(vbi + ii, cache.getNodeid(), cache.getVHF());
                ls.add(meta.toDocument());
            }
            byte[] vhb = md5.digest();
            if (!Arrays.equals(vhb, getVHB())) {
                throw new ServiceException(INVALID_VHB);
            }
            if (getVHP() == null || getVHP().length != 32) {
                throw new ServiceException(INVALID_VHP);
            }
            if (getKEU() == null || getKEU().length != 32) {
                throw new ServiceException(INVALID_KEU);
            }
            if (getKED() == null || getKED().length != 32) {
                throw new ServiceException(INVALID_KED);
            }
            return ls;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public BlockMeta makeBlockMeta(long VBI, int shardCount) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(getKED());
        meta.setNLINK(1);
        if (this.isRsShard()) {
            meta.setVNF(shardCount);
        } else {
            meta.setVNF(shardCount * -1);
        }
        meta.setVHB(getVHB());
        meta.setVHP(getVHP());
        return meta;
    }

    public SaveObjectMetaReq makeSaveObjectMetaReq(int userid, long vbi, ObjectId VNU) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(VNU);
        ObjectRefer refer = new ObjectRefer();
        refer.setId(getId());
        refer.setKEU(getKEU());
        refer.setOriginalSize(getOriginalSize());
        refer.setRealSize(this.realSize);
        refer.setSuperID((byte) ServerConfig.superNodeID);
        refer.setVBI(vbi);
        saveObjectMetaReq.setRefer(refer);
        return saveObjectMetaReq;
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
    public void setId(short id) {
        this.id = id;
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
    public void setVHP(byte[] VHP) {
        this.VHP = VHP;
    }

    /**
     * @return the VHB
     */
    public byte[] getVHB() {
        return VHB;
    }

    /**
     * @param VHB the VHB to set
     */
    public void setVHB(byte[] VHB) {
        this.VHB = VHB;
    }

    /**
     * @return the KEU
     */
    public byte[] getKEU() {
        return KEU;
    }

    /**
     * @param KEU the KEU to set
     */
    public void setKEU(byte[] KEU) {
        this.KEU = KEU;
    }

    /**
     * @return the KED
     */
    public byte[] getKED() {
        return KED;
    }

    /**
     * @param KED the KED to set
     */
    public void setKED(byte[] KED) {
        this.KED = KED;
    }

    /**
     * @return the originalSize
     */
    public long getOriginalSize() {
        return originalSize;
    }

    /**
     * @param originalSize the originalSize to set
     */
    public void setOriginalSize(long originalSize) {
        this.originalSize = originalSize;
    }

    /**
     * @return the realSize
     */
    public int getRealSize() {
        return realSize;
    }

    /**
     * @param realSize the realSize to set
     */
    public void setRealSize(int realSize) {
        this.realSize = realSize;
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
     * @return the rsShard
     */
    public boolean isRsShard() {
        return rsShard;
    }

    /**
     * @param rsShard the rsShard to set
     */
    public void setRsShard(boolean rsShard) {
        this.rsShard = rsShard;
    }
}
