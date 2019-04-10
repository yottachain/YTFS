package com.ytfs.service.packet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.dao.BlockMeta;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_KED;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_KEU;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_VHB;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_VHP;
import static com.ytfs.service.packet.ServiceErrorCode.TOO_BIG_BLOCK;
import java.util.Arrays;
import org.bson.types.ObjectId;

public class UploadBlockDBReq {

    private short id;
    private ObjectId VNU;
    private byte[] VHP;
    private byte[] VHB;
    private byte[] KEU;
    private byte[] KED;
    private long originalSize;  //编码前长度  6字节
    private byte[] data;

    public void verify(byte[] vhb) throws ServiceException {
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
        if (getData().length > ServerConfig.PL2 * 2) {
            throw new ServiceException(TOO_BIG_BLOCK);
        }
    }

    public BlockMeta makeBlockMeta(long VBI) {
        BlockMeta meta = new BlockMeta();
        meta.setVBI(VBI);
        meta.setKED(getKED());
        meta.setNLINK(1);
        meta.setVNF(0);
        meta.setVHB(getVHB());
        meta.setVHP(getVHP());
        return meta;
    }

    public SaveObjectMetaReq makeSaveObjectMetaReq(int userid, long vbi) {
        SaveObjectMetaReq saveObjectMetaReq = new SaveObjectMetaReq();
        saveObjectMetaReq.setUserID(userid);
        saveObjectMetaReq.setVNU(getVNU());
        ObjectRefer refer = new ObjectRefer();
        refer.setId(getId());
        refer.setKEU(getKEU());
        refer.setOriginalSize(getOriginalSize());
        refer.setRealSize(getData().length);
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
     * @return the VNU
     */
    public ObjectId getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(ObjectId VNU) {
        this.VNU = VNU;
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
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(byte[] data) {
        this.data = data;
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

}
