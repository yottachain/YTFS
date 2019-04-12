package com.ytfs.service.codec;

import com.ytfs.service.Function;
import static com.ytfs.service.Function.bytes2Integer;
import java.util.ArrayList;
import java.util.List;

public class ObjectRefer {

    /**
     * 解析
     *
     * @param bs
     * @return
     */
    public static List<ObjectRefer> parse(byte[] bs) {
        List<ObjectRefer> ls = new ArrayList();
        int len = bs.length / 52;
        for (int ii = 0; ii < len; ii++) {
            ObjectRefer refer = new ObjectRefer();
            ls.add(refer);
            refer.VBI = bytes2Integer(bs, ii * 52, 8);
            refer.superID = bs[ii * 52 + 8];
            refer.originalSize = bytes2Integer(bs, ii * 52 + 9, 6);
            refer.realSize = (int) bytes2Integer(bs, ii * 52 + 15, 3);
            byte[] bbs = new byte[32];
            System.arraycopy(bs, ii * 52 + 18, bbs, 0, 32);
            refer.KEU = bbs;
            refer.id = (short) bytes2Integer(bs, ii * 52 + 50, 2);
        }
        return ls;
    }

    /**
     * 合并
     *
     * @param ls
     * @return
     */
    public static byte[] merge(List<ObjectRefer> ls) {
        byte[] bs = new byte[50 * ls.size()];
        int pos = 0;
        for (ObjectRefer refer : ls) {
            Function.long2bytes(refer.getVBI(), bs, pos);
            pos=pos+8;
            bs[pos++] = refer.getSuperID();
            
            bs[pos++] = (byte) (refer.getOriginalSize() >>> 40);
            bs[pos++] = (byte) (refer.getOriginalSize() >>> 32);
            bs[pos++] = (byte) (refer.getOriginalSize() >>> 24);
            bs[pos++] = (byte) (refer.getOriginalSize() >>> 16);
            bs[pos++] = (byte) (refer.getOriginalSize() >>> 8);
            bs[pos++] = (byte) (refer.getOriginalSize());
                        
            bs[pos++] = (byte) (refer.getRealSize() >>> 16);
            bs[pos++] = (byte) (refer.getRealSize() >>> 8);
            bs[pos++] = (byte) (refer.getRealSize());
            
            System.arraycopy(refer.KEU, 0, bs, pos, 32);
            pos = pos + 32;
            bs[pos++] = (byte) (refer.getId() >>> 8);
            bs[pos++] = (byte) (refer.getId());
        }
        return bs;
    }

    private long VBI;  //8
    private byte superID;  //1
    private long originalSize;  //编码前长度  6字节
    private int realSize;  //实际长度    3字节   
    private byte[] KEU;  //32
    private short id;

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
     * @return the superID
     */
    public byte getSuperID() {
        return superID;
    }

    /**
     * @param superID the superID to set
     */
    public void setSuperID(byte superID) {
        this.superID = superID;
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
}
