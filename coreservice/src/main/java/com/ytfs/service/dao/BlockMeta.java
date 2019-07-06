package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.Binary;

public class BlockMeta {

    private long VBI = 0;//主键，给该数据块生成一个唯一编号
    private byte[] VHP;//该数据块的明文SHA256摘要
    private byte[] VHB;//每个加密后的数据分片的SHA256摘要，连接在一起后再计算出的MD5摘要
    private byte[] KED;//去重密钥
    private int VNF;  //分片数目 0 表示存在数据库中 >1 RS <1 副本
    private long NLINK;//引用计数，如果引用次数达到0xffffff，则该文件将永远不再删除

    public BlockMeta() {
    }

    public BlockMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this.VBI = doc.getLong("_id");
        }
        if (doc.containsKey("VHP")) {
            this.VHP = ((Binary) doc.get("VHP")).getData();
        }
        if (doc.containsKey("VHB")) {
            this.VHB = ((Binary) doc.get("VHB")).getData();
        }
        if (doc.containsKey("KED")) {
            this.KED = ((Binary) doc.get("KED")).getData();
        }
        if (doc.containsKey("VNF")) {
            this.VNF = doc.getInteger("VNF");
        }
        if (doc.containsKey("NLINK")) {
            this.NLINK = doc.getLong("NLINK");
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", VBI);
        doc.append("VHP", new Binary(VHP));
        doc.append("VHB", new Binary(VHB));
        doc.append("KED", new Binary(KED));
        doc.append("VNF", VNF);
        doc.append("NLINK", NLINK);
        return doc;
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
     * @return the VNF
     */
    public int getVNF() {
        return VNF;
    }

    /**
     * @param VNF the VNF to set
     */
    public void setVNF(int VNF) {
        this.VNF = VNF;
    }

    /**
     * @return the NLINK
     */
    public long getNLINK() {
        return NLINK;
    }

    /**
     * @param NLINK the NLINK to set
     */
    public void setNLINK(long NLINK) {
        this.NLINK = NLINK;
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
}
