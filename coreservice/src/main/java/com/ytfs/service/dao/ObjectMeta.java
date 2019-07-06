package com.ytfs.service.dao;

import java.nio.ByteBuffer;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class ObjectMeta {

    private int userID;
    private byte[] VHW;
    private ObjectId VNU;
    private int NLINK;
    private long length;
    private byte[] blocks;

    private final byte[] _id;

    public ObjectMeta(int userID, byte[] VHW) {
        this.userID = userID;
        this.VHW = VHW;
        byte[] id = new byte[VHW.length + 4];
        id[0] = (byte) (userID >>> 24);
        id[1] = (byte) (userID >>> 16);
        id[2] = (byte) (userID >>> 8);
        id[3] = (byte) (userID);
        System.arraycopy(VHW, 0, id, 4, VHW.length);
        this._id = id;
    }

    public ObjectMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this._id = ((Binary) doc.get("_id")).getData();
            ByteBuffer bb = ByteBuffer.wrap(_id);
            this.userID = bb.getInt();
            VHW = new byte[bb.remaining()];
            bb.get(VHW);
        } else {
            _id = null;
        }
        if (doc.containsKey("VNU")) {
            this.VNU = doc.getObjectId("VNU");
        }
        if (doc.containsKey("NLINK")) {
            this.NLINK = doc.getInteger("NLINK");
        }
        if (doc.containsKey("length")) {
            this.length = doc.getLong("length");
        }
        if (doc.containsKey("blocks")) {
            this.blocks = ((Binary) doc.get("blocks")).getData();
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", new Binary(getId()));
        doc.append("length", this.length);
        doc.append("VNU", VNU);
        doc.append("NLINK", NLINK);
        doc.append("blocks", new Binary(blocks));
        return doc;
    }

    /**
     * @return the userID
     */
    public int getUserID() {
        return userID;
    }

    /**
     * @param userID the userID to set
     */
    public void setUserID(int userID) {
        this.userID = userID;
    }

    /**
     * @return the VHW
     */
    public byte[] getVHW() {
        return VHW;
    }

    /**
     * @param VHW the VHW to set
     */
    public void setVHW(byte[] VHW) {
        this.VHW = VHW;
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
     * @return the NLINK
     */
    public int getNLINK() {
        return NLINK;
    }

    /**
     * @param NLINK the NLINK to set
     */
    public void setNLINK(int NLINK) {
        this.NLINK = NLINK;
    }

    /**
     * @return the blocks
     */
    public byte[] getBlocks() {
        return blocks;
    }

    /**
     * @param blocks the blocks to set
     */
    public void setBlocks(byte[] blocks) {
        this.blocks = blocks;
    }

    /**
     * @return the _id
     */
    public byte[] getId() {
        return _id;
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
