package com.ytfs.service.dao;

import org.bson.Document;
import org.bson.types.Binary;

public class ShardMeta {

    private long VFI;
    private int nodeId;
    private byte[] VHF;

    public ShardMeta() {
    }

    public ShardMeta(long VFI, int nodeId, byte[] VHF) {
        this.VFI = VFI;
        this.nodeId = nodeId;
        this.VHF = VHF;
    }

    public ShardMeta(Document doc) {
        if (doc.containsKey("_id")) {
            this.VFI = doc.getLong("_id");
        }
        if (doc.containsKey("nodeId")) {
            this.nodeId = doc.getInteger("nodeId");
        }
        if (doc.containsKey("VHF")) {
            this.VHF = ((Binary) doc.get("VHF")).getData();
        }
    }

    public Document toDocument() {
        Document doc = new Document();
        doc.append("_id", VFI);
        doc.append("nodeId", nodeId);
        doc.append("VHF", new Binary(VHF));
        return doc;
    }

    /**
     * @return the VFI
     */
    public long getVFI() {
        return VFI;
    }

    /**
     * @param VFI the VFI to set
     */
    public void setVFI(long VFI) {
        this.VFI = VFI;
    }

    /**
     * @return the nodeId
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
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

}
