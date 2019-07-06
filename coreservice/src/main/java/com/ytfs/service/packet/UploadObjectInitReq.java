package com.ytfs.service.packet;

public class UploadObjectInitReq {

    private long length;
    private byte[] VHW;

    public UploadObjectInitReq() {
    }

    public UploadObjectInitReq(long length, byte[] VHW) {
        this.length = length;
        this.VHW = VHW;
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

}
