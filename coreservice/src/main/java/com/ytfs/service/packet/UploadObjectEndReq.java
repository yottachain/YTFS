package com.ytfs.service.packet;

public class UploadObjectEndReq {

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

 
    private byte[] VHW = null;
}
