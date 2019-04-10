package com.ytfs.service.packet;

public class UploadShard2CResp {

    private int RES;  //0成功  100 bad request  101  空间不足 

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

}
