package com.ytfs.service;

public class UploadShardRes {

    public static final int RES_OK = 0;
    public static final int RES_NETIOERR = 400;
    public static final int RES_BAD_REQUEST = 100;
    public static final int RES_NO_SPACE = 101;

    private int SHARDID; //分片索引
    private int NODEID;
    private int RES;     // 0成功  100 bad request  101  空间不足  

    /**
     * @return the NODEID
     */
    public int getNODEID() {
        return NODEID;
    }

    /**
     * @param NODEID the NODEID to set
     */
    public void setNODEID(int NODEID) {
        this.NODEID = NODEID;
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
