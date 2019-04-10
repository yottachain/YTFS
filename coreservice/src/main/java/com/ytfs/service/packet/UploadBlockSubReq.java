package com.ytfs.service.packet;

import com.ytfs.service.UploadShardRes;

public class UploadBlockSubReq {

    private UploadShardRes[] res = null;
    private long VBI;    //上传块生成的流水号

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
     * @return the res
     */
    public UploadShardRes[] getRes() {
        return res;
    }

    /**
     * @param res the res to set
     */
    public void setRes(UploadShardRes[] res) {
        this.res = res;
    }

}
