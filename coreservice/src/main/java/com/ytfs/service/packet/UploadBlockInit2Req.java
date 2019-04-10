package com.ytfs.service.packet;

public class UploadBlockInit2Req extends UploadBlockInitReq {

    public UploadBlockInit2Req() {

    }

    public UploadBlockInit2Req(UploadBlockInitReq req) {
        this.setId(req.getId());
        this.setShardCount(req.getShardCount());
        this.setVHP(req.getVHP());
        this.setVNU(req.getVNU());
    }

}
