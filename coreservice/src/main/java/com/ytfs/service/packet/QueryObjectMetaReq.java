package com.ytfs.service.packet;

import org.bson.types.ObjectId;

public class QueryObjectMetaReq {

    private ObjectId VNU; //upload id
    private int userID;

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

}
