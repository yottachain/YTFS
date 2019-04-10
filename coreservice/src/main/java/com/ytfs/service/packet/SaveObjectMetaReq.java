package com.ytfs.service.packet;

import com.ytfs.service.codec.ObjectRefer;
import org.bson.types.ObjectId;

public class SaveObjectMetaReq {

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
     * @return the refer
     */
    public ObjectRefer getRefer() {
        return refer;
    }

    /**
     * @param refer the refer to set
     */
    public void setRefer(ObjectRefer refer) {
        this.refer = refer;
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

     

    private int userID;
    private ObjectId VNU;
    private ObjectRefer refer;

}
