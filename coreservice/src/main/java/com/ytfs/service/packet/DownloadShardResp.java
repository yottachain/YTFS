package com.ytfs.service.packet;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class DownloadShardResp {

    private byte[] data;

    public boolean verify(byte[] VHF) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(data);
            byte[] bs = sha256.digest();
            return Arrays.equals(bs, VHF);
        } catch (NoSuchAlgorithmException ex) {
            return false;
        }
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(byte[] data) {
        this.data = data;
    }

}
