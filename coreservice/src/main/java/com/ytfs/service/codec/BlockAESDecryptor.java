package com.ytfs.service.codec;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESDecryptor {

    private byte[] srcData;
    private Cipher cipher;
    private final byte[] end;
    private final byte[] data;

    public BlockAESDecryptor(byte[] data, int blockRealSize, byte[] key) {
        this.data = data;
        if (blockRealSize % 16 == 0) {
            end = makeEnd(key);
        } else {
            end = null;
        }
        init(key);
    }

    private void init(byte[] key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    private byte[] makeEnd(byte[] key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            return cipher.doFinal();
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void decrypt() {
        try {
            if (end == null) {
                this.srcData = cipher.doFinal(data);
            } else {
                byte[] bs1 = cipher.update(data);
                byte[] bs2 = cipher.doFinal(end);
                byte[] newdata = new byte[bs1.length + bs2.length];
                System.arraycopy(bs1, 0, newdata, 0, bs1.length);
                System.arraycopy(bs2, 0, newdata, bs1.length, bs2.length);
                this.srcData = newdata;
            }
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the srcData
     */
    public byte[] getSrcData() {
        return srcData;
    }
}
