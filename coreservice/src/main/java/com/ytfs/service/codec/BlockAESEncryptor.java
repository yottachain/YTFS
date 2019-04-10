package com.ytfs.service.codec;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESEncryptor {

    private Block block = null;
    private Cipher cipher;
    private byte[] data;
    private byte[] VHB;

    public BlockAESEncryptor(Block block, byte[] key) {
        this.block = block;
        init(key);
    }

    private void init(byte[] key) {
        try {//AES/ECB/PKCS5Padding    //ECB/CBC/CTR/CFB/OFB
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void encrypt() {
        try {
            this.data = cipher.doFinal(block.getData());
            this.VHB = makeVHB(this.data);
        } catch (BadPaddingException | IllegalBlockSizeException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public static byte[] makeVHB(byte[] data) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bs = sha256.digest(data);
            md5.update(data);
            return md5.digest(bs);
        } catch (NoSuchAlgorithmException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @return the VHB
     */
    public byte[] getVHB() {
        return VHB;
    }
}
