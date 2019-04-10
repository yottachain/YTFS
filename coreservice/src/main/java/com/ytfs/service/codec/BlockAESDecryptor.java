package com.ytfs.service.codec;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESDecryptor {

    /**
     * @return the block
     */
    public Block getBlock() {
        return block;
    }

    private Block block;
    private Cipher cipher;
    private final byte[] data;

    public BlockAESDecryptor(byte[] data, byte[] key) {
        this.data = data;
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

    public void decrypt() {
        try {
            byte[] bs = cipher.doFinal(data);
            this.block = new Block(bs);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }
}
