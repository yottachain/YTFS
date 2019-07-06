package com.ytfs.service.codec;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class BlockAESEncryptor {

    private final Block block;
    private Cipher cipher;
    private final BlockEncrypted blockEncrypted;

    public BlockAESEncryptor(Block block, byte[] key) {
        this.block = block;
        this.blockEncrypted = new BlockEncrypted();
        this.blockEncrypted.setBlocksize(block.getRealSize());
        init(key);
    }

    private void init(byte[] key) {
        try {//AES/ECB/PKCS5Padding    //ECB/CBC/CTR/CFB/OFB
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void encrypt() throws IOException {
        block.load();
        try {
            byte[] bs = cipher.doFinal(block.getData());
            if (block.getRealSize() % 16 == 0) {
                byte[] newdata = new byte[bs.length - 16];
                System.arraycopy(bs, 0, newdata, 0, newdata.length);
                getBlockEncrypted().setData(newdata);
            } else {
                getBlockEncrypted().setData(bs);
            }
        } catch (BadPaddingException | IllegalBlockSizeException r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    /**
     * @return the blockEncrypted
     */
    public BlockEncrypted getBlockEncrypted() {
        return blockEncrypted;
    }
}
