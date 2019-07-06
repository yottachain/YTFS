package com.ytfs.service.codec;

import com.ytfs.service.UserConfig;
import static com.ytfs.service.UserConfig.Default_PND;
import static com.ytfs.service.UserConfig.Default_Shard_Size;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BlockEncrypted {

    private int blocksize;
    private int encryptedBlockSize;
    private byte[] data;
    private boolean copyMode = false;
    private int shardCount;

    public BlockEncrypted() {
    }

    public BlockEncrypted(int blocksize) {
        this.blocksize = blocksize;
        int remain = blocksize % 16;
        if (remain == 0) {
            encryptedBlockSize = blocksize;
        } else {
            encryptedBlockSize = blocksize + (16 - remain);
        }
        if (encryptedBlockSize <= UserConfig.PL2) {
            shardCount = 0;
        } else {
            int shardsize = Default_Shard_Size - 1;
            int dataShardCount = encryptedBlockSize / shardsize;
            int remainSize = encryptedBlockSize % shardsize;
            if (dataShardCount == 0) {//副本
                shardCount = Default_PND;
                copyMode = true;
            } else {
                shardCount = dataShardCount + (remainSize > 0 ? 1 : 0) + Default_PND;
            }
        }
    }

    /**
     * 需要分片?
     *
     * @return
     */
    public boolean needEncode() {
        return encryptedBlockSize >= UserConfig.PL2;
    }

    public byte[] getVHB() {
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
     * @return the blocksize
     */
    public int getBlocksize() {
        return blocksize;
    }

    /**
     * @param blocksize the blocksize to set
     */
    public void setBlocksize(int blocksize) {
        this.blocksize = blocksize;
    }

    /**
     * @return the encryptedBlockSize
     */
    public int getEncryptedBlockSize() {
        return encryptedBlockSize;
    }

    /**
     * @param encryptedBlockSize the encryptedBlockSize to set
     */
    public void setEncryptedBlockSize(int encryptedBlockSize) {
        this.encryptedBlockSize = encryptedBlockSize;
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
        this.encryptedBlockSize = data.length;
    }

    /**
     * @return the copyMode
     */
    public boolean isCopyMode() {
        return copyMode;
    }

    /**
     * @param copyMode the copyMode to set
     */
    public void setCopyMode(boolean copyMode) {
        this.copyMode = copyMode;
    }

    /**
     * @return the shardCount
     */
    public int getShardCount() {
        return shardCount;
    }

    /**
     * @param shardCount the shardCount to set
     */
    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }
}
