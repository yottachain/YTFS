package com.ytfs.service.codec;

import static com.ytfs.service.UserConfig.Default_PND;
import static com.ytfs.service.UserConfig.Default_Shard_Size;
import com.ytfs.service.codec.erasure.ReedSolomon;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShardRSEncoder {

    private final BlockEncrypted encryptedBlock;
    private List<Shard> shardList;

    public ShardRSEncoder(BlockEncrypted block) {
        this.encryptedBlock = block;
    }

    public void encode() throws IOException {
        if (!encryptedBlock.needEncode()) {
            return;
        }
        shardList = new ArrayList();
        int shardsize = Default_Shard_Size - 1;
        int dataShardCount = encryptedBlock.getEncryptedBlockSize() / shardsize;
        int remainSize = encryptedBlock.getEncryptedBlockSize() % shardsize;
        if (dataShardCount == 0) {//副本
            byte[] bs = new byte[Default_Shard_Size];
            bs[0] = (byte) 0xFF;
            System.arraycopy(encryptedBlock.getData(), 0, bs, 1, remainSize);
            Arrays.fill(bs, remainSize + 1, bs.length - 1, (byte) 0x00);
            Shard shard = new Shard(bs, sha(bs));
            getShardList().add(shard);
            for (int ii = 0; ii < Default_PND; ii++) {
                getShardList().add(shard);
            }
            encryptedBlock.setCopyMode(true);
            encryptedBlock.setShardCount(Default_PND);
        } else {
            ReedSolomon reedSolomon = ReedSolomon.create(dataShardCount + (remainSize > 0 ? 1 : 0), Default_PND);
            byte[][] out = new byte[reedSolomon.getTotalShardCount()][Default_Shard_Size];
            for (int ii = 0; ii < dataShardCount; ii++) {
                out[ii][0] = (byte) ii;
                System.arraycopy(encryptedBlock.getData(), ii * shardsize, out[ii], 1, shardsize);
            }
            if (remainSize > 0) {
                byte[] bs = out[dataShardCount];
                bs[0] = (byte) dataShardCount;
                System.arraycopy(encryptedBlock.getData(), dataShardCount * shardsize, bs, 1, remainSize);
                Arrays.fill(bs, remainSize + 1, bs.length - 1, (byte) 0x00);
            }
            for (int ii = reedSolomon.getDataShardCount(); ii < reedSolomon.getTotalShardCount(); ii++) {
                out[ii][0] = (byte) ii;
            }
            reedSolomon.encodeParity(out, 1, shardsize);
            for (byte[] out1 : out) {
                Shard shard = new Shard(out1, sha(out1));
                getShardList().add(shard);
            }
            encryptedBlock.setShardCount(reedSolomon.getTotalShardCount());
        }
    }

    /**
     * @return the shardList
     */
    public List<Shard> getShardList() {
        return shardList;
    }

    static byte[] sha(byte[] data) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return sha.digest(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public byte[] makeVHB() {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            if (shardList.get(0).isRsShard()) {
                for (Shard s : shardList) {
                    md5.update(s.getVHF());
                }
            } else {
                md5.update(shardList.get(0).getVHF());
            }
            return md5.digest();
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }

    }
}
