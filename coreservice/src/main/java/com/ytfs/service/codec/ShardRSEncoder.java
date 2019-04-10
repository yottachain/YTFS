package com.ytfs.service.codec;

import com.ytfs.service.UserConfig;
import static com.ytfs.service.UserConfig.Default_PND;
import static com.ytfs.service.UserConfig.Default_Shard_Size;
import com.ytfs.service.codec.erasure.ReedSolomon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShardRSEncoder {

    private final Block block;
    private List<Shard> shardList;
    private boolean copyMode = false;

    public ShardRSEncoder(Block block) {
        this.block = block;
    }

    public void encode() throws IOException {
        getBlock().load();
        if (!needEncode()) {
            return;
        }
        shardList = new ArrayList();
        int shardsize = Default_Shard_Size - 1;
        int dataShardCount = getBlock().getRealSize() / shardsize;
        int remainSize = getBlock().getRealSize() % shardsize;
        if (dataShardCount == 0) {//副本
            byte[] bs = new byte[Default_Shard_Size];
            bs[0] = (byte) 0xFF;
            System.arraycopy(getBlock().getData(), 0, bs, 1, remainSize);
            Arrays.fill(bs, remainSize + 1, bs.length - 1, (byte) 0x00);
            Shard shard = new Shard(bs);
            getShardList().add(shard);
            for (int ii = 0; ii < Default_PND; ii++) {
                getShardList().add(shard);
            }
            copyMode = true;
        } else {
            ReedSolomon reedSolomon = ReedSolomon.create(dataShardCount + (remainSize > 0 ? 1 : 0), Default_PND);
            byte[][] out = new byte[reedSolomon.getTotalShardCount()][Default_Shard_Size];
            for (int ii = 0; ii < dataShardCount; ii++) {
                out[ii][0] = (byte) ii;
                System.arraycopy(getBlock().getData(), ii * shardsize, out[ii], 1, shardsize);
            }
            if (remainSize > 0) {
                byte[] bs = out[dataShardCount];
                bs[0] = (byte) dataShardCount;
                System.arraycopy(getBlock().getData(), dataShardCount * shardsize, bs, 1, remainSize);
                Arrays.fill(bs, remainSize + 1, bs.length - 1, (byte) 0x00);
            }
            for (int ii = reedSolomon.getDataShardCount(); ii < reedSolomon.getTotalShardCount(); ii++) {
                out[ii][0] = (byte) ii;
            }
            reedSolomon.encodeParity(out, 1, shardsize);
            for (byte[] out1 : out) {
                Shard shard = new Shard(out1);
                getShardList().add(shard);
            }
        }
    }

    /**
     * @return the shardList
     */
    public List<Shard> getShardList() {
        return shardList;
    }

    /**
     * 分片数目
     *
     * @return
     */
    public int getShardCount() {
        return shardList == null ? 0 : shardList.size();
    }

    public boolean isCopyMode() {
        return copyMode;
    }

    /**
     * 需要分片?
     *
     * @return
     */
    public boolean needEncode() {
        return getBlock().getRealSize() >= UserConfig.PL2;
    }

    /**
     * @return the block
     */
    public Block getBlock() {
        return block;
    }
}
