package com.ytfs.service.codec;

import static com.ytfs.service.UserConfig.Default_PND;
import com.ytfs.service.codec.erasure.ReedSolomon;
import java.io.IOException;
import java.util.List;

public class ShardRSHealMissing {

    private final List<Shard> shards;
    private final int blockRealSize;

    public ShardRSHealMissing(List<Shard> shards, int blockRealSize) {
        this.shards = shards;
        this.blockRealSize = blockRealSize;
    }

    public void heal() throws IOException {
        Shard shard = getShards().get(0);
        if (!shard.isRsShard()) {//副本
            getShards().clear();
            for (int ii = 0; ii < Default_PND + 1; ii++) {
                getShards().add(shard);
            }
        } else {
            int shardsize = shard.getData().length - 1;
            int dataShardCount = blockRealSize / shardsize;
            int remainSize = blockRealSize % shardsize;
            if (getShards().size() - dataShardCount - (remainSize > 0 ? 1 : 0) < 0) {
                throw new IllegalArgumentException("Not enough shards present");
            }
            ReedSolomon reedSolomon = ReedSolomon.create(dataShardCount + (remainSize > 0 ? 1 : 0), Default_PND);
            byte[][] datas = new byte[reedSolomon.getTotalShardCount()][];
            for (Shard shd : getShards()) {
                datas[shd.getShardIndex()] = shd.getData();
            }
            boolean[] shardPresent = new boolean[reedSolomon.getTotalShardCount()];
            for (int ii = 0; ii < reedSolomon.getTotalShardCount(); ii++) {
                if (datas[ii] == null) {
                    datas[ii] = new byte[shardsize + 1];
                    datas[ii][0] = (byte) ii;
                } else {
                    shardPresent[ii] = true;
                }
            }
            reedSolomon.decodeMissing(datas, shardPresent, 1, shardsize);
            for (int ii = 0; ii < reedSolomon.getTotalShardCount(); ii++) {
                if (shardPresent[ii] == false) {
                    Shard shd = new Shard(datas[ii], ShardRSEncoder.sha(datas[ii]));
                    shards.add(shd);
                }
            }
        }
    }

    /**
     * @return the shards
     */
    public List<Shard> getShards() {
        return shards;
    }
}
