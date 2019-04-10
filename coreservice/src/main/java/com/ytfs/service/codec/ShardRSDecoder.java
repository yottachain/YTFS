package com.ytfs.service.codec;

import static com.ytfs.service.UserConfig.Default_PND;
import com.ytfs.service.codec.erasure.ReedSolomon;
import java.io.IOException;
import java.util.List;

public class ShardRSDecoder {

    private final List<Shard> shards;
    private final int blockRealSize;

    public ShardRSDecoder(List<Shard> shards, int blockRealSize) {
        this.shards = shards;
        this.blockRealSize = blockRealSize;
    }

    public Block decode() throws IOException {
        Shard shard = shards.get(0);
        if (!shard.isRsShard()) {//副本
            byte[] data = new byte[blockRealSize];
            System.arraycopy(shard.getData(), 1, data, 0, blockRealSize);
            return new Block(data);
        } else {
            int shardsize = shard.getData().length - 1;
            int dataShardCount = blockRealSize / shardsize;
            int remainSize = blockRealSize % shardsize;
            ReedSolomon reedSolomon = ReedSolomon.create(dataShardCount + (remainSize > 0 ? 1 : 0), Default_PND);
            byte[][] datas = new byte[reedSolomon.getTotalShardCount()][];
            for (Shard shd : shards) {
                datas[shd.getShardIndex()] = shd.getData();
            }
            boolean needdecode = false;
            for (int ii = 0; ii < reedSolomon.getDataShardCount(); ii++) {
                if (datas[ii] == null) {
                    needdecode = true;
                    break;
                }
            }
            if (needdecode) {
                if (shards.size() - dataShardCount - (remainSize > 0 ? 1 : 0) < 0) {
                    throw new IllegalArgumentException("Not enough shards present");
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
            }
            byte[] data = readShard(datas, dataShardCount, shardsize, remainSize);
            return new Block(data);
        }
    }

    private byte[] readShard(byte[][] datas, int dataShardCount, int shardsize, int remainSize) {
        byte[] data = new byte[blockRealSize];
        for (int ii = 0; ii < dataShardCount; ii++) {
            byte[] bs = datas[ii];
            System.arraycopy(bs, 1, data, ii * shardsize, shardsize);
        }
        if (remainSize > 0) {
            byte[] bs = datas[dataShardCount];
            System.arraycopy(bs, 1, data, dataShardCount * shardsize, remainSize);
        }
        return data;
    }
}
