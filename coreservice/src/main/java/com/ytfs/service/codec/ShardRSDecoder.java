package com.ytfs.service.codec;

import static com.ytfs.service.UserConfig.Default_PND;
import com.ytfs.service.codec.erasure.ReedSolomon;
import java.util.List;

public class ShardRSDecoder {

    private final List<Shard> shards;
    private final int encryptedBlockSize;
    private final Shard copyShard;

    public ShardRSDecoder(List<Shard> shards, int encryptedBlockSize) {
        this.shards = shards;
        this.encryptedBlockSize = encryptedBlockSize;
        this.copyShard = null;
    }

    public ShardRSDecoder(Shard shard, int encryptedBlockSize) {
        this.shards = null;
        this.encryptedBlockSize = encryptedBlockSize;
        this.copyShard = shard;
    }

    public BlockEncrypted decode() {
        Shard shard = copyShard == null ? shards.get(0) : copyShard;
        if (!shard.isRsShard()) {//副本
            byte[] data = new byte[encryptedBlockSize];
            System.arraycopy(shard.getData(), 1, data, 0, encryptedBlockSize);
            BlockEncrypted b = new BlockEncrypted();
            b.setData(data);
            return b;
        } else {
            int shardsize = shard.getData().length - 1;
            int dataShardCount = encryptedBlockSize / shardsize;
            int remainSize = encryptedBlockSize % shardsize;
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
            BlockEncrypted b = new BlockEncrypted();
            b.setData(data);
            return b;
        }
    }

    private byte[] readShard(byte[][] datas, int dataShardCount, int shardsize, int remainSize) {
        byte[] data = new byte[encryptedBlockSize];
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
