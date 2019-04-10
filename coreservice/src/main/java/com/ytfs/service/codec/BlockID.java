package com.ytfs.service.codec;

public class BlockID {

    private final long blockid;
    private final int timestamp;

    public BlockID(long id) {
        this.blockid = id;
        this.timestamp = (int) (id >> 32);
    }

    public long[] getShardIds(int shardCount) {
        long[] ids = new long[shardCount];
        for (int ii = 0; ii < shardCount; ii++) {
            ids[ii] = blockid + ii;
        }
        return ids;
    }

    /**
     * @return the blockid
     */
    public long getBlockid() {
        return blockid;
    }

    /**
     * @return the timestamp
     */
    public int getTimestamp() {
        return timestamp;
    }

}
