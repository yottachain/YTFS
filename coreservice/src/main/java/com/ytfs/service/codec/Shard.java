package com.ytfs.service.codec;

public class Shard {

    private final byte[] data;//数据分片内容
    private final byte[] VHF;

    public Shard(byte[] data) {
        this.data = data;
        this.VHF = null;
    }

    public Shard(byte[] data, byte[] VHF) {
        this.data = data;
        this.VHF = VHF;
    }

    public boolean isRsShard() {
        return getData()[0] != (byte) 0xFF;
    }

    public int getShardIndex() {
        return getData()[0] & 0x0FF;
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @return the VHF
     */
    public byte[] getVHF() {
        return VHF;
    }
}
