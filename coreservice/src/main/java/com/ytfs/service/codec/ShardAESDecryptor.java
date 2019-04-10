package com.ytfs.service.codec;

import java.util.ArrayList;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class ShardAESDecryptor {

    private List<Shard> shards = null;
    private List<Shard> dec_shards = null;
    private Shard shard = null;
    private Shard dec_shard = null;
    private Cipher cipher;
    private byte[] end;

    public ShardAESDecryptor(Shard shard, byte[] key) {
        this.shard = shard;
        init(key);
    }

    public ShardAESDecryptor(List<Shard> shards, byte[] key) {
        this.shards = shards;
        this.dec_shards = new ArrayList();
        init(key);
    }

    private void init(byte[] key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            end = cipher.doFinal();
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    private byte[] decode(byte[] data) {
        try {
            byte[] bs1 = cipher.update(data);
            byte[] bs2 = cipher.doFinal(end);
            byte[] newdata = new byte[bs1.length + bs2.length];
            System.arraycopy(bs1, 0, newdata, 0, bs1.length);
            System.arraycopy(bs2, 0, newdata, bs1.length, bs2.length);
            return newdata;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void decrypt() {
        if (shard != null) {
            byte[] bs = decode(shard.getData());
            this.dec_shard = new Shard(bs);
        }
        if (shards != null) {
            Shard firstShard = shards.get(0);
            byte[] bs = decode(firstShard.getData());
            Shard shard1 = new Shard(bs);
            for (Shard sd : shards) {
                if (sd != firstShard) {
                    bs = decode(sd.getData());
                    this.dec_shards.add(new Shard(bs));

                } else {
                    this.dec_shards.add(shard1);
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

    /**
     * @return the shard
     */
    public Shard getShard() {
        return shard;
    }

    /**
     * @return the dec_shards
     */
    public List<Shard> getDec_shards() {
        return dec_shards;
    }

    /**
     * @return the dec_shard
     */
    public Shard getDec_shard() {
        return dec_shard;
    }
}
