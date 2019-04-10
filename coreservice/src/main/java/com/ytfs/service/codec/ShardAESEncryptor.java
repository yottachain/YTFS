package com.ytfs.service.codec;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class ShardAESEncryptor {

    private List<Shard> shards = null;
    private Cipher cipher;
    private List<Shard> enc_shards = null;

    public ShardAESEncryptor(List<Shard> shards, byte[] key) {
        this.shards = shards;
        this.enc_shards = new ArrayList();
        init(key);
    }

    private void init(byte[] key) {
        try {
            SecretKeySpec skeySpec = new SecretKeySpec(key, "AES");
            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    private byte[] encode(byte[] data) {
        try {
            byte[] bs = cipher.doFinal(data);
            byte[] newdata = new byte[bs.length - 16];
            System.arraycopy(bs, 0, newdata, 0, newdata.length);
            return newdata;
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    private byte[] sha(byte[] data) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return sha.digest(data);
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }
    }

    public void encrypt() {
        Shard firstShard = shards.get(0);
        byte[] bs = encode(firstShard.getData());
        byte[] dst = sha(bs);
        Shard shard = new Shard(bs, dst);
        for (Shard sd : shards) {
            if (sd != firstShard) {
                bs = encode(sd.getData());
                dst = sha(bs);
                enc_shards.add(new Shard(bs, dst));
            } else {
                enc_shards.add(shard);
            }
        }
    }

    public byte[] makeVHB() {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            if (shards.get(0).isRsShard()) {
                for (Shard s : enc_shards) {
                    md5.update(s.getVHF());
                }
            } else {
                md5.update(enc_shards.get(0).getVHF());
            }
            return md5.digest();
        } catch (Exception r) {
            throw new IllegalArgumentException(r.getMessage());
        }

    }

    /**
     * @return the shards
     */
    public List<Shard> getShards() {
        return shards;
    }

    /**
     * @return the enc_shards
     */
    public List<Shard> getEnc_shards() {
        return enc_shards;
    }
}
