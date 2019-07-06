package com.ytfs.service.servlet;

import com.ytfs.service.Function;
import static com.ytfs.service.ServerConfig.REDIS_EXPIRE;
import com.ytfs.service.dao.RedisSource;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.bson.types.ObjectId;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.JedisCluster;

public class UploadObjectCache {

    private int userid;
    private long filesize;

    /**
     * @return the userid
     */
    public int getUserid() {
        return userid;
    }

    /**
     * @param userid the userid to set
     */
    public void setUserid(int userid) {
        this.userid = userid;
    }

    /**
     * @return the filesize
     */
    public long getFilesize() {
        return filesize;
    }

    /**
     * @param filesize the filesize to set
     */
    public void setFilesize(long filesize) {
        this.filesize = filesize;
    }

    public static byte[] getCacheKey(ObjectId VNU) {
        byte[] key = new byte[16];
        System.arraycopy(VNU.toByteArray(), 0, key, 0, 12);
        Arrays.fill(key, 12, 15, (byte) 0x01);
        return key;
    }

    private short[] blockids = null;

    public boolean exists(ObjectId VNU, short num) {
        if (blockids != null) {
            for (short s : blockids) {
                if (s == num) {
                    return true;
                }
            }
            return false;
        }
        BasicCommands jedis = RedisSource.getJedis();
        byte[] key = getCacheKey(VNU);
        byte[] value = (jedis instanceof BinaryJedis)
                ? ((BinaryJedis) jedis).get(key)
                : ((BinaryJedisCluster) jedis).get(key);
        if (value == null) {
            return false;
        } else {
            ByteBuffer buf = ByteBuffer.wrap(key);
            while (buf.hasRemaining()) {
                short s = buf.getShort();
                if (s == num) {
                    return true;
                }
            }
            return false;
        }
    }

    public void setBlockNums(ObjectId VNU, short[] num) {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] key = getCacheKey(VNU);
        ByteBuffer buf = ByteBuffer.allocate(num.length * 2);
        for (short s : num) {
            buf.putShort(s);
        }
        buf.flip();
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).setex(key, REDIS_EXPIRE, buf.array());
        } else {
            ((BinaryJedisCluster) jedis).setex(key, REDIS_EXPIRE, buf.array());
        }
        blockids = num;
    }

    public static void setBlockNum(ObjectId VNU, short num) {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] key = getCacheKey(VNU);
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).append(key, Function.short2bytes(num));
        } else {
            ((BinaryJedisCluster) jedis).append(key, Function.short2bytes(num));
        }
    }
}
