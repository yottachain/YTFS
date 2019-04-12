package com.ytfs.service.servlet;

import static com.ytfs.service.Function.long2bytes;
import static com.ytfs.service.ServerConfig.REDIS_BLOCK_EXPIRE;
import static com.ytfs.service.ServerConfig.REDIS_EXPIRE;
import com.ytfs.service.dao.RedisSource;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.QueryObjectMetaResp;
import com.ytfs.service.packet.SerializationUtil;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_UPLOAD_ID;
import com.ytfs.service.packet.ServiceException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.bson.types.ObjectId;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.BinaryJedisCluster;

public class CacheAccessor {

    /**
     * 清除一次对象上传的所有session
     *
     * @param VNU
     * @param VBI
     */
    static void clearCache(ObjectId VNU, long VBI) {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] key1 = UploadObjectCache.getCacheKey(VNU);
        byte[] key2 = VNU.toByteArray();
        byte[] key3 = long2bytes(VBI);
        byte[] key4 = UploadBlockCache.getCacheKey1(VBI);
        byte[] key5 = UploadBlockCache.getCacheKey2(VBI);
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).del(new byte[][]{key1, key2, key3, key4, key5});
        } else {
            ((BinaryJedisCluster) jedis).del(new byte[][]{key1, key2, key3, key4, key5});
        }
    }

    /**
     * 获取对应数据块已上传的分片信息
     *
     * @param VBI
     * @return
     */
    static Map<Integer, UploadShardCache> getUploadShardCache(long VBI) {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] data = (jedis instanceof BinaryJedis)
                ? ((BinaryJedis) jedis).get(UploadBlockCache.getCacheKey1(VBI))
                : ((BinaryJedisCluster) jedis).get(UploadBlockCache.getCacheKey1(VBI));
        if (data == null || data.length == 0) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int len = data.length / 44;
        Map map = new HashMap();
        for (int ii = 0; ii < len; ii++) {
            UploadShardCache cache = new UploadShardCache();
            cache.fill(buf);
            map.put(cache.getShardid(), cache);
        }
        return map;
    }

    /**
     * 上传一个新数据分片后将分片信息写入cache
     *
     * @param shardCache
     * @param VBI
     */
    static void addUploadShardCache(UploadShardCache shardCache, long VBI) {
        BasicCommands jedis = RedisSource.getJedis();
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).append(UploadBlockCache.getCacheKey1(VBI), shardCache.toByte());
        } else {
            ((BinaryJedisCluster) jedis).append(UploadBlockCache.getCacheKey1(VBI), shardCache.toByte());
        }
    }

    /**
     * 更新数据块cache
     *
     * @param cache
     * @param VBI
     */
    static void putUploadBlockCache(UploadBlockCache cache, long VBI) {
        BasicCommands jedis = RedisSource.getJedis();
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
        } else {
            ((BinaryJedisCluster) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
        }
    }

    /**
     * 初始化数据块cache
     *
     * @param cache
     * @param VBI
     */
    static void setUploadBlockCache(UploadBlockCache cache, long VBI) {
        BasicCommands jedis = RedisSource.getJedis();
        if (jedis instanceof BinaryJedis) {
            ((BinaryJedis) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
            ((BinaryJedis) jedis).setex(UploadBlockCache.getCacheKey1(VBI), REDIS_BLOCK_EXPIRE, new byte[0]);
        } else {
            ((BinaryJedisCluster) jedis).setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
            ((BinaryJedisCluster) jedis).setex(UploadBlockCache.getCacheKey1(VBI), REDIS_BLOCK_EXPIRE, new byte[0]);
        }
    }

    /**
     * 获取数据块cache
     *
     * @param VBI
     * @return
     * @throws ServiceException
     */
    static UploadBlockCache getUploadBlockCache(long VBI) throws ServiceException {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] bs = (jedis instanceof BinaryJedis)
                ? ((BinaryJedis) jedis).get(long2bytes(VBI))
                : ((BinaryJedisCluster) jedis).get(long2bytes(VBI));
        if (bs == null) {
            throw new ServiceException(INVALID_UPLOAD_ID);
        }
        return (UploadBlockCache) SerializationUtil.deserialize(bs);
    }

    /**
     * 更新数据块cache中的验签失败次数
     *
     * @param VBI
     * @return
     * @throws ServiceException
     */
    static long getUploadBlockINC(long VBI) throws ServiceException {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] key = UploadBlockCache.getCacheKey2(VBI);
        if (jedis instanceof BinaryJedis) {
            long l = ((BinaryJedis) jedis).incr(key);
            ((BinaryJedis) jedis).expire(key, REDIS_BLOCK_EXPIRE);
            return l;
        } else {
            long l = ((BinaryJedisCluster) jedis).incr(key);
            ((BinaryJedisCluster) jedis).expire(key, REDIS_BLOCK_EXPIRE);
            return l;
        }
    }

    /**
     * 获取对象cache，不存在从BPU查询
     *
     * @param userid
     * @param VNU
     * @return
     * @throws ServiceException
     */
    static UploadObjectCache getUploadObjectCache(int userid, ObjectId VNU) throws ServiceException {
        BasicCommands jedis = RedisSource.getJedis();
        byte[] bs = (jedis instanceof BinaryJedis)
                ? ((BinaryJedis) jedis).get(VNU.toByteArray())
                : ((BinaryJedisCluster) jedis).get(VNU.toByteArray());
        UploadObjectCache cache;
        if (bs == null) {
            QueryObjectMetaReq req = new QueryObjectMetaReq();
            req.setUserID(userid);
            req.setVNU(VNU);
            Node node = SuperNodeList.getBlockSuperNodeByUserId(userid);
            QueryObjectMetaResp resp = (QueryObjectMetaResp) P2PUtils.requestBP(req, node);
            cache = new UploadObjectCache();
            cache.setFilesize(resp.getLength());
            cache.setUserid(userid);
            if (jedis instanceof BinaryJedis) {
                ((BinaryJedis) jedis).setex(VNU.toByteArray(), REDIS_EXPIRE, SerializationUtil.serialize(cache));
            } else {
                ((BinaryJedisCluster) jedis).setex(VNU.toByteArray(), REDIS_EXPIRE, SerializationUtil.serialize(cache));
            }
            cache.setBlockNums(VNU, resp.getBlocknums());
        } else {
            cache = (UploadObjectCache) SerializationUtil.deserialize(bs);
            if (cache.getUserid() != userid) {
                throw new ServiceException(INVALID_UPLOAD_ID);
            }
        }
        return cache;
    }
}
