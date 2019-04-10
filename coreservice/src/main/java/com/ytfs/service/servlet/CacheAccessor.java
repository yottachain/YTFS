package com.ytfs.service.servlet;

import static com.ytfs.service.Function.long2bytes;
import static com.ytfs.service.ServerConfig.REDIS_BLOCK_EXPIRE;
import static com.ytfs.service.ServerConfig.REDIS_EXPIRE;
import com.ytfs.service.dao.RedisSource;
import com.ytfs.service.net.P2PClient;
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
import redis.clients.jedis.JedisCluster;

public class CacheAccessor {

    /**
     * 清除一次对象上传的所有session
     *
     * @param VNU
     * @param VBI
     */
    static void clearCache(ObjectId VNU, long VBI) {
        JedisCluster jedis = RedisSource.getJedisCluster();
        byte[] key1 = UploadObjectCache.getCacheKey(VNU);
        byte[] key2 = VNU.toByteArray();
        byte[] key3 = long2bytes(VBI);
        byte[] key4 = UploadBlockCache.getCacheKey1(VBI);
        byte[] key5 = UploadBlockCache.getCacheKey2(VBI);
        jedis.del(new byte[][]{key1, key2, key3, key4, key5});
    }

    /**
     * 获取对应数据块已上传的分片信息
     *
     * @param VBI
     * @return
     */
    static Map<Integer, UploadShardCache> getUploadShardCache(long VBI) {
        JedisCluster jedis = RedisSource.getJedisCluster();
        byte[] data = jedis.get(UploadBlockCache.getCacheKey1(VBI));
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
        JedisCluster jedis = RedisSource.getJedisCluster();
        jedis.append(UploadBlockCache.getCacheKey1(VBI), shardCache.toByte());
    }

    /**
     * 更新数据块cache
     *
     * @param cache
     * @param VBI
     */
    static void putUploadBlockCache(UploadBlockCache cache, long VBI) {
        JedisCluster jedis = RedisSource.getJedisCluster();
        jedis.setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
    }

    /**
     * 初始化数据块cache
     *
     * @param cache
     * @param VBI
     */
    static void setUploadBlockCache(UploadBlockCache cache, long VBI) {
        JedisCluster jedis = RedisSource.getJedisCluster();
        jedis.setex(long2bytes(VBI), REDIS_BLOCK_EXPIRE, SerializationUtil.serialize(cache));
        jedis.setex(UploadBlockCache.getCacheKey1(VBI), REDIS_BLOCK_EXPIRE, new byte[0]);

    }

    /**
     * 获取数据块cache
     *
     * @param VBI
     * @return
     * @throws ServiceException
     */
    static UploadBlockCache getUploadBlockCache(long VBI) throws ServiceException {
        byte[] bs = RedisSource.getJedisCluster().get(long2bytes(VBI));
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
        JedisCluster jedis = RedisSource.getJedisCluster();
        byte[] key = UploadBlockCache.getCacheKey2(VBI);
        long l = jedis.incr(key);
        jedis.expire(key, REDIS_BLOCK_EXPIRE);
        return l;
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
        JedisCluster jedis = RedisSource.getJedisCluster();
        byte[] bs = jedis.get(VNU.toByteArray());
        UploadObjectCache cache;
        if (bs == null) {
            QueryObjectMetaReq req = new QueryObjectMetaReq();
            req.setUserID(userid);
            req.setVNU(VNU);
            Node node = SuperNodeList.getBlockSuperNodeByUserId(userid);
            QueryObjectMetaResp resp = (QueryObjectMetaResp) P2PClient.requestBP(req, node);
            cache = new UploadObjectCache();
            cache.setFilesize(resp.getLength());
            cache.setUserid(userid);
            jedis.setex(VNU.toByteArray(), REDIS_EXPIRE, SerializationUtil.serialize(cache));
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
