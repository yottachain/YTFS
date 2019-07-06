package com.ytfs.service.node;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;

public class NodeCache {

    private static final long MAX_SIZE = 100000;
    private static final long WRITE_EXPIRED_TIME = 10;
    private static final long READ_EXPIRED_TIME = 10;

    private static final Cache<String, Integer> superNodes = CacheBuilder.newBuilder()
            .expireAfterWrite(WRITE_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();
    private static final Cache<String, Integer> nodes = CacheBuilder.newBuilder()
            .expireAfterWrite(WRITE_EXPIRED_TIME, TimeUnit.MINUTES)
            .expireAfterAccess(READ_EXPIRED_TIME, TimeUnit.MINUTES)
            .maximumSize(MAX_SIZE)
            .build();

    public static int getSuperNodeId(String key) {
        Integer id = superNodes.getIfPresent(key);
        if (id == null) {
            id = NodeManager.getSuperNodeIDByPubKey(key);
            superNodes.put(key, id);
        }
        return id;
    }

    public static int getNodeId(String key) {
        Integer id = nodes.getIfPresent(key);
        if (id == null) {
            id = NodeManager.getNodeIDByPubKey(key);
            superNodes.put(key, id);
        }
        return id;
    }
}
