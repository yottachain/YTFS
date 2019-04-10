package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import static com.ytfs.service.UploadShardRes.RES_BAD_REQUEST;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadShardResp;
import com.ytfs.service.packet.VoidResp;

public class UploadShardHandler {


    /**
     * BPD收到存储节点的存储反馈
     *
     * @param resp
     * @param nodeid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp uploadShardResp(UploadShardResp resp, int nodeid) throws ServiceException, Throwable {
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(resp.getVBI());
        User user = UserCache.getUser(cache.getUserKey());
        if (user == null) {
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        resp.verify(user.getKUEp(), cache.getShardcount(), nodeid);
        if (cache.getNodes()[resp.getSHARDID()] != nodeid) {
            throw new ServiceException(ServiceErrorCode.INVALID_NODE_ID);
        }
        if (resp.getRES() == RES_BAD_REQUEST) {
            long failtimes = CacheAccessor.getUploadBlockINC(resp.getVBI());
            if (failtimes >= ServerConfig.PNF) {
                UploadObjectCache objcache = CacheAccessor.getUploadObjectCache(nodeid, cache.getVNU());
                EOSClient eos = new EOSClient(user.getEosID());
                eos.punishHDD(objcache.getFilesize());
                CacheAccessor.clearCache(cache.getVNU(), resp.getVBI());//清除缓存
                return new VoidResp();
            }
        }
        UploadShardCache shardCache = new UploadShardCache();
        shardCache.setNodeid(nodeid);
        shardCache.setRes(resp.getRES());
        shardCache.setVHF(resp.getVHF());
        CacheAccessor.addUploadShardCache(shardCache, resp.getVBI());
        return new VoidResp();
    }

}
