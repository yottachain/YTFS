package com.ytfs.service.servlet;

import com.ytfs.service.*;
import com.ytfs.service.codec.BlockEncrypted;
import com.ytfs.service.dao.*;
import com.ytfs.service.node.*;
import static com.ytfs.service.packet.ServiceErrorCode.*;
import com.ytfs.service.packet.*;
import java.util.*;
import org.bson.Document;

public class UploadBlockHandler {

    /**
     * 数据块上传完毕
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp complete(UploadBlockEndReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(req.getVBI());
        Map<Integer, UploadShardCache> caches = CacheAccessor.getUploadShardCache(req.getVBI());
        List<Document> ls = req.verify(caches, cache.getShardcount(), req.getVBI());
        ShardAccessor.saveShardMetas(ls);
        BlockMeta meta = req.makeBlockMeta(req.getVBI(), cache.getShardcount());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = req.makeSaveObjectMetaReq(userid, meta.getVBI(), cache.getVNU());
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            UploadObjectCache.setBlockNum(cache.getVNU(), req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }

    /**
     * 将数据块小于PL2的数据块写入数据库
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp saveToDB(UploadBlockDBReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        if (progress.exists(req.getVNU(), req.getId())) {
            return new VoidResp();
        }
        BlockEncrypted b = new BlockEncrypted();
        b.setData(req.getData());
        req.verify(b.getVHB());
        BlockMeta meta = req.makeBlockMeta(Sequence.generateBlockID(1));
        BlockAccessor.saveBlockData(meta.getVBI(), req.getData());
        BlockAccessor.saveBlockMeta(meta);
        SaveObjectMetaReq saveObjectMetaReq = req.makeSaveObjectMetaReq(userid, meta.getVBI());
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            UploadObjectCache.setBlockNum(req.getVNU(), req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }

    /**
     * 重复的数据块,引用计数+1,写用户元数据
     *
     * @param req
     * @param userid
     * @return OK
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp repeat(UploadBlockDupReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        if (progress.exists(req.getVNU(), req.getId())) {
            return new VoidResp();
        }
        BlockMeta meta = BlockAccessor.getBlockMeta(req.getVHP(), req.getVHB());
        if (meta == null) {
            throw new ServiceException(NO_SUCH_BLOCK);
        }
        req.verify();
        BlockAccessor.incBlockNLINK(meta);//+1
        SaveObjectMetaReq saveObjectMetaReq = req.makeSaveObjectMetaReq(userid, meta.getVBI());
        try {
            SaveObjectMetaResp resp = SuperReqestHandler.saveObjectMetaCall(saveObjectMetaReq);
            UploadObjectCache.setBlockNum(req.getVNU(), req.getId());
            if (resp.isExists()) {
                BlockAccessor.decBlockNLINK(meta);//-1
            }
        } catch (ServiceException r) {
            BlockAccessor.decBlockNLINK(meta);//-1
            throw r;
        }
        return new VoidResp();
    }

    /**
     * 检查数据块是否重复,或强制请求分配node
     *
     * @param req
     * @param userid
     * @return 0已上传 1重复 2未上传
     * @throws ServiceException
     * @throws Throwable
     */
    static Object init(UploadBlockInitReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        if (req.getShardCount() > 255) {
            throw new ServiceException(TOO_MANY_SHARDS);
        }
        Node n = SuperNodeList.getBlockSuperNode(req.getVHP());
        if (n.getNodeId() != ServerConfig.superNodeID) {//验证数据块是否对应
            throw new ServiceException(ILLEGAL_VHP_NODEID);
        }
        UploadObjectCache progress = CacheAccessor.getUploadObjectCache(userid, req.getVNU());
        UploadBlockInitResp resp = new UploadBlockInitResp();
        if (progress.exists(req.getVNU(), req.getId())) {
            return new VoidResp();
        }
        if (req instanceof UploadBlockInit2Req) {
            distributeNode(req, resp, user.getKUEp());
            return resp;
        }
        List<BlockMeta> ls = BlockAccessor.getBlockMeta(req.getVHP());
        if (ls.isEmpty()) {
            distributeNode(req, resp, user.getKUEp());
            return resp;
        } else {
            UploadBlockDupResp resp2 = new UploadBlockDupResp();
            resp2.setKEDANDVHB(ls);
            return resp2;
        }
    }

    /**
     * 请求补发数据分片
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static UploadBlockSubResp subUpload(UploadBlockSubReq req, User user) throws ServiceException, Throwable {
        UploadBlockCache cache = CacheAccessor.getUploadBlockCache(req.getVBI());
        List<UploadShardRes> fails = new ArrayList();
        Map<Integer, UploadShardCache> caches = CacheAccessor.getUploadShardCache(req.getVBI());
        UploadShardRes[] ress = req.getRes();
        for (UploadShardRes res : ress) {
            if (res.getRES() == UploadShardRes.RES_OK) {
                continue;
            }
            UploadShardCache ca = caches.get(res.getSHARDID());
            if (ca == null) {
                if (res.getRES() == UploadShardRes.RES_NETIOERR) {//需要惩罚节点
                    if (cache.getNodes()[res.getSHARDID()] == res.getNODEID()) {
                        NodeManager.punishNode(res.getNODEID());
                    }
                }
                fails.add(res);
            } else {
                if (ca.getRes() == UploadShardRes.RES_OK) {
                    continue;
                }
                if (res.getRES() == UploadShardRes.RES_NO_SPACE && res.getRES() == ca.getRes()) {
                    NodeManager.noSpace(res.getNODEID());
                }
                fails.add(res);
            }
        }
        UploadBlockSubResp resp = new UploadBlockSubResp();
        if (fails.isEmpty()) {
            CacheAccessor.clearCache(cache.getVNU(), req.getVBI());//清除缓存
            return resp;
        }
        Node[] nodes = NodeManager.getNode(fails.size());
        if (nodes.length != fails.size()) {
            throw new ServiceException(SERVER_ERROR);
        }
        resp.setNodes(nodes, fails, req.getVBI(), cache);
        CacheAccessor.putUploadBlockCache(cache, req.getVBI());
        return resp;
    }

    //分配节点
    private static void distributeNode(UploadBlockInitReq req, UploadBlockInitResp resp, byte[] userkey) throws Exception {
        if (req.getShardCount() > 0) {//需要数据库
            Node[] nodes = NodeManager.getNode(req.getShardCount());
            if (nodes.length != req.getShardCount()) {
                throw new ServiceException(SERVER_ERROR);
            }
            long blockid = Sequence.generateBlockID(req.getShardCount());
            resp.setNodes(nodes, blockid);
            UploadBlockCache cache = new UploadBlockCache(nodes, req.getShardCount());
            cache.setUserKey(userkey);
            cache.setVNU(req.getVNU());
            CacheAccessor.setUploadBlockCache(cache, blockid);
        }
    }
}
