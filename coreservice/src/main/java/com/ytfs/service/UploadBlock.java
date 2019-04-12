package com.ytfs.service;

import static com.ytfs.service.UploadShardRes.RES_OK;
import com.ytfs.service.codec.Block;
import com.ytfs.service.codec.BlockAESEncryptor;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.codec.Shard;
import com.ytfs.service.codec.ShardRSEncoder;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import static com.ytfs.service.packet.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadBlockSubResp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UploadBlock {

    private ShardRSEncoder rs;
    private Block block;
    private final short id;
    private final ShardNode[] nodes;
    private final long VBI;
    private final Node bpdNode;
    private final List<UploadShardRes> resList = new ArrayList();
    private final Map<Integer, Shard> map = new HashMap();

    public UploadBlock(Block block, short id, ShardNode[] nodes, long VBI, Node bpdNode) {
        this.block = block;
        this.id = id;
        this.nodes = nodes;
        this.VBI = VBI;
        this.bpdNode = bpdNode;
    }

    void onResponse(UploadShardRes res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    void upload() throws ServiceException, InterruptedException {
        try {
            byte[] ks = KeyStoreCoder.generateRandomKey();
            BlockAESEncryptor aes = new BlockAESEncryptor(block, ks);
            aes.encrypt();
            rs = new ShardRSEncoder(aes.getBlockEncrypted());
            rs.encode();
            firstUpload();
            subUpload();
            completeUploadBlock(ks);
        } catch (Exception r) {
            throw new ServiceException(SERVER_ERROR);
        }
    }

    private void completeUploadBlock(byte[] ks) throws ServiceException {
        UploadBlockEndReq req = new UploadBlockEndReq();
        req.setId(id);
        req.setVBI(VBI);
        req.setVHP(block.getVHP());
        req.setVHB(rs.makeVHB());
        req.setKEU(KeyStoreCoder.rsaEncryped(ks, UserConfig.KUEp));
        req.setKED(KeyStoreCoder.encryped(ks, block.getKD()));
        req.setOriginalSize(block.getOriginalSize());
        req.setRealSize(block.getRealSize());
        req.setRsShard(rs.getShardList().get(0).isRsShard());
        P2PUtils.requestBPU(req, bpdNode);
    }

    private void firstUpload() throws InterruptedException {
        List<Shard> shards = rs.getShardList();
        int nodeindex = 0;
        for (Shard sd : shards) {
            map.put(nodeindex, sd);
            ShardNode n = nodes[nodeindex];
            UploadShardReq req = new UploadShardReq();
            req.setBPDID(bpdNode.getNodeId());
            req.setBPDSIGN(n.getSign());
            req.setDAT(sd.getData());
            req.setSHARDID(nodeindex);
            req.setVBI(VBI);
            req.setVHF(sd.getVHF());
            req.sign(nodes[nodeindex].getNodeId());
            UploadShard.startUploadShard(req, n, this);
            nodeindex++;
        }
        synchronized (this) {
            if (resList.size() != shards.size()) {
                this.wait(1000 * 15);
            }
        }
    }

    private void subUpload() throws InterruptedException, ServiceException {
        while (true) {
            UploadBlockSubReq uloadBlockSubReq = doUploadShardRes();
            if (uloadBlockSubReq == null) {
                return;
            }
            UploadBlockSubResp resp = (UploadBlockSubResp) P2PUtils.requestBPU(uloadBlockSubReq, bpdNode);
            if (resp.getNodes() == null || resp.getNodes().length == 0) {
                return;
            }
            secondUpload(resp);
        }
    }

    private void secondUpload(UploadBlockSubResp resp) throws InterruptedException {
        ShardNode[] shardNodes = resp.getNodes();
        for (ShardNode n : shardNodes) {
            Shard sd = map.get(n.getShardid());
            UploadShardReq req = new UploadShardReq();
            req.setBPDID(bpdNode.getNodeId());
            req.setBPDSIGN(n.getSign());
            req.setDAT(sd.getData());
            req.setSHARDID(n.getShardid());
            req.setVBI(VBI);
            req.setVHF(sd.getVHF());
            req.sign(n.getNodeId());
            UploadShard.startUploadShard(req, n, this);
        }
        synchronized (this) {
            if (resList.size() != shardNodes.length) {
                this.wait(1000 * 15);
            }
        }
    }

    private UploadBlockSubReq doUploadShardRes() {
        List<UploadShardRes> ls = new ArrayList();
        for (UploadShardRes res : resList) {
            if (res.getRES() != RES_OK) {
                ls.add(res);
            }
        }
        resList.clear();
        if (ls.isEmpty()) {
            return null;
        } else {
            UploadShardRes[] ress = new UploadShardRes[ls.size()];
            ress = ls.toArray(ress);
            UploadBlockSubReq subreq = new UploadBlockSubReq();
            subreq.setRes(ress);
            subreq.setVBI(VBI);
            return subreq;
        }
    }
}
