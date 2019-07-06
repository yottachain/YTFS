package com.ytfs.service;

import static com.ytfs.service.UploadShardRes.RES_NETIOERR;
import static com.ytfs.service.UserConfig.UPLOADSHARDTHREAD;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.ShardNode;
import com.ytfs.service.packet.UploadShard2CResp;
import com.ytfs.service.packet.UploadShardReq;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;

public class UploadShard implements Runnable {

    private static final Logger LOG = Logger.getLogger(UploadShard.class);
    private static final ArrayBlockingQueue<UploadShard> queue;

    static {
        int num = UPLOADSHARDTHREAD > 50 ? 50 : UPLOADSHARDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new UploadShard());
        }
    }

    static void startUploadShard(UploadShardReq req, ShardNode node, UploadBlock uploadBlock) throws InterruptedException {
        UploadShard uploader = queue.take();
        uploader.node = node.getNode();
        uploader.req = req;
        uploader.uploadBlock = uploadBlock;
        GlobleThreadPool.execute(uploader);
    }

    private UploadShardReq req;
    private Node node;
    private UploadBlock uploadBlock;

    @Override
    public void run() {
        try {
            UploadShardRes res = new UploadShardRes();
            res.setSHARDID(req.getSHARDID());
            res.setNODEID(node.getNodeId());
            try {
                UploadShard2CResp resp = (UploadShard2CResp) P2PUtils.requestNode(req, node);
                res.setRES(resp.getRES());
            } catch (ServiceException ex) {
                LOG.error("Network error.");
                res.setRES(RES_NETIOERR);
            }
            uploadBlock.onResponse(res);
        } finally {
            queue.add(this);
        }
    }

}
