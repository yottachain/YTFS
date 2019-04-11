package com.ytfs.service;

import static com.ytfs.service.UserConfig.DOWNLOADSHARDTHREAD;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import com.ytfs.service.packet.ServiceException;
import java.util.concurrent.ArrayBlockingQueue;

public class DownloadShare implements Runnable {

    private static final ArrayBlockingQueue<DownloadShare> queue;

    static {
        int num = DOWNLOADSHARDTHREAD > 50 ? 50 : DOWNLOADSHARDTHREAD;
        num = num < 5 ? 5 : num;
        queue = new ArrayBlockingQueue(num);
        for (int ii = 0; ii < num; ii++) {
            queue.add(new DownloadShare());
        }
    }

    static void startDownloadShard(byte[] VHF, Node node, DownloadBlock downloadBlock) throws InterruptedException {
        DownloadShare downloader = queue.take();
        downloader.req = new DownloadShardReq();
        downloader.req.setVHF(VHF);
        downloader.downloadBlock = downloadBlock;
        downloader.node = node;
        GlobleThreadPool.execute(downloader);
    }

    private DownloadShardReq req;
    private Node node;
    private DownloadBlock downloadBlock;

    @Override
    public void run() {
        try {
            DownloadShardResp resp = new DownloadShardResp();
            try {
                resp = (DownloadShardResp) P2PUtils.requestNode(req, node);
            } catch (ServiceException ex) {
            }
            downloadBlock.onResponse(resp);
        } finally {
            queue.add(this);
        }
    }
}
