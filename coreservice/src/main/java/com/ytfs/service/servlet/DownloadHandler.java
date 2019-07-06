package com.ytfs.service.servlet;

import com.ytfs.service.dao.BlockAccessor;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.ShardAccessor;
import com.ytfs.service.dao.ShardMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.NodeManager;
import com.ytfs.service.packet.DownloadBlockDBResp;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadObjectInitReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.ServiceException;

public class DownloadHandler {

    /**
     * 获取对象引用块meta
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static DownloadObjectInitResp init(DownloadObjectInitReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        ObjectMeta meta = ObjectAccessor.getObject(userid, req.getVHW());
        DownloadObjectInitResp resp = new DownloadObjectInitResp();
        resp.setRefers(meta.getBlocks());
        return resp;
    }

    /**
     * 获取对象引用块meta
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static Object getBlockMeta(DownloadBlockInitReq req, User user) throws ServiceException, Throwable {
        int vnf = BlockAccessor.getBlockMetaVNF(req.getVBI());
        if (vnf == 0) {//存储在数据库
            byte[] dat = BlockAccessor.readBlockData(req.getVBI());
            DownloadBlockDBResp res = new DownloadBlockDBResp();
            res.setData(dat);
            return res;
        }
        DownloadBlockInitResp resp = new DownloadBlockInitResp();
        resp.setVNF(vnf);
        int len = vnf > 0 ? vnf : (vnf * -1);
        ShardMeta[] metas = ShardAccessor.getShardMeta(req.getVBI(), len);
        int[] nodeids = new int[metas.length];
        byte[][] VHF = new byte[metas.length][];
        for (int ii = 0; ii < nodeids.length; ii++) {
            nodeids[ii] = metas[ii].getNodeId();
            VHF[ii] = metas[ii].getVHF();
        }
        resp.setVHF(VHF);
        Node[] nodes = NodeManager.getNode(nodeids);
        resp.setNodes(nodes);
        return resp;
    }
}
