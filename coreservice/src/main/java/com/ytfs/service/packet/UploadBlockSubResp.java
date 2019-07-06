package com.ytfs.service.packet;

import com.ytfs.service.servlet.UploadBlockCache;
import com.ytfs.service.ServerConfig;
import com.ytfs.service.UploadShardRes;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.NodeManager;
import java.nio.ByteBuffer;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.List;

public class UploadBlockSubResp {

    /**
     * @return the nodes
     */
    public ShardNode[] getNodes() {
        return nodes;
    }

    /**
     * @param nodes the nodes to set
     */
    public void setNodes(ShardNode[] nodes) {
        this.nodes = nodes;
    }
    private ShardNode[] nodes;

    public void setNodes(Node[] ns, List<UploadShardRes> fails, long VBI, UploadBlockCache cache) {
        byte[] key = NodeManager.getSuperNodePrivateKey(ServerConfig.superNodeID);
        PrivateKey privateKey = (PrivateKey) KeyStoreCoder.rsaPrivateKey(key);
        this.nodes = new ShardNode[ns.length];
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ns[ii]);
            nodes[ii].setShardid(fails.get(ii).getSHARDID());
            try {    //对id和VNU签名
                Signature signet = java.security.Signature.getInstance("DSA");
                signet.initSign(privateKey);
                ByteBuffer bs = ByteBuffer.allocate(12);
                bs.putInt(nodes[ii].getNodeId());
                bs.putLong(VBI);
                bs.flip();
                signet.update(bs.array());
                byte[] signed = signet.sign();
                nodes[ii].setSign(signed);
            } catch (Exception r) {
                throw new IllegalArgumentException(r.getMessage());
            }
            cache.getNodes()[fails.get(ii).getSHARDID()] = nodes[ii].getNodeId();//更新缓存
        }
    }
}
