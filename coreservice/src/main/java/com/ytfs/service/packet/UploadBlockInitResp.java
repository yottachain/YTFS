package com.ytfs.service.packet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.NodeManager;
import java.nio.ByteBuffer;
import java.security.PrivateKey;
import java.security.Signature;

public class UploadBlockInitResp {

    private ShardNode[] nodes;
    private long VBI = 0;

    public void setNodes(Node[] ns, long VBI) {
        this.VBI = VBI;
        byte[] key = NodeManager.getSuperNodePrivateKey(ServerConfig.superNodeID);
        PrivateKey privateKey = (PrivateKey) KeyStoreCoder.rsaPrivateKey(key);
        this.nodes = new ShardNode[ns.length];
        for (int ii = 0; ii < ns.length; ii++) {
            nodes[ii] = new ShardNode(ii, ns[ii]);
            try {    //对id和VNU签名
                Signature signet = java.security.Signature.getInstance("DSA");
                signet.initSign(privateKey);
                String str = ns[ii].getKey() + VBI;
                signet.update(str.getBytes());
                byte[] signed = signet.sign();
                nodes[ii].setSign(signed);
            } catch (Exception r) {
                throw new IllegalArgumentException(r.getMessage());
            }
        }
    }

    /**
     * @return the VBI
     */
    public long getVBI() {
        return VBI;
    }

    /**
     * @param VBI the VBI to set
     */
    public void setVBI(long VBI) {
        this.VBI = VBI;
    }

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
}
