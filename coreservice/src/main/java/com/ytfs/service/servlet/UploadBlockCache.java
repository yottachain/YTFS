package com.ytfs.service.servlet;

import com.ytfs.service.Function;
import com.ytfs.service.node.Node;
import java.util.Arrays;
import org.bson.types.ObjectId;

public class UploadBlockCache {

    private byte[] userKey;
    private ObjectId VNU;
    private int[] nodes;//为该块分配的节点
    private int shardcount;//该数据块分片数 

    public UploadBlockCache() {
    }

    public UploadBlockCache(Node[] nodes, int shardcount) {
        this.shardcount = shardcount;
        setNodes(nodes);
    }

    private void setNodes(Node[] nodes) {
        this.nodes = new int[nodes.length];
        int ii = 0;
        for (Node n : nodes) {
            this.nodes[ii++] = n.getNodeId();
        }
    }

    /**
     * @return the nodes
     */
    public int[] getNodes() {
        return nodes;
    }

    /**
     * @param nodes the nodes to set
     */
    public void setNodes(int[] nodes) {
        this.nodes = nodes;
    }

    /**
     * @return the userid
     */
    public byte[] getUserKey() {
        return userKey;
    }

    /**
     * @param userKey the userid to set
     */
    public void setUserKey(byte[] userKey) {
        this.userKey = userKey;
    }

    /**
     * @return the VNU
     */
    public ObjectId getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(ObjectId VNU) {
        this.VNU = VNU;
    }

    /**
     * @return the shardcount
     */
    public int getShardcount() {
        return shardcount;
    }

    /**
     * @param shardcount the shardcount to set
     */
    public void setShardcount(int shardcount) {
        this.shardcount = shardcount;
    }

    public static byte[] getCacheKey1(long VBI) {
        byte[] key = new byte[10];
        System.arraycopy(Function.long2bytes(VBI), 0, key, 0, 8);
        Arrays.fill(key, 8, 9, (byte) 0x01);
        return key;
    }

    public static byte[] getCacheKey2(long VBI) {
        byte[] key = new byte[10];
        System.arraycopy(Function.long2bytes(VBI), 0, key, 0, 8);
        Arrays.fill(key, 8, 9, (byte) 0x02);
        return key;
    }
}
