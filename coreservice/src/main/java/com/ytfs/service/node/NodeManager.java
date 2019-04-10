package com.ytfs.service.node;

public class NodeManager {

    /**
     * 获取超级节点列表,包括节点编号,加密或解签用公钥,接口地址
     *
     * @return Node[]
     */
    public static Node[] getSuperNode() {
        return null;
    }

    /**
     * 获取超级节点对应的私钥,一般超级节点对数据进行签名时需要
     *
     * @param id
     * @return byte[]
     */
    public static byte[] getSuperNodePrivateKey(int id) {
        return null;
    }

    /**
     * 获取存储节点
     *
     * @param shardCount 根据某种算法分配shardCount个存储节点用来存储分片
     * @return
     */
    public static Node[] getNode(int shardCount) {
        return null;
    }

    /**
     * 获取节点
     *
     * @param nodeids
     * @return
     */
    public static Node[] getNode(int[] nodeids) {
        return null;
    }

    public static int getNodeIDByPubKey(String key) {
        return 0;
    }

    public static int getSuperNodeIDByPubKey(String key) {
        return 0;
    }

    /**
     * 节点吊线,需要惩罚
     *
     * @param nodeid
     */
    public static void punishNode(int nodeid) {
    }

    /**
     * 通报节点空间不足
     *
     * @param nodeid
     */
    public static void noSpace(int nodeid) {

    }

    /**
     * 向该存储节点对应的超级节点BPM发送消息,BPM记录该存储节点已经存储了VHF数据分片，
     * 相应增加该存储节点的已使用空间计数数据存储容量，但无需增加该存储节点的单位收益每周期收益
     *
     * @param nodeid
     * @param VHF
     */
    public static void recodeBPM(int nodeid, byte[] VHF) {

    }

}
