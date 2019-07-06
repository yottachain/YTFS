package com.ytfs.service.node;

public class Node {

    //节点ID
    private int nodeId;
    //地址列表
    private String[] addr;
    //加密或解签用公钥
    private String key;

    /**
     * @return the nodeId
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return the addr
     */
    public String[] getAddr() {
        return addr;
    }

    /**
     * @param addr the addr to set
     */
    public void setAddr(String[] addr) {
        this.addr = addr;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

}
