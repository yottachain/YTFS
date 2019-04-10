package com.ytfs.service.net;

public interface P2PMessageListener {

    /**
     * 接收到来自用户端的请求
     *
     * @param data
     * @param key
     * @return　byte[]
     */
    public byte[] onMessageFromUser(byte[] data, String key);

    /**
     * 接收到来自其他超级节点的请求
     *
     * @param data
     * @param key
     * @return byte[]
     */
    public byte[] onMessageFromSuperNode(byte[] data, String key);

    /**
     * 接收到来自存储节点的请求
     *
     * @param data
     * @param key
     * @return byte[]
     */
    public byte[] onMessageFromNode(byte[] data, String key);

}
