package com.ytfs.service.node;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.UserConfig;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.packet.ListSuperNodeReq;
import com.ytfs.service.packet.ListSuperNodeResp;
import com.ytfs.service.packet.ServiceException;

public class SuperNodeList {

    static Node[] superList = null;

    /**
     * 客户端获取node列表
     *
     * @return
     */
    private static Node[] getSuperNodeList() {
        if (superList != null) {
            return superList;
        }
        synchronized (SuperNodeList.class) {
            if (superList == null) {
                ListSuperNodeReq req = new ListSuperNodeReq();
                try {
                    ListSuperNodeResp res = (ListSuperNodeResp) P2PUtils.requestBPU(req, UserConfig.superNode);
                    superList = res.getSuperList();
                } catch (ServiceException ex) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex1) {
                    }
                }
            }
        }
        return superList;
    }

    /**
     * 获取数据块所属超级节点编号
     *
     * @param src
     * @return　0-32;
     */
    public static Node getBlockSuperNode(byte[] src) {
        int value = (int) (((src[0] & 0xFF) << 24)
                | ((src[1] & 0xFF) << 16)
                | ((src[2] & 0xFF) << 8)
                | (src[3] & 0xFF));
        value = value & 0x0FFFF;
        Node[] nodes = getSuperNodeList();
        int index = value % nodes.length;
        return nodes[index];
    }

    public static Node getBlockSuperNode(int id) {
        Node[] nodes = getSuperNodeList();
        return nodes[id];
    }

    public static Node getBlockSuperNodeByUserId(int userid) {
        Node[] nodes = getSuperNodeList();
        int index = userid % nodes.length;
        return nodes[index];
    }

    public static Node getLocalNode() {
        Node[] nodes = getSuperNodeList();
        return nodes[ServerConfig.superNodeID];
    }
}
