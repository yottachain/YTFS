package com.ytfs.service.packet;

import com.ytfs.service.node.Node;
import com.ytfs.service.node.NodeManager;

public class ListSuperNodeResp {

    public static ListSuperNodeResp newInstance() {
        ListSuperNodeResp resp = new ListSuperNodeResp();
        resp.setSuperList(NodeManager.getSuperNode());
        return resp;
    }

    /**
     * @return the superList
     */
    public Node[] getSuperList() {
        return superList;
    }

    /**
     * @param superList the superList to set
     */
    public void setSuperList(Node[] superList) {
        this.superList = superList;
    }
    private Node[] superList;
}
