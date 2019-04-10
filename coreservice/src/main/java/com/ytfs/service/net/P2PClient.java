package com.ytfs.service.net;

import com.ytfs.service.node.Node;
import com.ytfs.service.packet.SerializationUtil;
import static com.ytfs.service.packet.ServiceErrorCode.INTERNAL_ERROR;
import com.ytfs.service.packet.ServiceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class P2PClient {

    private static final List<String> CONNECTS = Collections.synchronizedList(new ArrayList());

    /**
     * 初始化P2P工具
     *
     * @param listener
     * @return 初始化失败，可返回-1，成功0
     */
    public static int start(P2PMessageListener listener) {
        try {
            //初始化p2p网络
            return 0;
        } catch (Exception e) {
            return -1;
        }
    }

    public static final int MSG_2BPU = 0;
    public static final int MSG_2BP = 1;
    public static final int MSG_2NODE = 2;

    public static Object requestBPU(Object obj, Node node) throws ServiceException {
        return request(obj, node, MSG_2BPU);
    }

    public static Object requestBP(Object obj, Node node) throws ServiceException {
        return request(obj, node, MSG_2BP);
    }

    public static Object requestNode(Object obj, Node node) throws ServiceException {
        return request(obj, node, MSG_2NODE);
    }

    public static Object request(Object obj, Node node, int type) throws ServiceException {
        if (!CONNECTS.contains(node.getKey())) {
            connect(node.getKey(), node.getAddr());
            CONNECTS.add(node.getKey());
        }
        byte[] data = SerializationUtil.serialize(obj);
        byte[] bs = null;
        try {                    //访问p2p网络
            switch (type) {
                case MSG_2BPU:
                    bs = sendToBPUMsg(node.getKey(), data);
                    break;
                case MSG_2BP:
                    bs = sendToBPMsg(node.getKey(), data);
                    break;
                default:
                    bs = sendToNodeMsg(node.getKey(), data);
                    break;
            }
        } catch (Throwable e) {
            throw new ServiceException(INTERNAL_ERROR, e.getMessage());
        }
        Object res = SerializationUtil.deserialize(bs);
        if (res instanceof ServiceException) {
            throw (ServiceException) res;
        }
        return res;
    }

    public static void connect(String nodeid, String[] addrs) {
    }

    public static byte[] sendToBPUMsg(String nodekey, byte[] msg) throws Throwable {
        return null;
    }

    public static byte[] sendToBPMsg(String nodekey, byte[] msg) throws Throwable {
        return null;
    }

    public static byte[] sendToNodeMsg(String nodekey, byte[] msg) throws Throwable {
        return null;
    }

    /**
     * 销毁
     */
    public static void stop() {
        //销毁P2P
    }
}
