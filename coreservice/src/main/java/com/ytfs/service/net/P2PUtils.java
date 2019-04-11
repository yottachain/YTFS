package com.ytfs.service.net;

import com.ytfs.service.node.Node;
import com.ytfs.service.packet.SerializationUtil;
import static com.ytfs.service.packet.ServiceErrorCode.INTERNAL_ERROR;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.servlet.FromBPMsgDispatcher;
import com.ytfs.service.servlet.FromNodeMsgDispatcher;
import com.ytfs.service.servlet.FromUserMsgDispatcher;
import io.yottachain.p2phost.YottaP2P;
import io.yottachain.p2phost.core.exception.P2pHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class P2PUtils {

    private static final List<String> CONNECTS = Collections.synchronizedList(new ArrayList());

    /**
     * 初始化P2P工具
     *
     * @param port
     * @param privateKey
     * @throws java.lang.Exception
     */
    public static void start(int port, String privateKey) throws Exception {
        YottaP2P.start(port, privateKey);
    }

    public static void register() {
        YottaP2P.registerUserCallback(new FromUserMsgDispatcher());
        YottaP2P.registerBPNodeCallback(new FromBPMsgDispatcher());
        YottaP2P.registerNodeCallback(new FromNodeMsgDispatcher());
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
            try {
                YottaP2P.connect(node.getKey(), node.getAddr());
            } catch (P2pHostException ex) {
                throw new ServiceException(INTERNAL_ERROR, ex.getMessage());
            }
            CONNECTS.add(node.getKey());
        }
        byte[] data = SerializationUtil.serialize(obj);
        byte[] bs = null;
        try {                    //访问p2p网络
            switch (type) {
                case MSG_2BPU:
                    bs = YottaP2P.sendToBPUMsg(node.getKey(), data);
                    break;
                case MSG_2BP:
                    bs = YottaP2P.sendToBPMsg(node.getKey(), data);
                    break;
                default:
                    bs = YottaP2P.sendToNodeMsg(node.getKey(), data);
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

    /**
     * 销毁
     */
    public static void stop() {
        try {
            YottaP2P.close();
        } catch (P2pHostException ex) {
        }
    }
}
