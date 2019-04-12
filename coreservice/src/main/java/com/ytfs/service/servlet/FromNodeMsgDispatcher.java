package com.ytfs.service.servlet;

import com.ytfs.service.node.NodeCache;
import com.ytfs.service.packet.SerializationUtil;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadShardResp;
import io.yottachain.p2phost.interfaces.NodeCallback;
import org.apache.log4j.Logger;

public class FromNodeMsgDispatcher implements NodeCallback {

    private static final Logger LOG = Logger.getLogger(FromNodeMsgDispatcher.class);

    @Override
    public byte[] onMessageFromNode(byte[] data, String nodekey) {
        int nodeId = NodeCache.getNodeId(nodekey);
        Object message = SerializationUtil.deserialize(data);
        LOG.debug("request:" + message.getClass().getSimpleName());
        Object response = null;
        try {
            if (message instanceof UploadShardResp) {
                response = UploadShardHandler.uploadShardResp((UploadShardResp) message, nodeId);
            }
            return SerializationUtil.serialize(response);
        } catch (ServiceException s) {
            return SerializationUtil.serialize(s);
        } catch (Throwable r) {
            ServiceException se = new ServiceException(ServiceErrorCode.SERVER_ERROR, r.getMessage());
            return SerializationUtil.serialize(se);
        }
    }

}
