package com.ytfs.service.servlet;

import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.SaveObjectMetaReq;
import com.ytfs.service.packet.SerializationUtil;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import io.yottachain.p2phost.interfaces.BPNodeCallback;
import org.apache.log4j.Logger;

public class FromBPMsgDispatcher implements BPNodeCallback {

    private static final Logger LOG = Logger.getLogger(FromBPMsgDispatcher.class);

    @Override
    public byte[] onMessageFromBPNode(byte[] bytes, String string) {
        Object message = SerializationUtil.deserialize(bytes);
        LOG.debug("request:" + message.getClass().getSimpleName());
        Object response = null;
        try {
            if (message instanceof QueryObjectMetaReq) {
                response = SuperReqestHandler.queryObjectMeta((QueryObjectMetaReq) message);
            } else if (message instanceof SaveObjectMetaReq) {
                response = SuperReqestHandler.saveObjectMeta((SaveObjectMetaReq) message);
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
