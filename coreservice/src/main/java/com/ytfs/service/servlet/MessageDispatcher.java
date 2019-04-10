package com.ytfs.service.servlet;

import com.ytfs.service.codec.Base58;
import com.ytfs.service.dao.User;
import com.ytfs.service.dao.UserCache;
import com.ytfs.service.net.P2PMessageListener;
import com.ytfs.service.node.NodeCache;
import com.ytfs.service.packet.DownloadObjectInitReq;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.ListSuperNodeReq;
import com.ytfs.service.packet.ListSuperNodeResp;
import com.ytfs.service.packet.QueryObjectMetaReq;
import com.ytfs.service.packet.SaveObjectMetaReq;
import com.ytfs.service.packet.SerializationUtil;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.UploadBlockEndReq;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockSubReq;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadShardResp;

public class MessageDispatcher implements P2PMessageListener {

    @Override
    public byte[] onMessageFromUser(byte[] data, String userkey) {
        User user = UserCache.getUser(Base58.decode(userkey));
        if (user == null) {
            ServiceException se = new ServiceException(ServiceErrorCode.INVALID_USER_ID);
            return SerializationUtil.serialize(se);
        }
        Object message = SerializationUtil.deserialize(data);
        Object response = null;
        try {
            if (message instanceof ListSuperNodeReq) {
                response = ListSuperNodeResp.newInstance();
            } else if (message instanceof UploadObjectInitReq) {
                response = UploadObjectHandler.init((UploadObjectInitReq) message, user);
            } else if (message instanceof UploadBlockInitReq) {
                response = UploadBlockHandler.init((UploadBlockInitReq) message, user);
            } else if (message instanceof UploadBlockDupReq) {
                response = UploadBlockHandler.repeat((UploadBlockDupReq) message, user);
            } else if (message instanceof UploadBlockDBReq) {
                response = UploadBlockHandler.saveToDB((UploadBlockDBReq) message, user);
            } else if (message instanceof UploadBlockSubReq) {
                response = UploadBlockHandler.subUpload((UploadBlockSubReq) message, user);
            } else if (message instanceof UploadBlockEndReq) {
                response = UploadBlockHandler.complete((UploadBlockEndReq) message, user);
            } else if (message instanceof UploadObjectEndReq) {
                response = UploadObjectHandler.complete((UploadObjectEndReq) message, user);
            } else if (message instanceof DownloadObjectInitReq) {
                response = DownloadHandler.init((DownloadObjectInitReq) message, user);
            } else if (message instanceof DownloadBlockInitReq) {
                response = DownloadHandler.getBlockMeta((DownloadBlockInitReq) message, user);
            }
            return SerializationUtil.serialize(response);
        } catch (ServiceException s) {
            return SerializationUtil.serialize(s);
        } catch (Throwable r) {
            ServiceException se = new ServiceException(ServiceErrorCode.SERVER_ERROR, r.getMessage());
            return SerializationUtil.serialize(se);
        }
    }

    @Override
    public byte[] onMessageFromSuperNode(byte[] data, String nodekey) {
        Object message = SerializationUtil.deserialize(data);
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

    @Override
    public byte[] onMessageFromNode(byte[] data, String nodekey) {
        int nodeId = NodeCache.getNodeId(nodekey);
        Object message = SerializationUtil.deserialize(data);
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
