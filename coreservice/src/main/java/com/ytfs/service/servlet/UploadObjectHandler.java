package com.ytfs.service.servlet;

import com.ytfs.service.ServerConfig;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.dao.ObjectAccessor;
import com.ytfs.service.dao.ObjectMeta;
import com.ytfs.service.dao.User;
import com.ytfs.service.eos.EOSClient;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.ServiceErrorCode;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import com.ytfs.service.packet.VoidResp;
import java.util.List;
import org.bson.types.ObjectId;

public class UploadObjectHandler {

    /**
     * 上传对象完毕
     *
     * @param req
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static VoidResp complete(UploadObjectEndReq req, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        ObjectMeta meta = new ObjectMeta(userid, req.getVHW());
        ObjectAccessor.getObjectAndUpdateNLINK(meta);
        ObjectAccessor.addNewObject(meta.getVNU());
        List<ObjectRefer> refers = ObjectRefer.parse(meta.getBlocks());
        long size = 0;
        for (ObjectRefer refer : refers) {
            size = size + refer.getRealSize();
        }
        size = ServerConfig.PMS + size;
        EOSClient eos = new EOSClient(userid);
        eos.freeHDD(meta.getLength());
        eos.deductHDD(size);
        return new VoidResp();
    }

    /**
     * 初始化上传对象
     *
     * @param ud
     * @param userid
     * @return
     * @throws ServiceException
     * @throws Throwable
     */
    static UploadObjectInitResp init(UploadObjectInitReq ud, User user) throws ServiceException, Throwable {
        int userid = user.getUserID();
        Node n = SuperNodeList.getBlockSuperNodeByUserId(userid);
        if (n.getNodeId() != ServerConfig.superNodeID) {
            throw new ServiceException(ServiceErrorCode.INVALID_USER_ID);
        }
        if (ud.getVHW() == null || ud.getVHW().length != 32) {
            throw new ServiceException(ServiceErrorCode.INVALID_VHW);
        }
        ObjectMeta meta = new ObjectMeta(userid, ud.getVHW());
        boolean exists = ObjectAccessor.isObjectExists(meta);
        UploadObjectInitResp resp = new UploadObjectInitResp(false, meta.getVNU());
        if (exists) {
            int nlink = meta.getNLINK();
            if (nlink == 0) {//正在上传               
                resp.setBlocks(meta.getBlocks());
            } else {
                ObjectAccessor.incObjectNLINK(meta);
                return new UploadObjectInitResp(true);
            }
        }
        EOSClient eos = new EOSClient(user.getEosID());
        boolean hasspace = eos.hasSpace(ud.getLength(), ServerConfig.PMS);
        if (!hasspace) {
            throw new ServiceException(ServiceErrorCode.NOT_ENOUGH_DHH);
        }
        if (!exists) {
            meta.setVNU(new ObjectId());
            meta.setNLINK(0);
            ObjectAccessor.addObject(meta);
        }
        eos.frozenHDD(ud.getLength());
        return new UploadObjectInitResp(false, meta.getVNU());
    }
}
