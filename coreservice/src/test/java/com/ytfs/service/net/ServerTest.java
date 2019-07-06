package com.ytfs.service.net;

import com.ytfs.service.packet.SerializationUtil;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadShardReq;
import com.ytfs.service.packet.UploadShardResp;
import io.yottachain.p2phost.YottaP2P;
import io.yottachain.p2phost.core.exception.P2pHostException;
import io.yottachain.p2phost.interfaces.BPNodeCallback;
import io.yottachain.p2phost.interfaces.NodeCallback;
import io.yottachain.p2phost.interfaces.UserCallback;
import java.io.IOException;

public class ServerTest {

    public static void main(String[] args) throws P2pHostException, IOException {
        YottaP2P.start(8888, "5JdDoNZwSADC3KG7xCh7mCF62fKp86sLf3GUNDY2B8t2UUB9HdJ");
        YottaP2P.registerUserCallback(userCallback);
        System.out.println(YottaP2P.id());
        //打印监听地址列表
        String[] addrs = YottaP2P.addrs();
        for (String str : addrs) {
            System.out.println(str);
        }
        
        System.in.read();
    }

    static UserCallback userCallback = new UserCallback() {
        @Override
        public byte[] onMessageFromUser(byte[] bytes, String string) {
            System.out.println(string);

            Object obj = SerializationUtil.deserialize(bytes);
            if (obj instanceof UploadShardReq) {
                UploadShardReq req = (UploadShardReq) obj;
                System.out.println(req.getBPDID());
                UploadShardResp resp = new UploadShardResp();
                resp.setRES(101);
                resp.setVBI(0);
                return SerializationUtil.serialize(resp);
            } else {
                ServiceException se = new ServiceException(1000);
                return SerializationUtil.serialize(se);
            }
        }
    };

    static BPNodeCallback bpnodeCallback = new BPNodeCallback() {
        @Override
        public byte[] onMessageFromBPNode(byte[] bytes, String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };

    static NodeCallback nodeCallback = new NodeCallback() {
        @Override
        public byte[] onMessageFromNode(byte[] bytes, String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };
}
