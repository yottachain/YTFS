package com.ytfs.service.packet;

import com.ytfs.service.servlet.UploadObjectCache;
import com.ytfs.service.node.Node;
import java.io.IOException;

public class ProtobufTest {

    public static void main(String[] args) throws IOException {
        UploadObjectCache resp = new UploadObjectCache();
        resp.setFilesize(111);
        resp.setUserid(22);
        byte[] bs = SerializationUtil.serialize(resp);

        System.out.println(bs.length);

        Object obj = SerializationUtil.deserialize(bs);
        if (obj instanceof UploadObjectCache) {
            UploadObjectCache res = (UploadObjectCache) obj;
            System.out.println(res.getUserid());
        }

        ServiceException err = new ServiceException(777);
        bs = SerializationUtil.serialize(err);
        System.out.println(bs.length);

        obj = SerializationUtil.deserialize(bs);
        if (obj instanceof ServiceException) {
            ServiceException err1 = (ServiceException) obj;
            System.out.println(err1.getErrorCode());
            System.out.println(err1.getMessage());
        }
    }
}
