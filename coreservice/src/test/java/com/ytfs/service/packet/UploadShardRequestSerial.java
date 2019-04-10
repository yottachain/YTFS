package com.ytfs.service.packet;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.commons.codec.binary.Hex;

public class UploadShardRequestSerial {

    public static void main(String[] args) throws IOException {
        Message.UploadShardRequest.Builder builder = Message.UploadShardRequest.newBuilder();
        builder.setSHARDID(5);
        builder.setBPDID(1);
        builder.setBPDSIGN(ByteString.copyFrom("aa".getBytes()));
        builder.setDAT(ByteString.copyFrom("bb".getBytes()));
        builder.setUSERSIGN(ByteString.copyFrom("cc".getBytes()));
        builder.setVBI(2);
        builder.setVHF(ByteString.copyFrom("dd".getBytes()));
        Message.UploadShardRequest info = builder.build();
        byte[] result = info.toByteArray();
        String ss = Hex.encodeHexString(result);
        System.out.println(ss);

        UploadShardReq req = new UploadShardReq();
        req.setSHARDID(5);
        req.setBPDID(1);
        req.setBPDSIGN("aa".getBytes());
        req.setDAT("bb".getBytes());
        req.setUSERSIGN("cc".getBytes());
        req.setVBI(2);
        req.setVHF("dd".getBytes());
        byte[] result1 = SerializationUtil.serialize(req);
        String ss1 = Hex.encodeHexString(result1);
        System.out.println(ss1);

    }
}
