package com.ytfs.service.codec;

import org.apache.commons.codec.binary.Hex;

public class Test {

    public static void main(String[] args) {
        byte[] key = "1234567890ab1234567890abcdefcdef".getBytes();
        Block b = new Block("asfaxvbfsebgser".getBytes());
        System.out.println(b.getData().length);

        BlockAESEncryptor code = new BlockAESEncryptor(b, key);
        code.encrypt();
        System.out.println(code.getData().length);
        byte[] hash = code.getVHB();
        
        System.out.println(Hex.encodeHexString(hash));

        code = new BlockAESEncryptor(b, key);
        code.encrypt();
        hash = code.getVHB();
        System.out.println(code.getData().length);
        System.out.println(Hex.encodeHexString(hash));
        
        code = new BlockAESEncryptor(b, key);
        code.encrypt();
        hash = code.getVHB();
        System.out.println(code.getData().length);
        System.out.println(Hex.encodeHexString(hash));

    }
}
