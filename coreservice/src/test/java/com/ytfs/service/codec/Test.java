package com.ytfs.service.codec;

import org.apache.commons.codec.binary.Hex;

public class Test {

    public static void main(String[] args) {
        String keystr="GZsJqUv51pw4c5HnBHiStK3jwJKXZjdtxVwkEShR9Ljb7ZUN1T";
        
        byte[] keybs= Base58.decode(keystr);
        
        byte[] key = "1234567890ab1234567890abcdefcdef".getBytes();
        Block b = new Block("asfaxvbfsebgser".getBytes());
        System.out.println(b.getData().length);

      
    }
}
