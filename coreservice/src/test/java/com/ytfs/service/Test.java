package com.ytfs.service;

public class Test {

    public static void main(String[] args) throws Exception {
        int ii = 23442;

        System.out.println(ii);

        byte[] bs = new byte[2];
        Function.short2bytes((short) ii, bs, 0);
        
        int newii=(short) Function.bytes2Integer(bs, 0, 2);

        //int newii = (short)Function.bytes2Integer(bs,0,2);
        System.out.println(newii);

  

    }
}
