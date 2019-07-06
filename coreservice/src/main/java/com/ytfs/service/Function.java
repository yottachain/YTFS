package com.ytfs.service;

public class Function {

    /**
     * byte[]->short
     *
     * @param bs
     * @return int
     */
    public static short bytes2Short(byte[] bs) {
        if (bs == null || bs.length < 1 || bs.length > 2) {
            throw new java.lang.IllegalArgumentException();
        }
        return (short) bytes2Integer(bs, 0, 2);
    }

    /**
     * byte[]->int
     *
     * @param bs
     * @return int
     */
    public static int bytes2int(byte[] bs) {
        if (bs == null || bs.length < 1 || bs.length > 4) {
            throw new java.lang.IllegalArgumentException();
        }
        return (int) bytes2Integer(bs, 0, 4);
    }

    /**
     * byte[]->long
     *
     * @param bs
     * @return long
     */
    public static long bytes2long(byte[] bs) {
        if (bs == null || bs.length < 1 || bs.length > 8) {
            throw new java.lang.IllegalArgumentException();
        }
        return bytes2Integer(bs, 0, 8);
    }

    public static long bytes2Integer(byte[] bs, int off, int len) {
        long num = 0;
        for (int ix = off, len1 = off + len, len2 = bs.length; ix < len1 && ix < len2; ++ix) {
            num <<= 8;
            num |= (bs[ix] & 0xff);
        }
        return num;
    }

    /*
    public static void integer2Bytes(long n, byte[] bs, int off, int len) {
        if (bs == null || bs.length < off + len) {
            throw new java.lang.IllegalArgumentException();
        }
        for(int ii=0;ii<len;ii++){
            bs[off+ii]=(byte)(n >>> )
            
        }
    }*/

    /**
     * short->byte[]
     *
     * @param n
     * @param bs
     * @param off
     */
    public static void short2bytes(short n, byte[] bs, int off) {
        if (bs == null || bs.length < off + 2) {
            throw new java.lang.IllegalArgumentException();
        }
        bs[off] = (byte) (n >>> 8);
        bs[++off] = (byte) (n);
    }

    /**
     * int->byte[]
     *
     * @param n
     * @param bs
     * @param off
     */
    public static void int2bytes(int n, byte[] bs, int off) {
        if (bs == null || bs.length < off + 4) {
            throw new java.lang.IllegalArgumentException();
        }
        bs[off] = (byte) (n >>> 24);
        bs[++off] = (byte) (n >>> 16);
        bs[++off] = (byte) (n >>> 8);
        bs[++off] = (byte) (n);
    }

    /**
     * int->byte[]
     *
     * @param n
     * @param bs
     * @param off
     */
    public static void long2bytes(long n, byte[] bs, int off) {
        if (bs == null || bs.length < off + 8) {
            throw new java.lang.IllegalArgumentException();
        }
        bs[off] = (byte) (n >>> 56);
        bs[++off] = (byte) (n >>> 48);
        bs[++off] = (byte) (n >>> 40);
        bs[++off] = (byte) (n >>> 32);
        bs[++off] = (byte) (n >>> 24);
        bs[++off] = (byte) (n >>> 16);
        bs[++off] = (byte) (n >>> 8);
        bs[++off] = (byte) (n);
    }

    /**
     * short->byte[]
     *
     * @param num
     * @return byte[]
     */
    public static byte[] short2bytes(short num) {
        byte[] result = new byte[2];
        short2bytes(num, result, 0);
        return result;
    }

    /**
     * int->byte[]
     *
     * @param num
     * @return byte[]
     */
    public static byte[] int2bytes(int num) {
        byte[] result = new byte[4];
        int2bytes(num, result, 0);
        return result;
    }

    /**
     * long->byte
     *
     * @param num
     * @return byte[]
     */
    public static byte[] long2bytes(long num) {
        byte[] result = new byte[8];
        long2bytes(num, result, 0);
        return result;
    }
}
