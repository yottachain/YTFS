package com.ytfs.service;

public class ServerConfig {

    //*************************不可配置参数*************************************
    //每个文件在上传后必须存储的最短周期，例如60天，以避免频繁存储删除数据导致的系统负载过重
    public final static long PMS = 60L * 24L * 60L * 60L * 1000L;

    //REDIS存储key的过期时间
    public final static int REDIS_EXPIRE = 60 * 60 * 10;

    //REDIS存储key的过期时间
    public final static int REDIS_BLOCK_EXPIRE = 60 * 60;

    //小于PL2的数据块，直接记录在元数据库中
    public final static int PL2 = 256;

    //存储节点验签失败,拒绝存储,超过3次,惩罚
    public final static int PNF = 3;

    //**************************可配置参数********************************
    //服务端超级节点编号,本服务节点编号
    public static int superNodeID;

    //私钥
    public static String privateKey;

    //端口
    public static int port;
}
