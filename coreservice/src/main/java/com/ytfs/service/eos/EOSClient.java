package com.ytfs.service.eos;

public class EOSClient {

    private final long eosID;//eos帐户ID
    //private final String password;  //需要密码？

    public EOSClient(long eosID) {
        this.eosID = eosID;
    }

    /**
     * 该用户是否有足够的HDD用于存储该数据最短存储时间PMS（例如60天）
     *
     * @param length 数据长度
     * @param PMS 最短存储时间PMS(单位ms)
     * @return true：有足够空间，false：没有
     * @throws java.lang.Throwable
     */
    public boolean hasSpace(long length, long PMS) throws Throwable {
        return true;
    }

    /**
     * 冻结该用户相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void frozenHDD(long length) throws Throwable {
    }

    /**
     * 释放相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void freeHDD(long length) throws Throwable {
    }

    /**
     * 扣除相应的HDD
     * @param length
     * @throws Throwable 
     */
    public void deductHDD(long length) throws Throwable {

    }

    /**
     * 没收该用户相应的HDD
     *
     * @param length
     * @throws Throwable
     */
    public void punishHDD(long length) throws Throwable {
    }

}
