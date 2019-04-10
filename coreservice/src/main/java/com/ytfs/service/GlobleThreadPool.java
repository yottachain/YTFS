package com.ytfs.service;

import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class GlobleThreadPool {

    private static final Logger log = Logger.getLogger(GlobleThreadPool.class);
    private static final int MAXIMUN_POOLSIZE = 1000;
    private static final ThreadPoolExecutor POOL;

    static {
        POOL = new ThreadPoolExecutor(0, MAXIMUN_POOLSIZE, 180, TimeUnit.SECONDS,
                new SynchronousQueue(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.DiscardOldestPolicy());
        POOL.allowCoreThreadTimeOut(true);
    }

    public static void shutdown() {
        POOL.shutdown();
    }

    public static String ThreadPoolInfo() {
        return "ActiveCount:" + POOL.getActiveCount()
                + ",CompletedTaskCount:" + POOL.getCompletedTaskCount()
                + ",CorePoolSize:" + POOL.getCorePoolSize()
                + ",MaximumPoolSize:" + POOL.getMaximumPoolSize()
                + ",Queue:" + POOL.getQueue().size();
    }

    public static void execute(Runnable command) {
        try {
            POOL.execute(command);
        } catch (Throwable s) {
            log.error("ThreadPool ERR:", s);
        }
    }

    public static void stopThread(Thread t) {
        if (t != null) {
            t.interrupt();
            try {
                t.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
