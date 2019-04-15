package com.ytfs.service;

import static com.ytfs.service.ServerConfig.privateKey;
import static com.ytfs.service.ServerConfig.superNodeID;
import static com.ytfs.service.ServerConfig.port;
import com.ytfs.service.dao.MongoSource;
import com.ytfs.service.dao.RedisSource;
import com.ytfs.service.net.P2PUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ServerInitor {

    private static final Logger LOG = Logger.getLogger(ServerInitor.class);

    public static void stop() {
        P2PUtils.stop();
        MongoSource.terminate();
        RedisSource.terminate();
    }

    public static void init() {
        try {
            LogConfigurator.configPath(new File("logs"), "INFO");
            load();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);//循环初始化
        }
        for (int ii = 0; ii < 10; ii++) {
            try {
                int port = ServerConfig.port + ii;
                P2PUtils.start(port, ServerConfig.privateKey);
                P2PUtils.register();
                LOG.info("P2P initialization completed, port " + port);
                break;
            } catch (Exception r) {
                LOG.info("P2P initialization failed!", r);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                }
                P2PUtils.stop();
            }
        }
    }

    private static void load() throws IOException {
        InputStream is = ClientInitor.class.getResourceAsStream("/server.properties");
        if (is == null) {
            throw new IOException("No properties file could be found for ytfs service");
        }
        Properties p = new Properties();
        p.load(is);
        is.close();
        try {
            String ss = p.getProperty("superNodeID");
            superNodeID = Integer.parseInt(ss);
            if (superNodeID < 1 || superNodeID > 31) {
                throw new IOException();
            }
        } catch (IOException | NumberFormatException d) {
            throw new IOException("The 'superNodeID' parameter is not configured.");
        }
        privateKey = p.getProperty("privateKey");
        if (privateKey == null || privateKey.trim().isEmpty()) {
            throw new IOException("The 'privateKey' parameter is not configured.");
        }
        try {
            String ss = p.getProperty("port").trim();
            port = Integer.parseInt(ss);
        } catch (NumberFormatException d) {
            throw new IOException("The 'port' parameter is not configured.");
        }
    }
}
