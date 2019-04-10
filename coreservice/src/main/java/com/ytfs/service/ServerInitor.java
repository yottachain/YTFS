package com.ytfs.service;

import static com.ytfs.service.ServerConfig.superNodeID;
import com.ytfs.service.net.P2PClient;
import com.ytfs.service.servlet.MessageDispatcher;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ServerInitor {

    public static void init() {
        try {
            LogConfigurator.configPath();
            load();
            int i = P2PClient.start(new MessageDispatcher());
            if (i != 0) {
                throw new Exception("P2P initialization failed.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);//循环初始化
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

    }
}
