package com.ytfs.service;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerRepository;

public class LogConfigurator {

    public static void configPath() throws IOException {
        configPath(null, "INFO");
    }

    private static JdkLogHandler handler;

    public synchronized static void configPath(File path, String lv) throws IOException {
        if (handler == null) {
            handler = new JdkLogHandler();
            PropertyConfigurator configurator = new PropertyConfigurator();
            Properties logproperties = new Properties();
            logproperties.load(LogConfigurator.class.getResourceAsStream("/log4j.properties"));
            if (lv != null) {
                if (lv.equalsIgnoreCase("DEBUG") || lv.equalsIgnoreCase("INFO") || lv.equalsIgnoreCase("WARN") || lv.equalsIgnoreCase("ERROR")) {
                    lv = lv.toUpperCase();
                } else {
                    lv = "INFO";
                }
            } else {
                lv = "INFO";
            }
            if (path == null) {
                logproperties.replace("log4j.rootCategory", lv + ",stdout");
                logproperties.replace("log4j.rootLogger", lv + ",stdout");
            } else {
                logproperties.replace("log4j.rootCategory", lv + ",logDailyFile,stdout");
                logproperties.replace("log4j.rootLogger", lv + ",logDailyFile,stdout");
                logproperties.replace("log4j.appender.logDailyFile.Threshold", lv);
                logproperties.replace("log4j.appender.logDailyFile.File", path.getAbsolutePath());
            }
            LoggerRepository lr = LogManager.getLoggerRepository();
            configurator.doConfigure(logproperties, lr);
            handler.setLevel(java.util.logging.Level.ALL);
            Enumeration<String> lognames = java.util.logging.LogManager.getLogManager().getLoggerNames();
            while (lognames.hasMoreElements()) {
                String name = lognames.nextElement();
                java.util.logging.Logger logger = java.util.logging.LogManager.getLogManager().getLogger(name);
                java.util.logging.Handler[] handlers = logger.getHandlers();
                for (java.util.logging.Handler h : handlers) {
                    logger.removeHandler(h);
                }
                logger.addHandler(handler);
            }
        }
    }

    /**
     * @return the log
     */
    public static org.apache.log4j.Logger getLog() {
        return org.apache.log4j.Logger.getRootLogger();
    }

    public static class JdkLogHandler extends Handler {

        /**
         * Close the <tt>Handler</tt> and free all associated resources.
         *
         * @throws SecurityException if a security manager exists and if the
         * caller does not have <tt>LoggingPermission("control")</tt>.
         */
        @Override
        public void close() throws SecurityException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        /**
         * Flush any buffered output.
         */
        @Override
        public void flush() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void publish(LogRecord record) {
            java.util.logging.Level level = record.getLevel();
            org.apache.log4j.Level log4jlv;
            if (level.equals(java.util.logging.Level.SEVERE)) {
                log4jlv = org.apache.log4j.Level.ERROR;
            } else if (level.equals(java.util.logging.Level.WARNING)) {
                log4jlv = org.apache.log4j.Level.WARN;
            } else if (level.equals(java.util.logging.Level.INFO)) {
                log4jlv = org.apache.log4j.Level.INFO;
            } else {
                log4jlv = org.apache.log4j.Level.DEBUG;
            }
            if (getLog().isEnabledFor(log4jlv)) {
                String msg = record.getParameters() == null ? record.getMessage() : MessageFormat.format(record.getMessage(), record.getParameters());
                getLog().log(record.getSourceClassName(), log4jlv, msg, null);
            }
        }
    }

}
