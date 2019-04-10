package com.ytfs.service.dao;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import java.io.InputStream;
import java.util.*;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoSource {

    private static final String DATABASENAME = "metabase";
    //自增编号生成器
    private static final String SEQ_TABLE_NAME = "sequence";
    public static final int SEQ_UID_VAR = 0;
    public static final int SEQ_BLKID_VAR = 1;
    //用户表
    public static final String USER_TABLE_NAME = "users";
    private static final String USER_INDEX_NAME = "KUEp";
    //用户文件去重表
    public static final String OBJECT_TABLE_NAME = "objects";
    private static final String OBJECT_INDEX_NAME = "VNU";
    public static final String OBJECT_NEW_TABLE_NAME = "objects_new";

    //数据块源信息表
    public static final String BLOCK_TABLE_NAME = "blocks";
    private static final String BLOCK_INDEX_VHP_VHB = "VHP_VHB";//唯一
    public static final String BLOCK_DAT_TABLE_NAME = "blocks_data";
    //分片元数据
    public static final String SHARD_TABLE_NAME = "shards";

    private static MongoSource source = null;

    private static void newInstance() {
        if (source != null) {
            return;
        }
        try {
            synchronized (MongoSource.class) {
                if (source == null) {
                    source = new MongoSource();
                }
            }
        } catch (Exception r) {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException ex) {
            }
            throw new MongoException(r.getMessage());
        }
    }

    static MongoCollection<Document> getSeqCollection() {
        newInstance();
        return source.seq_collection;
    }

    static MongoCollection<Document> getUserCollection() {
        newInstance();
        return source.user_collection;
    }

    static MongoCollection<Document> getObjectCollection() {
        newInstance();
        return source.object_collection;
    }

    static MongoCollection<Document> getObjectNewCollection() {
        newInstance();
        return source.object_new_collection;
    }

    static MongoCollection<Document> getBlockCollection() {
        newInstance();
        return source.block_collection;
    }

    static MongoCollection<Document> getBlockDatCollection() {
        newInstance();
        return source.block_dat_collection;
    }

    static MongoCollection<Document> getShardCollection() {
        newInstance();
        return source.shard_collection;
    }

    public static void terminate() {
        synchronized (MongoSource.class) {
            if (source != null) {
                source.client.close();
                source = null;
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(MongoSource.class);
    private MongoClient client = null;
    private MongoDatabase database;
    private MongoCollection<Document> seq_collection;
    private MongoCollection<Document> user_collection = null;
    private MongoCollection<Document> object_collection = null;
    private MongoCollection<Document> object_new_collection = null;
    private MongoCollection<Document> block_collection = null;
    private MongoCollection<Document> block_dat_collection = null;
    private MongoCollection<Document> shard_collection = null;

    private MongoSource() throws MongoException {
        try (InputStream inStream = MongoSource.class.getResourceAsStream("/mongo.properties")) {
            Properties p = new Properties();
            p.load(inStream);
            init(p);
            init_seq_collection();
            init_user_collection();
            init_object_collection();
            init_block_collection();
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            throw e instanceof MongoException ? (MongoException) e : new MongoException(e.getMessage());
        }
    }

    private ServerAddress toAddress(String host) {
        if (host.trim().isEmpty()) {
            return null;
        }
        String[] addr = host.trim().split(":");
        try {
            return new ServerAddress(addr[0], Integer.parseInt(addr[1]));
        } catch (NumberFormatException d) {
            LOG.warn("无效的服务器地址:[" + host + "]");
            return null;
        }
    }

    private void init(Properties p) {
        String hostlist = p.getProperty("serverlist");
        if (hostlist == null || hostlist.trim().isEmpty()) {
            throw new MongoException("MongoSource.properties文件中没有指定serverlist");
        }
        String[] hosts = hostlist.trim().split(",");
        List<ServerAddress> addrs = new ArrayList<>();
        for (String host : hosts) {
            ServerAddress addr = toAddress(host);
            if (addr != null) {
                addrs.add(addr);
                LOG.info("[" + addr.toString() + "]添加到服务器列表中...");
            }
        }
        if (addrs.size() < 3) {
            throw new MongoException("Mongo至少得有3台服务器");
        }
        MongoCredential credential = null;
        String username = p.getProperty("username", "").trim();
        if (!username.isEmpty()) {
            String password = p.getProperty("password", "").trim();
            credential = MongoCredential.createScramSha1Credential(username, "admin", password.toCharArray());
        }
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        if (credential != null) {
            builder = builder.credential(credential);
        }
        String ssl = p.getProperty("ssl", "false").trim();
        if (ssl.equalsIgnoreCase("true")) {
            builder = builder.applyToSslSettings(build -> build.enabled(true));
        }
        String compressors = p.getProperty("compressors", "").trim();
        if (!compressors.isEmpty()) {
            List<MongoCompressor> comps = new ArrayList<>();
            if (compressors.toLowerCase().contains("zlib")) {
                comps.add(MongoCompressor.createZlibCompressor());
            }
            if (compressors.toLowerCase().contains("snappy")) {
                comps.add(MongoCompressor.createSnappyCompressor());
            }
            if (!comps.isEmpty()) {
                builder = builder.compressorList(comps);
            }
        }
        MongoClientSettings settings = builder.applyToClusterSettings(build -> build.hosts(addrs)).build();
        client = MongoClients.create(settings);
        LOG.info("连接服务器成功!");
        database = client.getDatabase(DATABASENAME);
    }

    private void init_seq_collection() {
        seq_collection = database.getCollection(SEQ_TABLE_NAME);
        Bson bson = Filters.eq("_id", SEQ_UID_VAR); //为生成userID 
        Document doc = seq_collection.find(bson).first();
        if (doc == null) {
            doc = new Document();
            doc.append("_id", SEQ_UID_VAR);
            doc.append("seq", (int) 0);
            seq_collection.insertOne(doc);
        }
        bson = Filters.eq("_id", SEQ_BLKID_VAR); //为生成blockID
        doc = seq_collection.find(bson).first();
        if (doc == null) {
            doc = new Document();
            doc.append("_id", SEQ_BLKID_VAR);
            doc.append("seq", (int) 0);
            seq_collection.insertOne(doc);
        }
    }

    private void init_user_collection() {
        user_collection = database.getCollection(USER_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = user_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(USER_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(USER_INDEX_NAME);
            object_collection.createIndex(Indexes.ascending("KUEp"), indexOptions);
        }
        LOG.info("创建用户表!");
    }

    private void init_object_collection() {
        object_collection = database.getCollection(OBJECT_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = object_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(OBJECT_INDEX_NAME)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(OBJECT_INDEX_NAME);
            object_collection.createIndex(Indexes.ascending("VNU"), indexOptions);
        }
        object_new_collection = database.getCollection(OBJECT_NEW_TABLE_NAME);
        LOG.info("创建用户去重表!");
    }

    private void init_block_collection() {
        block_collection = database.getCollection(BLOCK_TABLE_NAME);
        boolean indexCreated = false;
        ListIndexesIterable<Document> indexs = block_collection.listIndexes();
        for (Document index : indexs) {
            if (index.get("name").equals(BLOCK_INDEX_VHP_VHB)) {
                indexCreated = true;
                break;
            }
        }
        if (!indexCreated) {
            IndexOptions indexOptions = new IndexOptions().unique(true);
            indexOptions = indexOptions.name(BLOCK_INDEX_VHP_VHB);
            block_collection.createIndex(Indexes.ascending("VHP", "VHB"), indexOptions);
        }
        block_dat_collection = database.getCollection(BLOCK_DAT_TABLE_NAME);
        shard_collection = database.getCollection(SHARD_TABLE_NAME);
        LOG.info("创建数据块META表!");
    }

}
