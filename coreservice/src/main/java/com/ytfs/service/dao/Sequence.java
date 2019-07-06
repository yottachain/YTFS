package com.ytfs.service.dao;

import com.mongodb.MongoException;
import com.mongodb.client.model.Filters;
import static com.ytfs.service.dao.MongoSource.SEQ_BLKID_VAR;
import static com.ytfs.service.dao.MongoSource.SEQ_UID_VAR;
import org.bson.Document;
import org.bson.conversions.Bson;

public class Sequence {

    /**
     * 生成一个唯一UserID序列号
     *
     * @return int
     */
    public static int getSequence() {
        Bson filter = Filters.eq("_id", SEQ_UID_VAR);
        Document update = new Document("$inc", new Document("seq", (int) 1));
        Document doc = MongoSource.getSeqCollection().findOneAndUpdate(filter, update);
        if (doc == null) {
            throw new MongoException("Sequence deleted.");
        }               
        if (doc.get("seq") instanceof Long) {
            return doc.getLong("seq").intValue();
        } else {
            return doc.getInteger("seq");
        }
    }

    /**
     * 生成一个BlockID序列号的低32位
     *
     * @param inc
     * @return
     */
    public static int getSequence(int inc) {
        Bson filter = Filters.eq("_id", SEQ_BLKID_VAR);
        Document update = new Document("$inc", new Document("seq", (int) inc));
        Document doc = MongoSource.getSeqCollection().findOneAndUpdate(filter, update);
        if (doc == null) {
            throw new MongoException("Sequence deleted.");
        }
        if (doc.get("seq") instanceof Long) {
            return doc.getLong("seq").intValue();
        } else {
            return doc.getInteger("seq");
        }
    }

    /**
     * 生成UserID
     *
     * @return INT32
     */
    public static int generateUserID() {
        return getSequence() + 1;
    }

    /**
     * 生成数据块,分片ID
     *
     * @param shardCount
     * @return INT64
     */
    public static long generateBlockID(int shardCount) {
        int h = (int) (System.currentTimeMillis() / 1000);
        int l = getSequence(shardCount);
        long high = (h & 0x000000ffffffffL) << 32;  //高32位
        long low = (++l) & 0x00000000ffffffffL;
        return high | low;
    }

}
