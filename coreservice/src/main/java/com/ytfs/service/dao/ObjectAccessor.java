package com.ytfs.service.dao;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class ObjectAccessor {
    
    public static void addNewObject(ObjectId id) {
        Document update = new Document("_id", id);
        update.append("time", System.currentTimeMillis());
        MongoSource.getObjectNewCollection().insertOne(update);
    }
    
    public static void addObject(ObjectMeta meta) {
        MongoSource.getObjectCollection().insertOne(meta.toDocument());
    }
    
    public static void incObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("$inc", new Document("NLINK", 1));
        MongoSource.getObjectCollection().updateOne(filter, update);
    }
    
    public static void getObjectAndUpdateNLINK(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("NLINK", 1);
        Document doc = MongoSource.getObjectCollection().findOneAndUpdate(filter, update);
        if (doc != null) {
            meta.setNLINK(doc.getInteger("NLINK"));
            meta.setVNU(doc.getObjectId("VNU"));
            meta.setBlocks(((Binary) doc.get("blocks")).getData());
        }
    }
    
    public static void decObjectNLINK(ObjectMeta meta) {
        if (meta.getNLINK() >= 255) {
            return;
        }
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document update = new Document("$inc", new Document("NLINK", -1));
        MongoSource.getObjectCollection().updateOne(filter, update);
    }
    
    public static boolean isObjectExists(ObjectMeta meta) {
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document fields = new Document("NLINK", 1);
        fields.append("VNU", 1);
        fields.append("blocks", 1);
        Document doc = MongoSource.getObjectCollection().find(filter).projection(fields).first();
        if (doc == null) {
            return false;
        } else {
            meta.setNLINK(doc.getInteger("NLINK"));
            meta.setVNU(doc.getObjectId("VNU"));
            meta.setBlocks(((Binary) doc.get("blocks")).getData());
            return true;
        }
    }
    
    public static void updateObject(ObjectId VNU, byte[] blocks) {
        Bson filter = Filters.eq("VNU", VNU);
        Document update = new Document("blocks", new Binary(blocks));
        MongoSource.getObjectCollection().updateOne(filter, update);
    }
    
    public static ObjectMeta getObject(ObjectId VNU) {
        Bson filter = Filters.eq("VNU", VNU);
        Document doc = MongoSource.getObjectCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(doc);
        }
    }
    
    public static ObjectMeta getObject(int uid, byte[] VHW) {
        ObjectMeta meta = new ObjectMeta(uid, VHW);
        Bson filter = Filters.eq("_id", new Binary(meta.getId()));
        Document doc = MongoSource.getObjectCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new ObjectMeta(doc);
        }
    }
}
