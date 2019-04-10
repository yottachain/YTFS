package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import static com.ytfs.service.packet.ServiceErrorCode.SERVER_ERROR;
import com.ytfs.service.packet.ServiceException;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

public class BlockAccessor {

    public static void saveBlockMeta(BlockMeta meta) {
        MongoSource.getBlockCollection().insertOne(meta.toDocument());
    }

    public static void saveBlockData(long vbi, byte[] dat) {
        Document doc = new Document();
        doc.append("_id", vbi);
        doc.append("dat", new Binary(dat));
        MongoSource.getBlockDatCollection().insertOne(doc);
    }

    public static byte[] readBlockData(long vbi) {
        Bson filter = Filters.eq("_id", vbi);
        Document doc = MongoSource.getBlockCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return ((Binary) doc.get("dat")).getData();
        }
    }

    public static List<BlockMeta> getBlockMeta(byte[] VHP) {
        List<BlockMeta> ls = new ArrayList();
        Bson filter = Filters.eq("VHP", new Binary(VHP));
        Document fields = new Document("VHB", 1);
        fields.append("KED", 1);
        FindIterable<Document> it = MongoSource.getBlockCollection().find(filter).projection(fields);
        for (Document doc : it) {
            ls.add(new BlockMeta(doc));
        }
        return ls;
    }

    public static BlockMeta getBlockMeta(byte[] VHP, byte[] VHB) {
        Bson bson1 = Filters.eq("VHP", new Binary(VHP));
        Bson bson2 = Filters.eq("VHB", new Binary(VHB));
        Bson bson = Filters.and(bson1, bson2);
        Document fields = new Document("_id", 1);
        fields.append("NLINK", 1);
        Document doc = MongoSource.getBlockCollection().find(bson).projection(fields).first();
        if (doc == null) {
            return null;
        } else {
            return new BlockMeta(doc);
        }
    }

    public static void decBlockNLINK(BlockMeta meta) {
        if (meta.getNLINK() < 0xFFFFFF) {
            Bson filter = Filters.eq("_id", meta.getVBI());
            Document update = new Document("$inc", new Document("NLINK", -1));
            MongoSource.getBlockCollection().findOneAndUpdate(filter, update);
        }
    }

    public static void incBlockNLINK(BlockMeta meta) {
        if (meta.getNLINK() < 0xFFFFFF) {
            Bson filter = Filters.eq("_id", meta.getVBI());
            Document update = new Document("$inc", new Document("NLINK", 1));
            MongoSource.getBlockCollection().findOneAndUpdate(filter, update);
        }
    }

    public static BlockMeta getBlockMeta(long VBI) {
        Bson filter = Filters.eq("_id", VBI);
        Document doc = MongoSource.getBlockCollection().find(filter).first();
        if (doc == null) {
            return null;
        } else {
            return new BlockMeta(doc);
        }
    }

    public static int getBlockMetaVNF(long VBI) throws ServiceException {
        Bson filter = Filters.eq("_id", VBI);
        Document fields = new Document("VNF", 1);
        Document doc = MongoSource.getBlockCollection().find(filter).projection(fields).first();
        if (doc == null) {
            throw new ServiceException(SERVER_ERROR);
        } else {
            return doc.getInteger("VNF");
        }
    }

}
