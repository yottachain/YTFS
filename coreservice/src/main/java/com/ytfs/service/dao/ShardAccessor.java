package com.ytfs.service.dao;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

public class ShardAccessor {

    public static void saveShardMeta(ShardMeta meta) {
        MongoSource.getShardCollection().insertOne(meta.toDocument());
    }

    public static void saveShardMetas(List<Document> metas) {
        MongoSource.getShardCollection().insertMany(metas);
    }

    public static ShardMeta[] getShardMeta(long VBI, int shardCount) {
        Long[] VFI = new Long[shardCount];
        for (int ii = 0; ii < shardCount; ii++) {
            VFI[ii] = VBI + ii;
        }
        ShardMeta[] metas = new ShardMeta[shardCount];
        Bson bson = Filters.in("_id", VFI);
        FindIterable<Document> documents = MongoSource.getShardCollection().find(bson).batchSize(shardCount);
        int count = 0;
        for (Document document : documents) {
            metas[count++] = new ShardMeta(document);
        }
        return metas;
    }
}
