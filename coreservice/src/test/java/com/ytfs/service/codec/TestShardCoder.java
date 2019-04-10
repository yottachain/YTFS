package com.ytfs.service.codec;

import java.io.IOException;
import java.util.List;

public class TestShardCoder {

    private static final byte[] key = KeyStoreCoder.generateRandomKey();

    public static void main(String[] args) throws IOException {
        //bigBlock();
        //middleBlock();
        smallBlock();
    }

    private static void middleBlock() throws IOException {
        Block block = new Block("d:\\311");
        block.load();
        ShardRSEncoder encoder = new ShardRSEncoder(block);
        encoder.encode();
        List<Shard> shards = encoder.getShardList();

        ShardAESEncryptor aesencoder = new ShardAESEncryptor(shards, key);
        aesencoder.encrypt();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardAESDecryptor aesdecoder = new ShardAESDecryptor(shards, key);
        aesdecoder.decrypt();
        ShardRSDecoder decoder = new ShardRSDecoder(shards, block.getRealSize());
        Block b = decoder.decode();
        b.save("d:\\312");

    }

    private static void bigBlock() throws IOException {
        Block block = new Block("d:\\aa.docx");
        block.load();
        ShardRSEncoder encoder = new ShardRSEncoder(block);
        encoder.encode();
        List<Shard> shards = encoder.getShardList();

        ShardAESEncryptor aesencoder = new ShardAESEncryptor(shards, key);
        aesencoder.encrypt();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardAESDecryptor aesdecoder = new ShardAESDecryptor(shards, key);
        aesdecoder.decrypt();

        ShardRSDecoder decoder = new ShardRSDecoder(shards, block.getRealSize());
        Block b = decoder.decode();
        b.save("d:\\cc.docx");

    }

    private static void smallBlock() throws IOException {
        Block block = new Block("d:\\seo.txt");
        block.load();

        ShardRSEncoder encoder = new ShardRSEncoder(block);
        encoder.encode();
        List<Shard> shards = encoder.getShardList();

        ShardAESEncryptor aesencoder = new ShardAESEncryptor(shards, key);
        aesencoder.encrypt();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardAESDecryptor aesdecoder = new ShardAESDecryptor(shards, key);
        aesdecoder.decrypt();

        ShardRSDecoder decoder = new ShardRSDecoder(shards, block.getRealSize());
        Block b = decoder.decode();
        b.save("d:\\cc.txt");
    }

    private static void deleteDataShard(List<Shard> shards) {
        shards.remove(2);
        shards.remove(5);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        /*
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);

        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
        shards.remove(7);
         */
        //shards.remove(7);
    }

    private static void deleteParityShard(List<Shard> shards) {
        shards.remove(shards.size() - 2);
        shards.remove(shards.size() - 2);
        shards.remove(shards.size() - 2);
    }
}
