package com.ytfs.service.codec;

import java.io.IOException;
import java.util.List;

public class TestShardCoder {

    private static final byte[] key = KeyStoreCoder.generateRandomKey();

    public static void main(String[] args) throws IOException {
       // middleBlock();
        smallBlock();
    }

    private static void middleBlock() throws IOException {
        Block block = new Block("d:\\P2PSearcher_DWJ.rar");
        block.load();
        
        BlockAESEncryptor aes = new BlockAESEncryptor(block, key);
        aes.encrypt();
        int encryptedBlockSize = aes.getBlockEncrypted().getEncryptedBlockSize();

        ShardRSEncoder encoder = new ShardRSEncoder(aes.getBlockEncrypted());
        encoder.encode();

        List<Shard> shards = encoder.getShardList();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardRSDecoder decoder = new ShardRSDecoder(shards, encryptedBlockSize);
        BlockEncrypted b = decoder.decode();

        BlockAESDecryptor aesdecoder = new BlockAESDecryptor(b.getData(), block.getRealSize(), key);
        aesdecoder.decrypt();

        block = new Block(aesdecoder.getSrcData());
        block.save("d:\\312.rar");

    }
 
    private static void smallBlock() throws IOException {
        Block block = new Block("d:\\seo.txt");
        block.load();

        BlockAESEncryptor aes = new BlockAESEncryptor(block, key);
        aes.encrypt();
        int encryptedBlockSize = aes.getBlockEncrypted().getEncryptedBlockSize();

        ShardRSEncoder encoder = new ShardRSEncoder(aes.getBlockEncrypted());
        encoder.encode();

        List<Shard> shards = encoder.getShardList();

        deleteDataShard(shards);
        //deleteParityShard(shards);

        ShardRSDecoder decoder = new ShardRSDecoder(shards, encryptedBlockSize);
        BlockEncrypted b = decoder.decode();

        BlockAESDecryptor aesdecoder = new BlockAESDecryptor(b.getData(), block.getRealSize(), key);
        aesdecoder.decrypt();

        block = new Block(aesdecoder.getSrcData());
        block.save("d:\\cc.txt");
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
