package com.ytfs.service.codec;

import java.io.IOException;

public class TestBlockCoder {

    public static void main(String[] args) throws IOException {
        YTFile fragmentor = new YTFile("e:\\win2012.iso");
        fragmentor.handle();

        BlockReader reader = new BlockReader("e:\\win2012.iso.blocks", "d:\\aa.iso");
        reader.read();
    }

}
