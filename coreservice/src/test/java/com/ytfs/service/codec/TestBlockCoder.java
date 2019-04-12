package com.ytfs.service.codec;

import java.io.IOException;

public class TestBlockCoder {

    public static void main(String[] args) throws IOException {
        YTFile fragmentor = new YTFile("e:\\win2012.iso");
        fragmentor.init("2123142342");
        fragmentor.handle();

        BlockReader reader = new BlockReader("e:\\2123142342", "d:\\aa.iso");
        reader.read();
    }

}
