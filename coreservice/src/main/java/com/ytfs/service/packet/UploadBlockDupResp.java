package com.ytfs.service.packet;

import com.ytfs.service.dao.BlockMeta;
import java.util.List;

public class UploadBlockDupResp {

    private byte[][] VHB;//每个加密后的数据分片的SHA256摘要，连接在一起后再计算出的MD5摘要
    private byte[][] KED;//去重密钥

    public void setKEDANDVHB(List<BlockMeta> ls) {
        VHB = new byte[ls.size()][];
        KED = new byte[ls.size()][];
        int index = 0;
        for (BlockMeta m : ls) {
            VHB[index] = m.getVHB();
            KED[index] = m.getKED();
            index++;
        }
    }

    /**
     * @return the VHB
     */
    public byte[][] getVHB() {
        return VHB;
    }

    /**
     * @param VHB the VHB to set
     */
    public void setVHB(byte[][] VHB) {
        this.VHB = VHB;
    }

    /**
     * @return the KED
     */
    public byte[][] getKED() {
        return KED;
    }

    /**
     * @param KED the KED to set
     */
    public void setKED(byte[][] KED) {
        this.KED = KED;
    }

}
