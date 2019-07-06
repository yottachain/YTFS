package com.ytfs.service.packet;

import com.ytfs.service.codec.ObjectRefer;
import java.util.List;
import org.bson.types.ObjectId;

public class UploadObjectInitResp {

    private boolean repeat = false;
    private ObjectId VNU = null;
    private short[] blocks = null;

    public UploadObjectInitResp() {

    }

    public UploadObjectInitResp(boolean repeat) {
        this.repeat = repeat;
    }

    public UploadObjectInitResp(boolean repeat, ObjectId VNU) {
        this.repeat = repeat;
        this.VNU = VNU;
    }

    /**
     * @return the repeat
     */
    public boolean isRepeat() {
        return repeat;
    }

    /**
     * @param repeat the repeat to set
     */
    public void setRepeat(boolean repeat) {
        this.repeat = repeat;
    }

    /**
     * @return the VNU
     */
    public ObjectId getVNU() {
        return VNU;
    }

    /**
     * @param VNU the VNU to set
     */
    public void setVNU(ObjectId VNU) {
        this.VNU = VNU;
    }

    /**
     * @return the blocks
     */
    public short[] getBlocks() {
        return blocks;
    }

    /**
     * @param blocks the blocks to set
     */
    public void setBlocks(short[] blocks) {
        this.blocks = blocks;
    }

    /**
     * @param blocks the blocks to set
     */
    public void setBlocks(byte[] blocks) {
        List<ObjectRefer> refers = ObjectRefer.parse(blocks);
        this.blocks = new short[refers.size()];
        for (int ii = 0; ii < this.blocks.length; ii++) {
            this.blocks[ii] = refers.get(ii).getId();
        }
    }
}
