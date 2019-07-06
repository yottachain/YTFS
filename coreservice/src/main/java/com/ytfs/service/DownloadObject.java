package com.ytfs.service;

import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.packet.DownloadObjectInitReq;
import com.ytfs.service.packet.DownloadObjectInitResp;
import com.ytfs.service.packet.ServiceException;
import java.io.InputStream;
import java.util.List;

public class DownloadObject {

    private final byte[] VHW;
    private List<ObjectRefer> refers;

    public DownloadObject(byte[] VHW) throws ServiceException {
        this.VHW = VHW;
        init();
    }

    private void init() throws ServiceException {
        DownloadObjectInitReq req = new DownloadObjectInitReq();
        req.setVHW(VHW);
        DownloadObjectInitResp resp = (DownloadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        refers = ObjectRefer.parse(resp.getRefers());
    }

    public InputStream load(long start, long end) {
        return new DownloadInputStream(refers, start, end);
    }
}
