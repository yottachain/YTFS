package com.ytfs.service;

import com.ytfs.service.codec.BlockAESDecryptor;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.codec.Shard;
import com.ytfs.service.codec.ShardAESDecryptor;
import com.ytfs.service.codec.ShardAESEncryptor;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.DownloadBlockDBResp;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import static com.ytfs.service.packet.ServiceErrorCode.INVALID_SHARD;
import com.ytfs.service.packet.ServiceException;
import java.util.ArrayList;
import java.util.List;

public class DownloadBlock {

    private final ObjectRefer refer;
    private byte[] data;
    private final List<DownloadShardResp> resList = new ArrayList();
    private byte[] ks;

    DownloadBlock(ObjectRefer refer) throws ServiceException {
        this.refer = refer;
    }

    public byte[] getData() {
        return data;
    }

    public void load() throws ServiceException {
        ks = KeyStoreCoder.decryped(refer.getKEU(), UserConfig.KUSp);
        DownloadBlockInitReq req = new DownloadBlockInitReq();
        req.setVBI(refer.getVBI());
        Node pbd = SuperNodeList.getBlockSuperNode(refer.getSuperID());
        Object resp = P2PUtils.requestBPU(req, pbd);
        if (resp instanceof DownloadBlockDBResp) {
            this.data = aesDBDecode(((DownloadBlockDBResp) resp).getData());
        } else {
            DownloadBlockInitResp initresp = (DownloadBlockInitResp) resp;
            if (initresp.getVNF() < 0) {
                this.data = loadCopyShard(initresp);
            } else {
                this.data = loadRSShard(initresp);
            }
        }
    }

    void onResponse(DownloadShardResp res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    private byte[] loadRSShard(DownloadBlockInitResp initresp) {
        return null;
    }

    private void firstDownload(ShardAESEncryptor enc) throws InterruptedException {
        /*
        List<Shard> shards = enc.getEnc_shards();
        int nodeindex = 0;
        for (Shard sd : shards) {
            map.put(nodeindex, sd);
            ShardNode n = nodes[nodeindex];
            UploadShardReq req = new UploadShardReq();
            req.setBPDID(bpdNode.getNodeId());
            req.setBPDSIGN(n.getSign());
            req.setDAT(sd.getData());
            req.setSHARDID(nodeindex);
            req.setVBI(VBI);
            req.setVHF(sd.getVHF());
            req.sign(nodes[nodeindex].getNodeId());
            UploadShard.startUploadShard(req, n, this);
            nodeindex++;
        } 
        synchronized (this) {
            if (resList.size() != shards.size()) {
                this.wait(1000 * 15);
            }
        }*/
    }

    private byte[] loadCopyShard(DownloadBlockInitResp initresp) throws ServiceException {
        DownloadShardReq req = new DownloadShardReq();
        int len = initresp.getVNF() * -1;
        int index = 0;
        ServiceException t = null;
        while (index < len) {
            try {
                Node n = initresp.getNodes()[index];
                byte[] VHF = initresp.getVHF()[index];
                req.setVHF(VHF);
                DownloadShardResp resp = (DownloadShardResp) P2PUtils.requestNode(req, n);
                if (resp.verify(VHF)) {
                    return aesCopyDecode(resp.getData());
                }
                index++;
            } catch (ServiceException e) {
                t = e;
            }
        }
        throw t == null ? new ServiceException(INVALID_SHARD) : t;
    }

    private byte[] aesCopyDecode(byte[] data) {
        ShardAESDecryptor dec = new ShardAESDecryptor(new Shard(data), ks);
        dec.decrypt();
        Shard s = dec.getDec_shard();
        byte[] newdata = new byte[refer.getRealSize()];
        System.arraycopy(s.getData(), 1, data, 0, newdata.length);
        return newdata;
    }

    private byte[] aesDBDecode(byte[] data) {
        BlockAESDecryptor dec = new BlockAESDecryptor(data, ks);
        dec.decrypt();
        return dec.getBlock().getData();
    }
}
