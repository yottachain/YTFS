package com.ytfs.service;

import com.ytfs.service.codec.BlockAESDecryptor;
import com.ytfs.service.codec.BlockEncrypted;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.codec.ObjectRefer;
import com.ytfs.service.codec.Shard;
import com.ytfs.service.codec.ShardRSDecoder;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.DownloadBlockDBResp;
import com.ytfs.service.packet.DownloadBlockInitReq;
import com.ytfs.service.packet.DownloadBlockInitResp;
import com.ytfs.service.packet.DownloadShardReq;
import com.ytfs.service.packet.DownloadShardResp;
import static com.ytfs.service.packet.ServiceErrorCode.INTERNAL_ERROR;
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
                try {
                    this.data = loadRSShard(initresp);
                } catch (InterruptedException e) {
                    throw new ServiceException(INTERNAL_ERROR, e.getMessage());
                }
            }
        }
    }

    void onResponse(DownloadShardResp res) {
        synchronized (this) {
            resList.add(res);
            this.notify();
        }
    }

    private byte[] loadRSShard(DownloadBlockInitResp initresp) throws InterruptedException, ServiceException {
        List<Shard> shards = new ArrayList();
        int len = initresp.getVNF() - UserConfig.Default_PND;
        int nodeindex = 0;
        while (true) {
            int count = len - shards.size();
            if (count <= 0) {
                break;
            }
            if (count > initresp.getNodes().length - nodeindex) {
                break;
            }
            for (int ii = 0; ii < count; ii++) {
                Node n = initresp.getNodes()[nodeindex];
                byte[] VHF = initresp.getVHF()[nodeindex];
                DownloadShardReq req = new DownloadShardReq();
                req.setVHF(VHF);
                DownloadShare.startDownloadShard(VHF, n, this);
                nodeindex++;
            }
            synchronized (this) {
                if (resList.size() != count) {
                    this.wait(1000 * 15);
                }
            }
            for (DownloadShardResp res : resList) {
                if (res.getData() != null) {
                    shards.add(new Shard(res.getData()));
                }
            }
            resList.clear();
        }
        if (shards.size() >= len) {
            BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
            ShardRSDecoder rsdec = new ShardRSDecoder(shards, be.getEncryptedBlockSize());
            be = rsdec.decode();
            BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), refer.getRealSize(), ks);
            dec.decrypt();
            return dec.getSrcData();
        } else {
            throw new ServiceException(INTERNAL_ERROR);
        }
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
        BlockEncrypted be = new BlockEncrypted(refer.getRealSize());
        ShardRSDecoder rsdec = new ShardRSDecoder(new Shard(data), be.getEncryptedBlockSize());
        be = rsdec.decode();
        BlockAESDecryptor dec = new BlockAESDecryptor(be.getData(), refer.getRealSize(), ks);
        dec.decrypt();
        return dec.getSrcData();
    }

    private byte[] aesDBDecode(byte[] data) {
        BlockAESDecryptor dec = new BlockAESDecryptor(data, refer.getRealSize(), ks);
        dec.decrypt();
        return dec.getSrcData();
    }

}
