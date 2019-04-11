package com.ytfs.service;

import com.ytfs.service.codec.Block;
import com.ytfs.service.codec.BlockAESEncryptor;
import com.ytfs.service.codec.KeyStoreCoder;
import com.ytfs.service.codec.ShardAESEncryptor;
import com.ytfs.service.codec.ShardRSEncoder;
import com.ytfs.service.codec.YTFile;
import com.ytfs.service.net.P2PUtils;
import com.ytfs.service.node.Node;
import com.ytfs.service.node.SuperNodeList;
import com.ytfs.service.packet.ServiceException;
import com.ytfs.service.packet.UploadBlockDBReq;
import com.ytfs.service.packet.UploadBlockDupReq;
import com.ytfs.service.packet.UploadBlockInit2Req;
import com.ytfs.service.packet.UploadBlockDupResp;
import com.ytfs.service.packet.UploadBlockInitReq;
import com.ytfs.service.packet.UploadBlockInitResp;
import com.ytfs.service.packet.UploadObjectEndReq;
import com.ytfs.service.packet.UploadObjectInitReq;
import com.ytfs.service.packet.UploadObjectInitResp;
import com.ytfs.service.packet.VoidResp;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.bson.types.ObjectId;

public class UploadObject {

    private final YTFile ytfile;

    public UploadObject(byte[] data) throws IOException {
        ytfile = new YTFile(data);
    }

    public UploadObject(String path) throws IOException {
        ytfile = new YTFile(path);
    }

    public byte[] upload() throws ServiceException, IOException, InterruptedException {
        UploadObjectInitReq req = new UploadObjectInitReq(ytfile.getLength(), ytfile.getVHW());
        UploadObjectInitResp res = (UploadObjectInitResp) P2PUtils.requestBPU(req, UserConfig.superNode);
        if (!res.isRepeat()) {
            ytfile.init(res.getVNU().toHexString());
            ytfile.handle();
            List<Block> blockList = ytfile.getBlockList();
            short[] refers = res.getBlocks();
            short ii = 0;
            for (Block b : blockList) {
                try {
                    b.load();//出错需要重新分块
                } catch (IOException d) {
                    ytfile.clear();
                    throw d;
                }
                boolean uploaded = false;
                if (res.getBlocks() != null) { //检查是否已经上传
                    for (short refer : refers) {
                        if (ii == refer) {
                            uploaded = true;
                            break;
                        }
                    }
                }
                if (!uploaded) {
                    upload(b, res.getVNU(), ii);
                }
                ii++;
            }
            complete(ytfile.getVHW());
        }
        return ytfile.getVHW();
    }

    //结束上传
    private void complete(byte[] VHW) throws ServiceException {
        UploadObjectEndReq req = new UploadObjectEndReq();
        req.setVHW(VHW);
        P2PUtils.requestBPU(req, UserConfig.superNode);
    }

    //上传块
    private void upload(Block b, ObjectId vnu, short id) throws ServiceException, IOException, InterruptedException {
        b.calculate();
        Node node = SuperNodeList.getBlockSuperNode(b.getVHP());
        ShardRSEncoder rs = new ShardRSEncoder(b);
        rs.encode();
        UploadBlockInitReq req = new UploadBlockInitReq(vnu, b.getVHP(), rs.getShardCount(), id);
        Object resp = P2PUtils.requestBPU(req, node);
        if (resp instanceof VoidResp) {//已经上传
            return;
        }
        if (resp instanceof UploadBlockDupResp) {//重复,resp.getExist()=0已经上传
            UploadBlockDupReq uploadBlockDupReq = checkResp((UploadBlockDupResp) resp, b, rs);
            if (uploadBlockDupReq != null) {//请求节点
                uploadBlockDupReq.setId(id);
                uploadBlockDupReq.setVHP(b.getVHP());  //计数
                uploadBlockDupReq.setOriginalSize(b.getOriginalSize());
                uploadBlockDupReq.setRealSize(b.getRealSize());
                uploadBlockDupReq.setVNU(vnu);
                P2PUtils.requestBPU(uploadBlockDupReq, node);
            } else {
                if (!rs.needEncode()) {
                    UploadBlockToDB(b, vnu, id, node);
                } else {//请求分配节点
                    UploadBlockInit2Req req2 = new UploadBlockInit2Req(req);
                    UploadBlockInitResp resp1 = (UploadBlockInitResp) P2PUtils.requestBPU(req2, node);
                    UploadBlock ub = new UploadBlock(rs, id, resp1.getNodes(), resp1.getVBI(), node);
                    ub.upload();
                }
            }
        }
        if (resp instanceof UploadBlockInitResp) {
            if (!rs.needEncode()) {
                UploadBlockToDB(b, vnu, id, node);
            }
            UploadBlockInitResp resp1 = (UploadBlockInitResp) resp;
            UploadBlock ub = new UploadBlock(rs, id, resp1.getNodes(), resp1.getVBI(), node);
            ub.upload();
        }
    }

    //上传小文件至数据库
    private void UploadBlockToDB(Block b, ObjectId vnu, short id, Node node) throws ServiceException {
        byte[] ks = KeyStoreCoder.generateRandomKey();
        BlockAESEncryptor enc = new BlockAESEncryptor(b, ks);
        enc.encrypt();
        UploadBlockDBReq req = new UploadBlockDBReq();
        req.setId(id);
        req.setVNU(vnu);
        req.setVHP(b.getVHP());
        req.setVHB(enc.getVHB());
        req.setKEU(KeyStoreCoder.rsaEncryped(ks, UserConfig.KUEp));
        req.setKED(KeyStoreCoder.encryped(ks, b.getKD()));
        req.setOriginalSize(b.getOriginalSize());
        req.setData(enc.getData());
        P2PUtils.requestBPU(req, node);
    }

    //检查重复
    private UploadBlockDupReq checkResp(UploadBlockDupResp resp, Block b, ShardRSEncoder rs) {
        byte[][] keds = resp.getKED();
        byte[][] vhbs = resp.getVHB();
        for (int ii = 0; ii < keds.length; ii++) {
            byte[] ked = keds[ii];
            try {
                byte[] ks = KeyStoreCoder.decryped(ked, b.getKD());
                byte[] VHB;
                if (rs.needEncode()) {
                    ShardAESEncryptor enc = new ShardAESEncryptor(rs.getShardList(), ks);
                    enc.encrypt();
                    VHB = enc.makeVHB();
                } else {
                    BlockAESEncryptor enc = new BlockAESEncryptor(b, ks);
                    enc.encrypt();
                    VHB = enc.getVHB();
                }
                if (Arrays.equals(vhbs[ii], VHB)) {
                    UploadBlockDupReq req = new UploadBlockDupReq();
                    req.setVHB(VHB);
                    byte[] keu = KeyStoreCoder.encryped(ks, UserConfig.KUEp);
                    req.setKEU(keu);
                    return req;
                }
            } catch (Exception r) {//解密不了,认为作假
            }
        }
        return null;
    }

}
