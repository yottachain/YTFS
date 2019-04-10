package com.ytfs.service.dao;

import com.ytfs.service.LogConfigurator;
import org.bson.types.ObjectId;

public class Test {

    public static void main(String[] a) throws Exception {
        LogConfigurator.configPath();
        testSeq();
        
        //testUser();
        //testObject();
    }

    private static void testSeq() throws Exception {
        //int uid = Sequence.getSequence(1);
        //System.out.println(Integer.toHexString(uid));

        long l=Sequence.generateBlockID(10);
        System.out.println(Long.toHexString(l));
        System.out.println(Long.toHexString(l+1));
        System.out.println(Long.toHexString(l+2));
        System.out.println(Long.toHexString(l+3));
        System.out.println(Long.toHexString(l+4));
        System.out.println(Long.toHexString(l+5));
        System.out.println(Long.toHexString(l+6));
        System.out.println(Long.toHexString(l+7));
        System.out.println(Long.toHexString(l+8));
        System.out.println(Long.toHexString(l+9));
        System.out.println("********************************"); 
        l=Sequence.generateBlockID(10);
        System.out.println(Long.toHexString(l));
        System.out.println(Long.toHexString(l+1));
        System.out.println(Long.toHexString(l+2));
        System.out.println(Long.toHexString(l+3));
        System.out.println(Long.toHexString(l+4));
        System.out.println(Long.toHexString(l+5));
        System.out.println(Long.toHexString(l+6));
        System.out.println(Long.toHexString(l+7));
        System.out.println(Long.toHexString(l+8));
        System.out.println(Long.toHexString(l+9));
    }

    private static void testUser() throws Exception {
        User usr = new User(Sequence.generateUserID());
        usr.setKUEp("aaswfe".getBytes());
        usr.setKUSp("dfhgdfhdh".getBytes());
        usr.setSecretKey("sbbb".getBytes());
        usr.setTotalBaseCost(2);
        usr.setUsedSpace(465);

        UserAccessor.addUser(usr);

        usr = UserAccessor.getUser(usr.getUserID());
        System.out.println(usr.getUserID());
        System.out.println(new String(usr.getKUEp()));
        System.out.println(new String(usr.getKUSp()));
        System.out.println(new String(usr.getSecretKey()));
        System.out.println(usr.getTotalBaseCost());
        System.out.println(usr.getUsedSpace());
    }

    private static void testObject() throws Exception {
        ObjectMeta meta = new ObjectMeta(1,"sssssssssssgewggwgwg".getBytes());
        meta.setBlocks("fsdgdth".getBytes());
        meta.setNLINK(1);
        meta.setVNU(new ObjectId());
        //ObjectDAO.addObject(meta);
        
        ObjectAccessor.isObjectExists(meta);
        System.out.println("nlink:"+meta.getNLINK());
        System.out.println("VNU:"+meta.getVNU());
        
        //ObjectDAO.incObjectNLINK(meta);
        
        
        meta = ObjectAccessor.getObject(meta.getUserID(), meta.getVHW());
        System.out.println(meta.getUserID());
        System.out.println(new String(meta.getVHW()));
        System.out.println(new String(meta.getBlocks()));
        System.out.println(meta.getNLINK());
    }
}
