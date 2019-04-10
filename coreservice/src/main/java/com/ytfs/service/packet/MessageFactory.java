package com.ytfs.service.packet;

import com.github.snksoft.crc.CRC;
import com.ytfs.service.Function;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.codec.binary.Hex;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.classreading.MetadataReader;

public class MessageFactory {

    private static final Map<Short, Class> id_class_Map = new HashMap();
    private static final Map<Class, Short> class_id_Map = new HashMap();
    private static final ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(true);

    static {
        try {
            regMessageType();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
    }

    public static short getMessageID(Class cls) {
        Short id = class_id_Map.get(cls);
        if (id == null) {
            throw new RuntimeException("Invalid instruction.");
        } else {
            return id;
        }
    }

    public static Class getMessageType(short commandid)  {
        Class cls = id_class_Map.get(commandid);
        if (cls == null) {
            throw new RuntimeException("Invalid instruction.");
        } else {
            return cls;
        }
    }

    private synchronized static void regMessageType() throws IOException {
        ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resourcePatternResolver.getResources("classpath*:com/ytfs/service/packet/*.class");
        for (Resource r : resources) {
            if (r.isReadable()) {
                try {
                    checkResource(r);
                } catch (Exception e) {
                }
            }
        }
    }

    private static void checkResource(Resource resource) throws IOException, ClassNotFoundException {
        MetadataReader metadataReader = provider.getMetadataReaderFactory().getMetadataReader(resource);
        ClassMetadata metadata = metadataReader.getClassMetadata();
        if (metadata.isConcrete() && metadata.hasSuperClass()) {
            try {
                Class cls = Class.forName(metadata.getClassName());
                putClass(cls);
            } catch (Throwable e) {
            }
        }
    }

    private static void putClass(Class cls) throws IOException, ReflectiveOperationException {
        short id = (short) CRC.calculateCRC(CRC.Parameters.CRC16, cls.getName().getBytes());
        if (id_class_Map.containsKey(id)) {
            throw new IOException("'" + cls.getName() + "' initialization error, CommandID is repeated.");
        } else {
            id_class_Map.put(id, cls);
            class_id_Map.put(cls, id);
        }
    }

    public static void main(String[] args) {
        Set<Short> set = id_class_Map.keySet();
        for (Short s : set) {            
            System.out.print("MessageID:" + Hex.encodeHexString(Function.short2bytes(s)) + "---->");
            System.out.println(id_class_Map.get(s).getName());
        }
    }
}
