package ytfs

import (
	"encoding/binary"
	"fmt"
	"github.com/tecbot/gorocksdb"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"os"
	"path"
	"sync"
)

var mdbFileName = "/maindb"
var ytPosKey = "ytfs_rocks_pos_key"

type KvDB struct {
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
	PosKey ydcommon.IndexTableKey
	PosIdx ydcommon.IndexTableValue
}

func openKVDB(DBPath string) (kvdb *KvDB, err error) {
	// 使用 gorocksdb 连接 RocksDB
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	// 设置输入目标数据库文件（可自行配置，./db 为当前测试文件的目录下的 db 文件夹）
	db, err := gorocksdb.OpenDb(opts, DBPath)
	if err != nil {
		fmt.Println("[kvdb] open rocksdb error")
		return nil, err
	}

	// 创建输入输出流
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	diskPoskey := ydcommon.BytesToHash([]byte(ytPosKey))
    val,err := db.Get(ro,diskPoskey[:])
	posIdx := binary.LittleEndian.Uint32(val.Data())
	return &KvDB{
		Rdb   :  db,
		ro    :  ro,
		wo    :  wo,
		PosKey:  ydcommon.IndexTableKey(diskPoskey),
		PosIdx:  ydcommon.IndexTableValue(posIdx),
	}, err
}

func openYTFSK(dir string, config *opt.Options) (*YTFS, error) {
	//TODO: file lock to avoid re-open.
	//1. open system dir for YTFS
	if fi, err := os.Stat(dir); err == nil {
		// dir/file exists, check if it can be reloaded.
		if !fi.IsDir() {
			return nil, ErrDirNameConflict
		}
		err := openYTFSDir(dir, config)
		if err != nil && err != ErrEmptyYTFSDir {
			return nil, err
		}
	} else {
		// create new dir
		if err := os.MkdirAll(dir, os.ModeDir|os.ModePerm); err != nil {
			return nil, err
		}
	}

	// initial a new ytfs.
	// save config
	configName := path.Join(dir, "config.json")
	err := opt.SaveConfig(config, configName)
	if err != nil {
		return nil, err
	}

	//open main kv-db
	mainDBPath := path.Join(dir, mdbFileName)
	mDB, err := openKVDB(mainDBPath)
	if err != nil {
		fmt.Println("[KVDB]open main kv-DB for save hash error:", err)
		return nil, err
	}
	//3. open storages
	context, err := NewContext(dir, config, uint64(mDB.PosIdx))
	if err != nil {
		return nil, err
	}

	ytfs := &YTFS{
		config : config,
		db     : mDB,
		context: context,
		mutex  : new(sync.Mutex),
	}

	fmt.Println("Open YTFS success @" + dir)
	return ytfs, nil
}


func (rd *KvDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	val, err := rd.Rdb.Get(rd.ro, key[:])
	pos := binary.LittleEndian.Uint32(val.Data())
	//	fmt.Println("[rocksdb] Rocksdbval=",val,"Rocksdbval32=",pos)
	if err != nil {
		fmt.Println("[rocksdb] rocksdb get pos error:", err)
		return 0, err
	}
	return ydcommon.IndexTableValue(pos), nil
}

func (rd *KvDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	return nil
}

//func (rd *KvDB) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
//	return nil, nil
//}

func (rd *KvDB) Close() {
}

func (rd *KvDB) Reset() {
}

func (rd *KvDB) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
	//	keyValue:=make(map[ydcommon.IndexTableKey]ydcommon.IndexTableValue,len(batch))
	i := 0
	valbuf := make([]byte, 4)
	for _,value := range kvPairs{
		HKey := value.Hash[:]
		HPos := value.OffsetIdx
		binary.LittleEndian.PutUint32(valbuf, uint32(HPos))
		err := rd.Rdb.Put(rd.wo, HKey, valbuf)

		if err != nil {
			fmt.Println("[rocksdb]put dnhash to rocksdb error", err)
			return nil, err
		}
        i++
	}
	//currentPos,err := rd.Get(rd.Poskey)
	//if err != nil {
	//	fmt.Println("get current write pos err:", err)
	//	return nil, err
	//}
	rd.PosIdx = ydcommon.IndexTableValue(uint32(rd.PosIdx) + uint32(i))
	binary.LittleEndian.PutUint32(valbuf, uint32(rd.PosIdx))
    err := rd.Rdb.Put(rd.wo,rd.PosKey[:],valbuf)
	if err != nil {
		fmt.Println("update write pos to metadatafile err:", err)
		return nil, err
	}
	//fmt.Printf("[noconflict] write success batch_write_time: %d ms, batch_len %d", time.Now().Sub(begin).Milliseconds(), bufCnt)
	return nil, nil
}

func (rd *KvDB) BatchWriteKV(batch map[ydcommon.IndexTableKey][]byte) error {
	var err error
	Wbatch := new(gorocksdb.WriteBatch)
	for key, val := range batch {
		Wbatch.Put(key[:], val)

	}
	err = rd.Rdb.Write(rd.wo, Wbatch)
	return err
}

func (rd *KvDB) resetKV(batchIndexes []ydcommon.IndexItem, resetCnt uint32) {
	for j := uint32(0); j < resetCnt; j++ {
		hashKey := batchIndexes[j].Hash[:]
		rd.Rdb.Delete(rd.wo, hashKey[:])
	}
}
