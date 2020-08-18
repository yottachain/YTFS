package ytfs

import (
	"encoding/binary"
	"fmt"
	"github.com/tecbot/gorocksdb"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTDataNode/logger"
	"os"
	"path"
	"sync"
	"unsafe"
)

var mdbFileName = "/maindb"
var ytPosKey    = "yt_rocks_pos_key"
var ytBlkSzKey  = "yt_blk_size_key"

type KvDB struct {
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
	PosKey ydcommon.IndexTableKey
	PosIdx ydcommon.IndexTableValue
	BlkKey ydcommon.IndexTableKey
	BlkVal uint32
	Header *ydcommon.Header
}

func openKVDB(DBPath string) (kvdb *KvDB, err error) {
//	var posIdx uint32
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	db, err := gorocksdb.OpenDb(opts, DBPath)
	if err != nil {
		fmt.Println("[kvdb] open rocksdb error")
		return nil, err
	}

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	return &KvDB{
		Rdb   :  db,
		ro    :  ro,
		wo    :  wo,
		//PosKey:  ydcommon.IndexTableKey(diskPoskey),
		//PosIdx:  ydcommon.IndexTableValue(posIdx),
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

	//IndexDBPath := path.Join(dir,"index.db")
	//if PathExists(mainDBPath) && PathExists(IndexDBPath){
	//	fmt.Println("[KVDB][error] there are two metadata DB found!!")
	//	return nil,ErrTwoMetaFile
	//}

	Header,err := initializeHeader(config)
	if err != nil {
		fmt.Println("[rocksdb]initialize Header error")
		return nil,err
	}
    mDB.Header = Header

	//get start Pos from rocksdb
	HKey := ydcommon.BytesToHash([]byte(ytPosKey))
	mDB.PosKey = ydcommon.IndexTableKey(HKey)
	PosRocksdb, err := mDB.Get(mDB.PosKey)
	if err != nil {
		fmt.Println("[rocksdb] get start write pos err=",err)
		return nil, err
	}

	//if indexdb exist, get write start pos from index.db
	fileIdxdb := path.Join(dir,"index.db")
	if PathExists(fileIdxdb){
		indexDB, err := NewIndexDB(dir, config)
		if err != nil {
			return nil,err
		}

		//if rocksdb start pos < index.db start pos, there must be some error
		posIdxdb := indexDB.schema.DataEndPoint
		if uint64(PosRocksdb) < posIdxdb{
			log.Println("pos error:",ErrDBConfig)
			return nil,ErrDBConfig
		}
	}

	mDB.PosIdx = PosRocksdb
	fmt.Println("[rocksdb] OpenYTFSK Current start posidx=",mDB.PosIdx)

	//check blksize to rocksdb
	HKey = ydcommon.BytesToHash([]byte(ytBlkSzKey))
	mDB.BlkKey = ydcommon.IndexTableKey(HKey)
	Blksize,err := mDB.Get(mDB.BlkKey)
	if  err != nil  {
		fmt.Println("[rocksdb] get BlkSize error")
		return nil,err
	}

	valbuf := make([]byte,4)
	if uint32(Blksize) != Header.DataBlockSize {
		if uint32(Blksize) != 0{
			fmt.Println("[rocksdb] error, BlkSize mismatch")
			return nil,err
		}

		binary.LittleEndian.PutUint32(valbuf, uint32(Header.DataBlockSize))
		err := mDB.Rdb.Put(mDB.wo, mDB.BlkKey[:], valbuf)
		if err != nil {
			fmt.Println("[rocksdb]set blksize to rocksdb err:", err)
			return nil, err
		}
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

	fileName := path.Join(dir, "dbsafe")
	if ! PathExists(fileName) {
		if _, err := os.Create(fileName);err != nil {
			log.Println("create arbiration file error!")
			return nil,err
		}
	}

	fmt.Println("Open YTFS success @" + dir)
	return ytfs, nil
}


func (rd *KvDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	var retval uint32
	val, err := rd.Rdb.Get(rd.ro, key[:])
	if err != nil {
		fmt.Println("[rocksdb] get pos error:", err)
		return 0, err
	}

	if val.Exists(){
		retval = binary.LittleEndian.Uint32(val.Data())
	}
	return ydcommon.IndexTableValue(retval), nil
}

func initializeHeader( config *opt.Options) (*ydcommon.Header, error) {
	m, n := config.IndexTableCols, config.IndexTableRows
	t, d, h := config.TotalVolumn, config.DataBlockSize, uint32(unsafe.Sizeof(ydcommon.Header{}))

	ytfsSize := uint64(0)
	for _, storageOption := range config.Storages {
		ytfsSize += storageOption.StorageVolume
	}

	// write header.
	header := ydcommon.Header{
		Tag:            [4]byte{'Y', 'T', 'F', 'S'},
		Version:        [4]byte{'0', '.', '0', '3'},
		YtfsCapability: t,
		YtfsSize:       ytfsSize,
		DataBlockSize:  d,
		RangeCapacity:  n,
		RangeCoverage:  m,
		HashOffset:     h,
		DataEndPoint:   0,
		RecycleOffset:  uint64(h) + (uint64(n)+1)*(uint64(m)*36+4),
		Reserved:       0xCDCDCDCDCDCDCDCD,
	}
	return &header, nil
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
			fmt.Println("[rocksdb]put dnhash to rocksdb error:", err)
			return nil, err
		}
        i++
	}

    fmt.Println("[rockspos] BatchPut PosIdx=",rd.PosIdx,"i=",i)
	rd.PosIdx = ydcommon.IndexTableValue(uint32(rd.PosIdx) + uint32(i))
	binary.LittleEndian.PutUint32(valbuf, uint32(rd.PosIdx))
    err := rd.Rdb.Put(rd.wo,rd.PosKey[:],valbuf)
	if err != nil {
		fmt.Println("update write pos to db error:", err)
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

func (rd *KvDB) Len() uint64 {
	return uint64(rd.PosIdx)
}

func (rd *KvDB) TotalSize() uint64{
	return rd.Header.YtfsSize
}

func (rd *KvDB) BlockSize() uint32{
	return rd.BlkVal
}

func (rd *KvDB) Meta() *ydcommon.Header{
	return rd.Header
}

func (rd *KvDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	return nil
}

func (rd *KvDB) Close() {
}

func (rd *KvDB) Reset() {
}
