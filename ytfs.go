package ytfs

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"path"
	"sync"
	"time"

	"github.com/mr-tron/base58/base58"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

type ytfsStatus struct {
	ctxSP *storagePointer
	//TODO: index status
}

var gcspacecntkey = "gcspacecnt_rocksdb"

//type KvDB struct {
//	Rdb *gorocksdb.DB
//	ro  *gorocksdb.ReadOptions
//	wo  *gorocksdb.WriteOptions
//}

// YTFS is a data block save/load lib based on key-value styled db APIs.
type YTFS struct {
	// config of this YTFS
	config *opt.Options
	// key-value db which saves hash <-> position
	db DB
	// running context
	context *Context
	// lock of YTFS
	mutex *sync.Mutex
	// saved status
	savedStatus []ytfsStatus
}

var GcLock sync.Mutex

const GcWrtOverNum = 3

// Open opens or creates a YTFS for the given storage.
// The YTFS will be created if not exist.
//
// The returned YTFS instance is safe for concurrent use.
// The YTFS must be closed after use, by calling Close method.
// Usage Sample, ref to playground.go:
//
//	...
//	config := opt.DefaultOptions()
//
//	ytfs, err := ytfs.Open(path, config)
//	if err != nil {
//		panic(err)
//	}
//	defer ytfs.Close()
//	err = ytfs.Put(ydcommon.IndexTableKey, ydcommon.IndexTableValue)
//	if err != nil {
//		panic(err)
//	}
//
//	ydcommon.IndexTableValue, err = ytfs.Gut(ydcommon.IndexTableKey)
//	if err != nil {
//		panic(err)
//	}
//	...
func OpenInit(dir string, config *opt.Options, dnId uint32) (ytfs *YTFS, err error) {
	settings, err := opt.FinalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return openYTFS(dir, settings, true, dnId)
}

func OpenGet(dir string, config *opt.Options, dnId uint32) (ytfs *YTFS, err error) {
	settings, err := opt.FinalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return openYTFS(dir, settings, false, dnId)
}

func Open(dir string, config *opt.Options, dnid uint32) (ytfs *YTFS, err error) {
	settings, err := opt.FinalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return startYTFS(dir, settings, dnid)
}

// NewYTFS create a YTFS by config
func NewYTFS(dir string, config *opt.Options, init bool, dnId uint32) (*YTFS, error) {
	ytfs := new(YTFS)
	indexDB, err := NewIndexDB(dir, config, init)
	if err != nil {
		return nil, err
	}
	context, err := NewContext(dir, config, indexDB.schema.DataEndPoint, init, dnId)
	if err != nil {
		return nil, err
	}
	ytfs.db = indexDB
	ytfs.context = context
	ytfs.mutex = new(sync.Mutex)
	return ytfs, nil
}

const DbVersion = "0.04"
const OldDbVersion = "0.03"
const StoreVersion002 = "0.02"
const StoreVersion001 = "0.01"

var StoreVersion003 = [4]byte{0x0, '.', 0x0, 0x3}

// used for start ytfs-node
func startYTFSI(dir string, config *opt.Options, dnid uint32, init bool) (*YTFS, error) {
	//TODO: file lock to avoid re-open.

	fileName := path.Join(dir, "dbsafe")
	if PathExists(fileName) {
		fmt.Printf("db config error!")
		return nil, ErrDBConfig
	}

	idxFile := path.Join(dir, "index.db")
	if !PathExists(idxFile) {
		if !init {
			fmt.Println("indexdb Miss")
			return nil, ErrDBMiss
		}
	}

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

	// open index db
	indexDB, err := NewIndexDB(dir, config, init)
	if err != nil {
		return nil, err
	}

	ret, err := indexDB.CheckDbDnId(dnid)
	if !ret {
		return nil, err
	}

	//if 0 == indexDB.schema.DataEndPoint {
	//	if config.IndexTableCols < 512 || config.IndexTableCols > 2048 {
	//		err = fmt.Errorf("yotta config: config.M setting is incorrect")
	//		fmt.Println("[error]:", err, "M=", config.IndexTableCols, "N=", config.IndexTableRows)
	//		return nil, err
	//	}
	//}

	//3. open storages
	context, err := NewContext(dir, config, indexDB.schema.DataEndPoint, init, dnid)
	if err != nil {
		return nil, err
	}

	ret, err = context.CheckStorageDnid(dnid)
	if err != nil {
		return nil, err
	}

	ytfs := &YTFS{
		config:  config,
		db:      indexDB,
		context: context,
		mutex:   new(sync.Mutex),
	}

	if !init && ytfs.PosIdx() < 5 {
		err = fmt.Errorf("ytfs not init")
		fmt.Println("[ytfs] error:", err.Error())
		return nil, err
	}

	fmt.Println("Open YTFS success @" + dir)
	return ytfs, nil
}

// used for init ytfs-node
func openYTFSI(dir string, config *opt.Options, init bool, dnId uint32) (*YTFS, error) {
	//TODO: file lock to avoid re-open.

	fileName := path.Join(dir, "dbsafe")
	if PathExists(fileName) {
		fmt.Printf("db config error!")
		return nil, ErrDBConfig
	}

	idxFile := path.Join(dir, "index.db")
	if !PathExists(idxFile) {
		if !init {
			fmt.Println("indexdb Miss")
			return nil, ErrDBMiss
		}
	}

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

	// open index db
	indexDB, err := NewIndexDB(dir, config, init)
	if err != nil {
		return nil, err
	}

	//if 0 == indexDB.schema.DataEndPoint {
	//	if config.IndexTableCols < 512 || config.IndexTableCols > 2048 {
	//		err = fmt.Errorf("yotta config: config.M setting is incorrect")
	//		fmt.Println("[error]:", err, "M=", config.IndexTableCols, "N=", config.IndexTableRows)
	//		return nil, err
	//	}
	//}

	//3. open storages
	context, err := NewContext(dir, config, indexDB.schema.DataEndPoint, init, dnId)
	if err != nil {
		return nil, err
	}

	ytfs := &YTFS{
		config:  config,
		db:      indexDB,
		context: context,
		mutex:   new(sync.Mutex),
	}

	if !init && ytfs.PosIdx() < 5 {
		err = fmt.Errorf("ytfs not init")
		fmt.Println("[ytfs] error:", err.Error())
		return nil, err
	}

	fmt.Println("Open YTFS success @" + dir)
	return ytfs, nil
}

func (ytfs *YTFS) DiskAndUseCap() (uint32, uint32) {
	var totalRealCap uint32
	var totalConfCap uint32
	NowPos := ytfs.context.sp.index
	storArray := ytfs.context.storages

	for _, stordev := range storArray {
		totalRealCap += stordev.RealDiskCap
		totalConfCap += stordev.Cap
	}
	fmt.Println("[diskcap] totalRealCap=", totalRealCap, "totalConfCap=", totalConfCap)
	if totalRealCap > totalConfCap {
		fmt.Println("[diskcap] totalConfCap=", totalConfCap, "NowPos=", NowPos)
		return totalConfCap, NowPos
	}
	fmt.Println("[diskcap] totalRealCap=", totalRealCap, "NowPos=", NowPos)
	return totalRealCap, NowPos
}

func (ytfs *YTFS) GetTotalCap() (uint32, uint32, uint) {
	var totalRealCap uint32
	var totalConfCap uint32
	var diskNums uint
	storArray := ytfs.context.storages

	for _, stordev := range storArray {
		diskNums++
		totalRealCap += stordev.RealDiskCap
		totalConfCap += stordev.Cap
	}

	return totalConfCap, totalRealCap, diskNums
}

func openYTFSDir(dir string, config *opt.Options) error {
	configPath := path.Join(dir, "config.json")
	if _, err := os.Stat(configPath); err == nil {
		// TODO: recover data and check config consistency with input.
		oldConfig, err := opt.ParseConfig(configPath)
		if err != nil {
			return err
		}

		if !oldConfig.Equal(config) {
			return ErrSettingMismatch
		}

		return nil
	}

	return ErrEmptyYTFSDir
}

func validateYTFSSchema(meta *ydcommon.Header, opt *opt.Options) (*ydcommon.Header, *opt.Options, error) {
	if meta.YtfsCapability != opt.TotalVolumn || meta.DataBlockSize != opt.DataBlockSize {
		return nil, nil, ErrConfigIndexMismatch
	}
	return meta, opt, nil
}

// Get gets the value for the given key. It returns ErrNotFound if the
// DB does not contains the key.
//
// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after Get returns.

func (ytfs *YTFS) Get(key ydcommon.IndexTableKey) ([]byte, error) {
	pos, _, err := ytfs.db.Get(key)
	if err != nil {
		fmt.Println("[db] db get pos error:", err)
		return nil, err
	}

	data, err := ytfs.context.Get(pos)
	if err != nil {
		fmt.Println("[verify] get data error:", err, " key:", base58.Encode(key.Hsh[:]), " pos:", pos)
		return nil, err
	}

	return data, nil
	//return ytfs.context.Get(pos)
}

func (ytfs *YTFS) GetData(pos uint32) ([]byte, error) {
	data, err := ytfs.context.Get(ydcommon.IndexTableValue(pos))
	if err != nil {
		fmt.Println("[verify] get data error:", err, " pos:", pos)
	}

	return data, err
}

func (ytfs *YTFS) GetPosIdx(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	pos, _, err := ytfs.db.Get(key)
	if err != nil {
		fmt.Println("[db] db get pos error:", err)
		return 0, err
	}

	return pos, nil
}

// Put sets the value for the given key. It panic if there exists any previous value
// for that key; YottaDisk is not a multi-map.
// It is safe to modify the contents of the arguments after Put returns but not
// before.

func (ytfs *YTFS) Put(key ydcommon.IndexTableKey, buf []byte) error {
	ytfs.mutex.Lock()
	defer ytfs.mutex.Unlock()
	//_, err := ytfs.db.Get(key)
	//if err == nil {
	//	return ErrDataConflict
	//}

	pos, err := ytfs.context.Put(buf)
	if err != nil {
		return err
	}

	return ytfs.db.Put(key, ydcommon.IndexTableValue(pos))
}

func (ytfs *YTFS) PutDataAt(buf []byte, globalID uint32) error {
	ytfs.mutex.Lock()
	defer ytfs.mutex.Unlock()

	_, err := ytfs.context.PutAt(buf, globalID)
	if err != nil {
		return err
	}

	return nil
}

func (ytfs *YTFS) SetStoragePointer(globalID uint32) error {
	return ytfs.context.SetStoragePointer(globalID)
}

/*
 * Batch mode func list
 */
func (ytfs *YTFS) restoreYTFS() {
	//TODO: save index
	fmt.Println("[rocksdb] in restoreYTFS()")
	id := len(ytfs.savedStatus) - 1
	ydcommon.YottaAssert(id >= 0)
	ytfs.context.restore(ytfs.savedStatus[id].ctxSP)
	ytfs.savedStatus = ytfs.savedStatus[:id]
}

func (ytfs *YTFS) restoreIndex(conflict map[ydcommon.IndexTableKey]byte, batchindex []ydcommon.IndexItem, btCnt uint32) error {
	var err error
	tbItemMap := make(map[uint32]uint32, btCnt)
	for _, kvPairs := range batchindex {
		hashkey := kvPairs.Hash
		if _, ok := conflict[hashkey]; ok {
			fmt.Println("[restoreIndex] hashkey conflict:", base58.Encode(hashkey.Hsh[:]))
			continue
		}
		idx := ytfs.db.(*IndexDB).indexFile.GetTableEntryIndex(hashkey)
		err = ytfs.db.(*IndexDB).indexFile.ClearItemFromTable(idx, hashkey, btCnt, tbItemMap)
		if err != nil {
			fmt.Printf("[restoreIndex] reset tableidx %v hashkey %v \n", idx, hashkey)
			return err
		}
	}
	err = ytfs.db.(*IndexDB).indexFile.ResetTableSize(tbItemMap)
	if err != nil {
		fmt.Println("[restoreIndex] ResetTableSize error")
	}
	return err
}

func (ytfs *YTFS) saveCurrentYTFS() {
	//TODO: restore index
	ytfs.savedStatus = append(ytfs.savedStatus, ytfsStatus{
		ctxSP: ytfs.context.save(),
	})
}

//func (ytfs *YTFS) checkConflicts(conflicts map[ydcommon.IndexTableKey]byte, batch map[ydcommon.IndexTableKey][]byte) {
//	dir := util.GetYTFSPath()
//	fileName := path.Join(dir, "hashconflict.new")
//	hashConflict, _ := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
//	defer hashConflict.Close()
//
//	fileName2 := path.Join(dir, "hashconflict.old")
//	hashConflict2, _ := os.OpenFile(fileName2, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
//	defer hashConflict2.Close()
//
//	for ha, cflct := range conflicts {
//		if cflct == 1 {
//			hashConflict.WriteString("hash:")
//			hashConflict.WriteString(base58.Encode(ha[:]))
//			hashConflict.WriteString("\n\r")
//			hashConflict.Write(batch[ha])
//
//			hashConflict2.WriteString("hash:")
//			hashConflict2.WriteString(base58.Encode(ha[:]))
//			hashConflict2.WriteString("\n\r")
//			oldData, err := ytfs.Get(ha)
//			if err != nil {
//				fmt.Printf("get hash conflict slice data err,hash:%v", base58.Encode(ha[:]))
//			}
//
//			hashConflict2.Write(oldData)
//			fmt.Printf("find hash conflict, hash:%v", base58.Encode(ha[:]))
//		}
//	}
//}

//var mutexindex uint64 = 0
// BatchPut sets the value array for the given key array.
// It panics if there exists any previous value for that key as YottaDisk is not a multi-map.
// It is safe to modify the contents of the arguments after Put returns but not
// before.

func (ytfs *YTFS) BatchPut(batch map[ydcommon.IndexTableKey][]byte) (map[ydcommon.IndexTableKey]byte, error) {

	var firstShardHash []byte
	for key, _ := range batch {
		firstShardHash = key.Hsh[:]
	}
	fmt.Println("[YTFSPERF]  enter YTFS::BatchPut first_shard_hash : ", base58.Encode(firstShardHash), " ,  batch len : ", len(batch))
	startTime := time.Now()

	if ytfs.config.UseKvDb {
		gcspace, err := ytfs.db.GetDb([]byte(gcspacecntkey))
		fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, ytfs.db.GetDb use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

		if gcspace != nil {
			fmt.Println("[gcdel]  batchput get gcspacecnt len(gcspace)=", len(gcspace))
		}

		if err != nil || gcspace == nil {
			if err != nil {
				fmt.Println("[gcdel]  batchput ytfs.db.GetDb gcspacecnt error:", err)
			}

			return ytfs.BatchPutNormal(batch)
		}

		gccnt := binary.LittleEndian.Uint32(gcspace)
		if gccnt > uint32(len(batch)) {
			return ytfs.BatchPutGc(batch)
		}
	}
	return ytfs.BatchPutNormal(batch)
}

func (ytfs *YTFS) BatchPutGcUnDo(bitmaptab []ydcommon.GcTableItem, num uint32, errcode int) {
	if errcode < 3 {
		return
	}

	gcspace, err := ytfs.db.GetDb([]byte(gcspacecntkey))
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.GetDb gcspacecnt error:", err)
		return
	}

	gccnt := binary.LittleEndian.Uint32(gcspace)
	gccnt = gccnt + num
	space := make([]byte, 4)
	binary.LittleEndian.PutUint32(space, gccnt)
	err = ytfs.db.PutDb([]byte(gcspacecntkey), space)
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.PutDb gcspacecnt error:", err)
		return
	}

	pos := make([]byte, 4)
	for _, gctabItem := range bitmaptab {
		binary.LittleEndian.PutUint32(pos, uint32(gctabItem.Gcval))
		err := ytfs.db.PutDb(gctabItem.Gckey[:], pos)
		if err != nil {
			fmt.Println("[gcdel] delete Gckey:del-", base58.Encode(gctabItem.Gckey[3:]), "from db error:", err)
			return
		}
	}
	return
}

func (ytfs *YTFS) BatchPutGcDo(bitmaptab []ydcommon.GcTableItem, num uint32) (int, error) {
	gcspace, err := ytfs.db.GetDb([]byte(gcspacecntkey))
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.GetDb gcspacecnt error:", err)
		return 1, err
	}

	gccnt := binary.LittleEndian.Uint32(gcspace)
	gccnt = gccnt - num
	space := make([]byte, 4)
	binary.LittleEndian.PutUint32(space, gccnt)
	err = ytfs.db.PutDb([]byte(gcspacecntkey), space)
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.PutDb gcspacecnt error:", err)
		return 2, err
	}

	for i, gctabItem := range bitmaptab {
		if i >= int(num) {
			break
		}
		err := ytfs.db.DeleteDb(gctabItem.Gckey[:])
		if err != nil {
			fmt.Println("[gcdel] delete Gckey:del-",
				base58.Encode(gctabItem.Gckey[3:]), "from db error:", err)
			return 3, err
		}
	}
	return 0, err
}

func (ytfs *YTFS) BatchPutGc(batch map[ydcommon.IndexTableKey][]byte) (map[ydcommon.IndexTableKey]byte, error) {
	var firstShardHash []byte
	for key, _ := range batch {
		firstShardHash = key.Hsh[:]
	}
	fmt.Println("[YTFSPERF]  enter YTFS::BatchPutGc first_shard_hash : ", base58.Encode(firstShardHash), " ,  batch len : ", len(batch))
	startTime := time.Now()

	lenbatch := len(batch)
	GcLock.Lock()
	defer GcLock.Unlock()

	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutGc get lock use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	startTime = time.Now()
	//GcWrtOverNum much use
	bitmaptab, err := ytfs.db.GetBitMapTab(lenbatch + GcWrtOverNum)
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutGc-db.GetBitMapTab use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	if err != nil || len(bitmaptab) < lenbatch {
		fmt.Println("[gcdel] get del bitmaptab error:", err)
		return ytfs.BatchPutNormal(batch)
	}

	startTime = time.Now()

	i := 0
	for key, val := range batch {
		gctabItem := bitmaptab[i]
		pos := gctabItem.Gcval
		_, err := ytfs.context.PutAt(val, uint32(pos))
		if err != nil {
			fmt.Println("[gcdel] put data to disk pos:", pos, "error", err)
			return nil, err
		}
		err = ytfs.db.Put(key, ydcommon.IndexTableValue(pos))
		if err != nil {
			fmt.Println("[gcdel] put indexkey:", base58.Encode(key.Hsh[:]), "to db error", err)
			return nil, err
		}
		i++
	}

	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutGc-db.WriteIndex use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	startTime = time.Now()
	errcode, err := ytfs.BatchPutGcDo(bitmaptab, uint32(i))
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutGc-BatchPutGcDo use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	if err != nil {
		startTime = time.Now()
		ytfs.BatchPutGcUnDo(bitmaptab, uint32(i), errcode)
		fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutGc-BatchPutGcUnDo use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())
	}

	return nil, err
}

func (ytfs *YTFS) BatchPutNormal(batch map[ydcommon.IndexTableKey][]byte) (map[ydcommon.IndexTableKey]byte, error) {
	var firstShardHash []byte
	for key, _ := range batch {
		firstShardHash = key.Hsh[:]
	}
	fmt.Println("[YTFSPERF]  enter YTFS::BatchPutNormal first_shard_hash : ", base58.Encode(firstShardHash), " ,  batch len : ", len(batch))
	startTime := time.Now()

	ytfs.mutex.Lock()
	defer ytfs.mutex.Unlock()

	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutNormal get lock use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	if len(batch) > 1000 {
		return nil, fmt.Errorf("Batch Size is too big")
	}
	fmt.Println("BatchPut len(batch)=", len(batch))

	// NO get check, but retore all status if error
	startTime = time.Now()
	ytfs.saveCurrentYTFS()
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutNormal-saveCurrentYTFS use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	batchIndexes := make([]ydcommon.IndexItem, len(batch))
	batchBuffer := []byte{}
	bufCnt := len(batch)
	i := 0
	for k, v := range batch {
		batchBuffer = append(batchBuffer, v...)
		batchIndexes[i] = ydcommon.IndexItem{
			Hash:      k,
			OffsetIdx: ydcommon.IndexTableValue(0)}
		i++
	}

	startTime = time.Now()
	startPos, err := ytfs.context.BatchPut(bufCnt, batchBuffer)
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutNormal-context.BatchPut use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	if err != nil {
		fmt.Println("[indexdb] ytfs.context.BatchPut error")
		ytfs.restoreYTFS()
		return nil, err
	}

	//update the write position to db
	startTime = time.Now()
	err = ytfs.db.UpdateMeta(uint64(bufCnt))
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutNormal-db.UpdateMeta use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())
	if err != nil {
		fmt.Println("update position error:", err)
		return nil, err
	}

	for i := uint32(0); i < uint32(bufCnt); i++ {
		batchIndexes[i] = ydcommon.IndexItem{
			Hash:      batchIndexes[i].Hash,
			OffsetIdx: ydcommon.IndexTableValue(startPos + i)}

	}
	startTime = time.Now()
	conflicts, err := ytfs.db.BatchPut(batchIndexes)
	fmt.Printf("[YTFSPERF] first_shard_hash %s , batch len: %d, BatchPutNormal-db.BatchPut use [%f]\n", base58.Encode(firstShardHash), len(batch), time.Now().Sub(startTime).Seconds())

	if err != nil {
		fmt.Println("update K-V to DB error:", err)
		return conflicts, err
	}

	return nil, nil
}

func (ytfs *YTFS) GetCapProofSpace() uint32 {
	ytfs.mutex.Lock()
	defer ytfs.mutex.Unlock()
	buf := make([]byte, ytfs.config.DataBlockSize)

	storageContexts := ytfs.context.GetStorageContext()
	var useCap uint32
	var useAbleCap uint32
	for _, storage := range storageContexts {
		confCap := storage.Cap
		RealCap := storage.RealDiskCap
		if confCap >= RealCap {
			useCap += RealCap
		} else {
			useCap += confCap
		}
		if useCap < 1 {
			continue
		}
		fmt.Printf("[cap proof] dev name %s, config cap %d, real cap %d\n", storage.Name, confCap, RealCap)
		rand.Read(buf)
		useAbleCap = ytfs.context.GetAvailablePos(buf, useCap-1)
		if useAbleCap+1 != useCap {
			break
		}
	}

	return ytfs.context.RandCheckAvailablePos(buf, 10, useAbleCap) + 1
}

func (ytfs *YTFS) TruncatStorageFile() {
	ytfs.mutex.Lock()
	defer ytfs.mutex.Unlock()

	storageContexts := ytfs.context.GetStorageContext()
	for _, storage := range storageContexts {
		if storage.Disk.GetStorage().GetFd().Type == ydcommon.FileStorageType {
			st, err := os.Stat(storage.Name)
			if err != nil {
				fmt.Printf("ytfs truncat %s get stat err %s\n", storage.Name, err.Error())
				continue
			}
			statSize := st.Size()
			configSize := storage.Disk.GetStorageHeader().DiskCapacity
			//configSize := int64(storage.Disk.Capability())
			if uint64(statSize) > configSize {
				err = os.Truncate(storage.Name, int64(configSize))
				if err != nil {
					fmt.Printf("ytfs truncat %s err %s\n", storage.Name, err.Error())
				} else {
					fmt.Printf("ytfs truncat %s cursize %d, after truncat size %d\n",
						storage.Name, statSize, configSize)
				}
			} else {
				fmt.Printf("ytfs storage %s don't need truncat, file size %d, config size %d\n",
					storage.Name, statSize, configSize)
			}
		}
	}

	return
}

// Meta reports current meta information.
func (ytfs *YTFS) Meta() *ydcommon.Header {
	//	return ytfs.db.(*IndexDB).schema
	return ytfs.db.Meta()
}

func (ytfs *YTFS) Totalsize() uint64 {
	return ytfs.db.TotalSize()
}

func (ytfs *YTFS) BlkSize() uint32 {
	return ytfs.db.BlockSize()
}

// Close closes the YTFS.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (ytfs *YTFS) Close() {
	ytfs.db.Close()
	ytfs.context.Close()
}

// Reset resets an existed YottaDisk, and make it ready
// for next put/get operation. so far we do quick format which just
// erases the header.
func (ytfs *YTFS) Reset() error {
	ytfs.db.Reset()
	ytfs.context.Reset()
	return nil
}

// Cap report capacity of YTFS, just like cap() of a slice
func (ytfs *YTFS) Cap() uint64 {
	cap := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		cap += uint64(stroageCtx.Cap)
	}
	return cap
}

// Len report len of YTFS, just like len() of a slice
func (ytfs *YTFS) Len() uint64 {
	//return ytfs.db.(*IndexDB).schema.DataEndPoint
	return ytfs.db.Len()
}

// String reports current YTFS status.
func (ytfs *YTFS) String() string {
	meta, _ := json.MarshalIndent(ytfs.db.(*IndexDB).schema, "", "	")
	// min := (int64)(math.MaxInt64)
	// max := (int64)(math.MinInt64)
	// sum := (int64)(0)
	// table := fmt.Sprintf("Total table Count: %d\n"+
	// 	"Total saved items: %d\n"+
	// 	"Maximum table size: %d\n"+
	// 	"Minimum table size: %d\n"+
	// 	"Average table size: %d\n", len(disk.index.sizes), sum, max, min, avg)
	return string(meta) + "\n"
}

func (ytfs *YTFS) ScanDB() {

}

func (ytfs *YTFS) YtfsDB() DB {
	return ytfs.db
}

var hash0Str = "0000000000000000"

type Hashtohash struct {
	DBhash   []byte
	Datahash []byte
	Pos      uint32
	Hid      int64
}

func (ytfs *YTFS) VerifySliceOne(key ydcommon.IndexTableKey) (Hashtohash, error) {
	var errHash Hashtohash
	pos, hid, err := ytfs.db.Get(key)
	slice, err := ytfs.Get(key)
	if err != nil {
		fmt.Println("[verify]get slice fail, key=", base58.Encode(key.Hsh[:]))
		errHash.DBhash = key.Hsh[:]
		errHash.Datahash = []byte(hash0Str)
		errHash.Hid = int64(hid)
		return errHash, err
	}

	sha := md5.New()
	sha.Write(slice)
	if !bytes.Equal(sha.Sum(nil), key.Hsh[:]) {
		err = fmt.Errorf("verify error")
		errHash.DBhash = key.Hsh[:]
		errHash.Datahash = sha.Sum(nil)
		errHash.Pos = uint32(pos)
		errHash.Hid = int64(hid)
		fmt.Printf("VerifySliceOne pos %d,  key=%s Hid=%d\n",
			pos, base58.Encode(key.Hsh[:]), hid)

		return errHash, err
	}
	return errHash, nil
}

func (ytfs *YTFS) VerifySlice(startkey string, traveEntries uint64) ([]Hashtohash, string, error) {
	//log.Println("[verify] VerifySlice start")
	retSlice, beginkey, err := ytfs.db.TravelDBforverify(ytfs.VerifySliceOne, startkey, traveEntries)
	return retSlice, beginkey, err
}

func (ytfs *YTFS) VerifyHashSlice(key ydcommon.IndexTableKey, slice []byte) bool {
	sha := md5.New()
	sha.Write(slice)
	return bytes.Equal(sha.Sum(nil), key.Hsh[:])
}

func (ytfs *YTFS) GetBitMapTab(num int) ([]ydcommon.GcTableItem, error) {
	return ytfs.db.GetBitMapTab(num)
}

func (ytfs *YTFS) GcDelBitMap(bitMapTable []ydcommon.GcTableItem) (succs, fails uint32) {
	for _, v := range bitMapTable {
		err := ytfs.db.DeleteDb(v.Gckey[:])
		if err != nil {
			fails++
		} else {
			succs++
		}
	}

	return
}

func (ytfs *YTFS) GcProcess(key ydcommon.IndexTableKey) error {
	var err error
	fmt.Println("[gcdel] GcProcess A start collect space key=", base58.Encode(key.Hsh[:]))
	slice, err := ytfs.Get(key)
	if err != nil {
		fmt.Println("[gcdel] get slice error:", err, "key=", base58.Encode(key.Hsh[:]))
		return err
	}
	fmt.Println("[gcdel] GcProcess B verify collect space key=", base58.Encode(key.Hsh[:]))
	if !ytfs.VerifyHashSlice(key, slice) {
		err = fmt.Errorf("verify data error!")
		slicehs := md5.Sum(slice)
		fmt.Println("[gcdel] verify data error, hash:", base58.Encode(key.Hsh[:]), "slice hash:", base58.Encode(slicehs[:]))

		err1 := ytfs.db.Delete(key)
		if err1 != nil {
			fmt.Println("[gcdel]  ytfs.db.Delete error:", err)
		}
		return err
	}

	fmt.Println("[gcdel] GcProcess C renamekey collect space key=", base58.Encode(key.Hsh[:]))
	pos, _, _ := ytfs.db.Get(key)
	if pos < 5 {
		err = fmt.Errorf("reserve data block, pos<5")
		return err
	}

	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, uint32(pos))

	gckey := []byte("del")
	gckey = append(gckey, key.Hsh[:]...)
	err = ytfs.db.PutDb(gckey, val)
	if err != nil {
		fmt.Println("[gcdel] PutDB error:", err)
		return err
	}

	fmt.Println("[gcdel] GcProcess D deletekey collect space key=", base58.Encode(key.Hsh[:]))
	err = ytfs.db.Delete(key)
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.Delete error:", err)
		return err
	}

	fmt.Println("[gcdel] GcProcess E get_old_gcspace collect space key=", base58.Encode(key.Hsh[:]))
	GcLock.Lock()
	defer GcLock.Unlock()
	gccnt := uint32(0)
	gcspace, err := ytfs.db.GetDb([]byte(gcspacecntkey))
	if err != nil {
		if gcspace != nil {
			gccnt = binary.LittleEndian.Uint32(gcspace)
		}
		fmt.Println("[gcdel]  ytfs.db.GetDb gcspacecnt error:", err, "gcspacecnt", gccnt)
		return err
	}

	fmt.Println("[gcdel] GcProcess F resize_gcspace collect space key=", base58.Encode(key.Hsh[:]))

	if gcspace != nil {
		gccnt = binary.LittleEndian.Uint32(gcspace)
	}
	fmt.Println("[gcdel] GcProcess G resize_gcspace collect space key=", base58.Encode(key.Hsh[:]), "gcspacecnt", gccnt)

	gccnt++
	binary.LittleEndian.PutUint32(val, gccnt)
	err = ytfs.db.PutDb([]byte(gcspacecntkey), val)
	if err != nil {
		fmt.Println("[gcdel]  ytfs.db.PutDb gcspacecnt error:", err)
	}
	fmt.Println("[gcdel] GcProcess H end collect space key=", base58.Encode(key.Hsh[:]))

	return err
}

func (ytfs *YTFS) GetGcNums() uint32 {
	GcLock.Lock()
	defer GcLock.Unlock()
	gccnt := uint32(0)
	gcspace, err := ytfs.db.GetDb([]byte(gcspacecntkey))
	if err != nil {
		if gcspace != nil {
			gccnt = binary.LittleEndian.Uint32(gcspace)
		}
		fmt.Println("[gcdel]  ytfs.db.GetDb gcspacecnt error:", err, "gcspacecnt", gccnt)
		return 0
	}

	if gcspace != nil {
		gccnt = binary.LittleEndian.Uint32(gcspace)
	}

	return gccnt
}

func (ytfs *YTFS) PutGcNums(nums uint32) error {
	space := make([]byte, 4)
	binary.LittleEndian.PutUint32(space, nums)
	err := ytfs.db.PutDb([]byte(gcspacecntkey), space)
	if err != nil {
		fmt.Println("ytfs.db.PutDb gcspacecnt error:", err)
		return err
	}

	return nil
}

func (ytfs *YTFS) PosIdx() uint64 {
	return ytfs.db.PosPtr()
}

func (ytfs *YTFS) ModifyPos(pos uint64) error {
	return ytfs.db.ModifyMeta(pos)
}

func (ytfs *YTFS) magrateData(key, value []byte) error {
	dataPos := binary.LittleEndian.Uint32(value)
	if uint64(dataPos) > ytfs.PosIdx() {
		Hkey := ydcommon.IndexTableKey{Hsh: ydcommon.BytesToHash(key), Id: 0}
		shard, err := ytfs.Get(Hkey)
		if err != nil {
			log.Printf("[magrate] get hash err:%s, key:%s\n",
				err.Error(), base58.Encode(key))
			return err
		}

		err = ytfs.Put(Hkey, shard)
		if err != nil {
			hash := md5.Sum(shard)
			log.Printf("[magrate] put hash err:%s, key:%s, shard:%s\n",
				err.Error(), base58.Encode(key), base58.Encode(hash[:]))
			return err
		}
	}

	return nil
}
