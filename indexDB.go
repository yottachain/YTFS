package ytfs

import (
	"encoding/binary"
	"fmt"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
	//"fmt"
	"path"
	"sort"

	"os"
)

// IndexDB key value db for hash <-> position.
type IndexDB struct {
	// meta data
	schema *ydcommon.Header

	// index file
	indexFile *storage.YTFSIndexFile
}

func (db *IndexDB) Len() uint64 {
	return db.schema.DataEndPoint
}

func (db *IndexDB) PosPtr() uint64 {
	return db.schema.DataEndPoint
}

func (db *IndexDB) BlockSize() uint32 {
	return db.schema.DataBlockSize
}

func (db *IndexDB) TotalSize() uint64 {
	return db.schema.YtfsSize
}

func (db *IndexDB) Meta() *ydcommon.Header {
	return db.schema
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func CheckDbStatus(dir, file1, file2 string) bool {
	fileName1 := path.Join(dir, "index.db")
	fileName2 := path.Join(dir, "metadata.db")
	var bl = false
	if PathExists(fileName1) && PathExists(fileName2) {
		bl = true
	}
	return bl
}

func (db *IndexDB) GetReserved() uint32 {
	return db.indexFile.GetReversed()
}

func (db *IndexDB) SetReserved(reserved uint32) error {
	valueBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueBuf, reserved)
	err := db.indexFile.SetReversed(valueBuf)
	return err
}

func (db *IndexDB) SetDnIdToIdxDB(dnid uint32) error {
	var err error
	Bdn := make([]byte, 4)
	binary.LittleEndian.PutUint32(Bdn, dnid)
	err = db.indexFile.SetDnIdToIdxDB(Bdn)
	return err
}

func (db *IndexDB) SetVersionToIdxDB(version string) error {
	var err error
	vs := []byte(version)
	//binary.LittleEndian.PutUint32(Bdn, dnid)
	err = db.indexFile.SetVersionToIdxDB(vs[0:4])
	return err
}

func (db *IndexDB) GetDnIdFromIdxDB() uint32 {
	return db.indexFile.GetDnIdFromIdxDB()
}

func (db *IndexDB) CheckDbDnId(dnid uint32) (bool, error) {
	fmt.Println("version=", string(db.schema.Version[:]))
	var err error
	var dbDn uint32
	if string(db.schema.Version[:]) == OldDbVersion {
		err = db.SetDnIdToIdxDB(dnid)
		if err != nil {
			fmt.Println("SetDnIdToIdxDB error", err.Error())
			return false, err
		}
		_ = db.SetVersionToIdxDB(DbVersion)
	} else {
		dbDn = db.GetDnIdFromIdxDB()
		if dbDn != dnid {
			fmt.Println("error: dnid not equal,db=", dbDn, " cfg=", dnid)
			err = fmt.Errorf("dnid(db) not equal dnid(cfg)")
			return false, err
		}
	}
	fmt.Println("CheckDbDnId, db=", dbDn, " cfg=", dnid)
	return true, nil
}

// NewIndexDB creates a new index db based on input file if it's exist.
func NewIndexDB(dir string, config *opt.Options, init bool) (*IndexDB, error) {
	fileName := path.Join(dir, "index.db")

	indexFile, err := storage.OpenYTFSIndexFile(fileName, config, init)
	if err != nil {
		return nil, err
	}

	err = validateDBSchema(indexFile.MetaData(), config)
	if err != nil {
		return nil, err
	}

	return &IndexDB{
		schema:    indexFile.MetaData(),
		indexFile: indexFile,
	}, nil
}

// Get queries value corresponding to the input key.
func (db *IndexDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, ydcommon.HashId, error) {
	return db.indexFile.Get(key)
}

// Put add new key value pair to db.
func (db *IndexDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	kvPairs := make([]ydcommon.IndexItem, 1)
	kvPairs[0] = ydcommon.IndexItem{
		Hash:      key,
		OffsetIdx: value}
	//return db.indexFile.Put(key, value)
	_, err := db.indexFile.BatchPut(kvPairs)
	return err
}

func (db *IndexDB) UpdateMeta(accout uint64) error {
	return db.indexFile.UpdateMeta(accout)
}

func (db *IndexDB) ModifyMeta(accout uint64) error {
	return db.indexFile.ModifyMeta(accout)
}

// BatchPut add a set of new key value pairs to db.
func (db *IndexDB) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
	// sorr kvPair by hash entry to make sure write in sequence.
	sort.Slice(kvPairs, func(i, j int) bool {
		return db.indexFile.GetTableEntryIndex(kvPairs[i].Hash) < db.indexFile.GetTableEntryIndex(kvPairs[j].Hash)
	})

	// var err error
	// for _, v := range kvPairs{
	// 		err = db.indexFile.Put(v.Hash, v.OffsetIdx)
	// 		if err != nil {
	// 				return err
	// 		}
	// }
	// return nil
	return db.indexFile.BatchPut(kvPairs)
}

// Close finishes all actions and close db connection.
func (db *IndexDB) Close() {
	db.indexFile.Close()
}

// Reset finishes all actions and close db connection.
func (db *IndexDB) Reset() {
	db.indexFile.Format()
}

func (db *IndexDB) TravelDB(fn func(key, val []byte) error) int64 {
	//var DBIter storage.TableIterator
	ytIndexFile := db.indexFile
	options := db.indexFile.GetYTFSIndexFileOpts()
	DBIter := storage.GetIdxDbIter(ytIndexFile, options)
	succ := int64(0)

	exitStatus := false

	for {
		tab, err := DBIter.GetNoNilTableBytes()
		if err != nil {
			fmt.Println("[indexdb] get table error :", err)
			if err.Error() != "table_end" {
				continue
			} else {
				exitStatus = true
			}
		}

		for key, val := range tab {
			err := fn(key.Hsh[:], val)
			if err != nil {
				fmt.Println("[indexdb] TravelDB error: ", err)
				continue
			}
			succ++
		}

		if exitStatus {
			break
		}
	}
	return succ
}

func (db *IndexDB) TravelDBforverify(fn func(key ydcommon.IndexTableKey) (Hashtohash, error), startkey string, traveEntries uint64) ([]Hashtohash, string, int, error) {
	//var DBIter storage.TableIterator
	var err error
	var retSlice []Hashtohash
	var beginKey string
	beginKey = ""
	//ytIndexFile := db.indexFile
	//options := db.indexFile.GetYTFSIndexFileOpts()
	//DBIter:= storage.GetIdxDbIter(ytIndexFile, options)
	//errCnt := int64(0)

	//for {
	//	tab,err:=DBIter.GetNoNilTableBytes()
	//	if err != nil {
	//		fmt.Println("[indexdb] get table error :",err)
	//		//continue
	//		return errCnt,err
	//	}
	//
	//	for key, val := range tab{
	//		_, err := fn(key[:],val)
	//		if err != nil{
	//			errCnt++
	//			return errCnt,err
	//		}
	//		//if !b {
	//		//	fmt.Println("[indexdb] TravelDB error: ",err)
	//		//	continue
	//		//}
	//		//succ++
	//	}
	//}
	return retSlice, beginKey, 0, err
}

func validateDBSchema(meta *ydcommon.Header, opt *opt.Options) error {
	if opt.UseKvDb {
		fmt.Println("[rocksdb] using rocksdb")
		if meta.DataBlockSize != opt.DataBlockSize {
			fmt.Println("[rocksdb] config datablock size miss match")
			return ErrConfigIndexMismatch
		}
		return nil
	}

	if meta.YtfsCapability != opt.TotalVolumn || meta.DataBlockSize != opt.DataBlockSize {
		return ErrConfigIndexMismatch
	}
	return nil
}

func (db *IndexDB) ScanDB() {
	//db.indexFile.Format()
}

func (db *IndexDB) Delete(key ydcommon.IndexTableKey) error {
	err := fmt.Errorf("not support!")
	return err
}

func (db *IndexDB) PutDb(key, value []byte) error {
	err := fmt.Errorf("not support!")
	return err
	//return db.Rdb.Put(rd.wo,key,value)
}

func (db *IndexDB) GetDb(key []byte) ([]byte, error) {
	err := fmt.Errorf("not support!")
	return nil, err
}

func (db *IndexDB) DeleteDb(key []byte) error {
	err := fmt.Errorf("not support!")
	return err
}

func (db *IndexDB) GetBitMapTab(num int) ([]ydcommon.GcTableItem, error) {
	err := fmt.Errorf("not support!")
	return nil, err
}

func (rd *IndexDB) GetDBKeysNum() uint64 {
	return 0
}

func (rd *IndexDB) PutCapCurSrcData(curData []byte) (err error) {
	return nil
}

func (rd *IndexDB) GetCapProofInitStat() (bool, error) {
	return false, nil
}

func (rd *IndexDB) PutCapProofInitStat() (err error) {
	return nil
}
