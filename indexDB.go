package ytfs

import (
	"path"
	"sort"
	"fmt"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
)

// IndexDB key value db for hash <-> position.
type IndexDB struct {
	// meta data
	schema *ydcommon.Header

	// index file
	indexFile *storage.YTFSIndexFile
}

// NewIndexDB creates a new index db based on input file if it's exist.
func NewIndexDB(dir string, config *opt.Options) (*IndexDB, error) {
	fileName := path.Join(dir, "index.db")
	indexFile, err := storage.OpenYTFSIndexFile(fileName, config)
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
func (db *IndexDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	return db.indexFile.Get(key)
}

// Put add new key value pair to db.
func (db *IndexDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	return db.indexFile.Put(key, value)
}

// BatchPut add a set of new key value pairs to db.
func (db *IndexDB) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
	// sorr kvPair by hash entry to make sure write in sequence.
	sort.Slice(kvPairs, func(i, j int) bool {
		fmt.Println("[memtrace] after sort.Slice ")
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
