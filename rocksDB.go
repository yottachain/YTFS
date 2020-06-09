package ytfs

import (
	"github.com/tecbot/gorocksdb"
	ydcommon "github.com/yottachain/YTFS/common"
)

type RocksDB struct {
	Rdb *gorocksdb.DB
	ro  *gorocksdb.ReadOptions
	wo  *gorocksdb.WriteOptions
}

func (rd *RocksDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	return 0, nil
}

func (rd *RocksDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	return nil
}

func (rd *RocksDB) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
	return nil, nil
}

func (rd *RocksDB) Close() {
}

func (rd *RocksDB) Reset() {
}
