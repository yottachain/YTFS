package ytfs

import ydcommon "github.com/yottachain/YTFS/common"

type DB interface {
	//Type() string
	Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error)
	Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error
	BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error)
	UpdateMeta(account uint64) error
	TravelDB(fn func(key, value []byte) error) int64
	TravelDBforverify(fn func(ydcommon.IndexTableKey) ([]byte,error),startkey string, traveEntries uint64) (int64, error)
	Len() uint64
	TotalSize() uint64
	BlockSize() uint32
	Meta() *ydcommon.Header
	Close()
	Reset()
	ScanDB()
}
