package ytfs

import ydcommon "github.com/yottachain/YTFS/common"

type DB interface {
	//Type() string
	Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error)
	Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error
	BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error)
	UpdateMeta(account uint64) error
	TravelDBforFn(fn func(key, value []byte) ([]byte,error),startkey string, traveTimes uint64) (int64, error)
	Len() uint64
	TotalSize() uint64
	BlockSize() uint32
	Meta() *ydcommon.Header
	Close()
	Reset()
	ScanDB()
}
