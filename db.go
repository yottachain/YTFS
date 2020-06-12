package ytfs

import ydcommon "github.com/yottachain/YTFS/common"

type DB interface {
	//Type() string
	Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error)
	Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error
	BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error)
	Len() uint64
	TotalSize() uint64
	BlockSize() uint32
	Meta() *ydcommon.Header
	Close()
	Reset()
}
