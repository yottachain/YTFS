package ytfs

import ydcommon "github.com/yottachain/YTFS/common"

type DB interface {
	//Type() string
	Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, ydcommon.HashId, error)
	Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error
	PutDb(key, value []byte) error
	GetDb(key []byte) ([]byte, error)
	GetBitMapTab(num int) ([]ydcommon.GcTableItem, error)
	Delete(key ydcommon.IndexTableKey) error
	DeleteDb(key []byte) error
	BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error)
	UpdateMeta(account uint64) error
	ModifyMeta(account uint64) error
	TravelDB(fn func(key, value []byte) error) int64
	TravelDBforverify(fn func(key ydcommon.IndexTableKey) (Hashtohash, error), startkey string, traveEntries uint64) ([]Hashtohash, string, error)
	Len() uint64
	PosPtr() uint64
	TotalSize() uint64
	BlockSize() uint32
	Meta() *ydcommon.Header
	Close()
	Reset()
	ScanDB()
	GetReserved() uint32
	SetReserved(reserved uint32) error
	CheckDbDnId(uint32) (bool, error)
	//GcProcess(fn func(key ydcommon.IndexTableKey) (Hashtohash,error)) error
}
