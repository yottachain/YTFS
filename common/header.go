// Package common provides common types used throughout YottaDisk.
package common

// Header header of YTFS
type Header struct {
	Tag            [4]byte `json:"tag"`
	Version        [4]byte `json:"version"`
	YtfsCapability uint64  `json:"ytfsCaps"` // max supported size.
	YtfsSize       uint64  `json:"ytfsSize"` // current enabled, i.e, sum of plugin storage. for consistency check.
	DataBlockSize  uint32  `json:"dataBlkSize"`
	RangeCapacity  uint32  `json:"rangeCapacity"`
	RangeCoverage  uint32  `json:"rangeCoverage"`
	HashOffset     uint32  `json:"hashOffset"`
	DataEndPoint   uint64  `json:"dataEndPoint"` // if no del, it is the data count, if have del, it tells the sp of context.
	RecycleOffset  uint64  `json:"RecycleOffset"`
	Reserved       uint64  `json:"reserved"`
}

// StorageHeader header of storage
type StorageHeader struct {
	Tag           [4]byte `json:"tag"`
	Version       [4]byte `json:"version"`
	DiskCapacity  uint64  `json:"diskCapacity"`
	DataBlockSize uint32  `json:"dataBlkSize"`
	DataOffset    uint32  `json:"dataOffset"`
	DataCapacity  uint32  `json:"DataCapacity"`
	Reserved      uint32  `json:"reserved"`
}
