// Package common provides common types used throughout YottaDisk.
package common


// Header header of YTFS
type Header struct {
	Tag				[4]byte	`json:"tag"`
	Version			[4]byte	`json:"version"`
	YtfsCapability	uint64	`json:"ytfsCaps"` // max supported size.
	YtfsSize        uint64  `json:"ytfsSize"` // current enabled, i.e, sum of plugin storage. for consistency check.
	DataBlockSize	uint32	`json:"dataBlkSize"`
	RangeCaps		uint32	`json:"rangeCaps"`
	RangeCoverage	uint32	`json:"rangeCoverage"`
	HashOffset		uint32	`json:"hashOffset"`
	DataCount		uint64	`json:"dataCount"`
	ResolveOffset	uint64	`json:"resolveOffset"`
	Reserved		uint64	`json:"reserved"`
}

// StorageHeader header of storage
type StorageHeader struct {
	Tag				[4]byte	`json:"tag"`
	Version			[4]byte	`json:"version"`
	DiskCaps		uint64	`json:"diskCaps"`
	DataBlockSize	uint32	`json:"dataBlkSize"`
	DataOffset		uint32	`json:"dataOffset"`
	DataCount       uint32	`json:"dataCount"`
	DataCaps        uint32  `json:"dataCaps"`
	Reserved		uint64	`json:"reserved"`
}