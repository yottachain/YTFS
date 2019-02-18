// Package common provides common types used throughout YottaDisk.
package common

// Header header of storage
type Header struct {
	Tag				[4]byte	`json:"tag"`
	Version			[4]byte	`json:"version"`
	DiskCaps		uint64	`json:"diskCaps"`
	DataBlockSize	uint32	`json:"dataBlkSize"`
	RangeCaps		uint32	`json:"rangeCaps"`
	RangeCoverage	uint32	`json:"rangeCoverage"`
	RangeOffset		uint32	`json:"rangeOffset"`
	HashOffset		uint64	`json:"hashOffset"`
	DataOffset		uint64	`json:"dataOffset"`
	DataCount		uint32	`json:"dataCount"`
	AllocOffset		uint64	`json:"allocationOffset"`
	ResolveOffset	uint64	`json:"resolveOffset"`
	Reserved		uint64	`json:"reserved"`
}