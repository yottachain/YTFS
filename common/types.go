// Package common provides common types used throughout YottaDisk.
package common

import (
	"errors"

	//"github.com/ethereum/go-ethereum/common"
)

// StorageType represent a file type.
type StorageType int

const (
	// FileStorageType File storage type
	FileStorageType StorageType = iota
	// BlockStorageType Disk storage type
	BlockStorageType
	// DummyStorageType Dummy storage type
	DummyStorageType
)

const (
    HashLength = 16
)

type Hash [HashLength]byte

func BytesToHash(b []byte) Hash {
	var h Hash
	h.SetBytes(b)
	return h
}

func (h *Hash) SetBytes(b []byte) {
	if len(b) > len(h) {
		b = b[len(b)-HashLength:]
	}

	copy(h[HashLength-len(b):], b)
}

func HexToHash(s string) Hash { return BytesToHash(FromHex(s)) }

type IndexTableKey Hash
type IndexTableValue uint32
type IndexTable map[IndexTableKey]IndexTableValue
type IndexItem struct {
	Hash      IndexTableKey
	OffsetIdx IndexTableValue
}

// IsPowerOfTwo tells if x is power of 2
func IsPowerOfTwo(x uint64) bool {
	return (x == 0) || ((x != 0) && ((x & (x - 1)) == 0))
}

// YottaAssert asserts condition
func YottaAssert(condition bool) {
	if !condition {
		panic(errors.New("Assert Failed"))
	}
}

// YottaAssertMsg asserts condition with message
func YottaAssertMsg(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
