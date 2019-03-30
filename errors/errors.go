// Package errors provides common error types used throughout YottaDisk.
package errors

import (
	"errors"
)

// Common errors.
var (
	ErrHeadNotFound     = errors.New("YTFS: head not found")
	ErrDataNotFound     = errors.New("YTFS: data not found")
	ErrDataOverflow     = errors.New("YTFS: overflow happens, all data disk full")
	ErrContextOverflow  = errors.New("YTFS: context can not support >32bit address")
	ErrConfigCache      = errors.New("YTFS: Cache size config error")
	ErrStorageSize      = errors.New("YTFS: storage size does not meet settings")
	ErrStorageType      = errors.New("YTFS: Unknown storage type")
	ErrStorageHeader    = errors.New("YTFS: existing storage header does not meet settings")
	ErrContextIDMapping = errors.New("YTFS: context mapping global id to device failed")
	ErrConflict         = errors.New("YTFS: conflict hash value")
	ErrRangeFull        = errors.New("YTFS: Range is full")
	ErrReadOnly         = errors.New("YTFS: read-only mode")
	ErrClosed           = errors.New("YTFS: closed")
)

// New returns an error that formats as the given text.
func New(text string) error {
	return errors.New(text)
}
