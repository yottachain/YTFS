package yottadisk

import (
	"github.com/yotta-disk/errors"
)

// Common errors.
var (
	ErrDataNotFound		= errors.ErrDataNotFound
	ErrHeaderNotFound	= errors.ErrHeadNotFound
	ErrConflict         = errors.New("yotta-disk: conflict happens as 1 hash table overflows")
	ErrRangeFull        = errors.New("yotta-disk: Range is full")
	ErrReadOnly         = errors.New("yotta-disk: read-only mode")
	ErrClosed           = errors.New("yotta-disk: closed")
)
