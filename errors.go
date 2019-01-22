package yottadisk

import (
	"github.com/yottachain/YTFS/errors"
)

// Common errors.
var (
	ErrDataNotFound		= errors.ErrDataNotFound
	ErrHeaderNotFound	= errors.ErrHeadNotFound
	ErrConflict         = errors.New("YTFS: conflict hash value")
	ErrRangeFull        = errors.New("YTFS: Range is full")
	ErrReadOnly         = errors.New("YTFS: read-only mode")
	ErrClosed           = errors.New("YTFS: closed")
)
