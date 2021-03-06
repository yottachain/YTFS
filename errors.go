package ytfs

import (
	"github.com/yottachain/YTFS/errors"
)

// Common errors.
var (
	ErrDataConflict        = errors.New("YTFS: hash key conflict happens")
	ErrDirNameConflict     = errors.New("YTFS: ytfs can not open dir because of name conflict")
	ErrEmptyYTFSDir        = errors.New("YTFS: dir has no ytfs contents")
	ErrSettingMismatch     = errors.New("YTFS: ytfs initailize failed because new config not consistent")
	ErrConfigIndexMismatch = errors.New("YTFS: ytfs initailize failed because indexDB and config mismatch")
)
