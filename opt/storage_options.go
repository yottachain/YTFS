// Package opt provides sets of options used by YottaDisk.
package opt

import (
	"errors"
	"io/ioutil"

	ytfs "github.com/yottachain/YTFS/common"
)

// config errors
var (
	ErrStorageConfigN          = errors.New("yotta storage config: config.N should be power of 2 and less than MAX_RANGE")
	ErrStorageConfigM          = errors.New("yotta storage config: config.M setting is incorrect")
	ErrStorageConfigMetaPeriod = errors.New("yotta storage config: Meta sync period should be power of 2")
)

// StorageOptions sets options of YTFS storage
type StorageOptions struct {
	StorageName   string           `json:"storage"`
	StorageType   ytfs.StorageType `json:"type"`
	ReadOnly      bool             `json:"readonly"`
	SyncPeriod    uint32           `json:"syncPeriod"`
	StorageVolume uint64           `json:"storageSize"`
	DataBlockSize uint32           `json:"dataBlockSize"`
}

// Equal compares 2 StorageOptions to tell if it is equal
func (opt *StorageOptions) Equal(other *StorageOptions) bool {
	return opt.StorageName == other.StorageName && opt.StorageType == other.StorageType && opt.StorageVolume == other.StorageVolume && opt.DataBlockSize == other.DataBlockSize
}

// DefaultStorageOptions default config
func DefaultStorageOptions() *StorageOptions {
	tmpFile, err := ioutil.TempFile("", "yotta-play")
	if err != nil {
		panic(err)
	}

	config := &StorageOptions{
		StorageName:   tmpFile.Name(),
		StorageType:   ytfs.FileStorageType,
		ReadOnly:      false,
		SyncPeriod:    1,
		StorageVolume: 1 << 20, // 1M for default
		DataBlockSize: 32,      // Just save HashLen for test.
	}

	return config
}
