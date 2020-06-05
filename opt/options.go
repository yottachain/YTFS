// Package opt provides sets of options used by YottaDisk.
package opt

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
	"math/bits"

	// "unsafe"

	// "github.com/ethereum/go-ethereum/common"

	ytfs "github.com/yottachain/YTFS/common"
)

// common size limitations
const (
	// MaxDiskCapability = 1 << 47 // 128T
	MaxRangeCoverage = math.MaxInt32 // 2G
	MaxRangeNumber   = math.MaxInt32 // 2G
)

// config errors
var (
	ErrConfigC          = errors.New("yotta config: config.C should in range [sum(Ti), MaxDiskCapability]")
	ErrConfigN          = errors.New("yotta config: config.N should be power of 2 and less than MAX_RANGE")
	ErrConfigD          = errors.New("yotta config: config.D should be consistent with YTFS")
	ErrConfigM          = errors.New("yotta config: config.M setting is incorrect")
	ErrConfigSyncPeriod = errors.New("yotta config: config.SyncPeriod setting is not power of 2")
)

// Options Config options
type Options struct {
	YTFSTag        string           `json:"ytfs"`
	Storages       []StorageOptions `json:"storages"`
	SyncPeriod     uint32           `json:"syncPeriod"`
	ReadOnly       bool             `json:"readonly"`
	IndexTableCols uint32           `json:"M"`
	IndexTableRows uint32           `json:"N"`
	DataBlockSize  uint32           `json:"D"`
	TotalVolumn    uint64           `json:"C"`
	UseKvDb  	   bool             `json:"UseKvDb"`
}

// Equal compares 2 Options to tell if it is equal
func (opt *Options) Equal(other *Options) bool {
	bEqual := opt.YTFSTag == other.YTFSTag && opt.IndexTableCols == other.IndexTableCols &&
		opt.IndexTableRows == other.IndexTableRows && opt.DataBlockSize == other.DataBlockSize && opt.TotalVolumn == other.TotalVolumn

	if bEqual {
		// check storages
		i, j := len(opt.Storages), len(other.Storages)
		if i <= j {
			// support expension only
			for k := 0; k < i && bEqual; k++ {
				bEqual = opt.Storages[k].Equal(&other.Storages[k])
			}
		}
	}

	return bEqual
}

// DefaultOptions default config
func DefaultOptions() *Options {
	tmpFile1, err := ioutil.TempFile("", "yotta-play-1")
	if err != nil {
		panic(err)
	}
	tmpFile2, err := ioutil.TempFile("", "yotta-play-2")
	if err != nil {
		panic(err)
	}

	config := &Options{
		YTFSTag: "ytfs default setting",
		Storages: []StorageOptions{
			{
				StorageName:   tmpFile1.Name(),
				StorageType:   ytfs.FileStorageType,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: 1 << 20,
				DataBlockSize: 1 << 15,
			},
			{
				StorageName:   tmpFile2.Name(),
				StorageType:   ytfs.FileStorageType,
				ReadOnly:      false,
				SyncPeriod:    1,
				StorageVolume: 1 << 20,
				DataBlockSize: 1 << 15,
			},
		},
		ReadOnly:       false,
		SyncPeriod:     1,
		IndexTableCols: 0,
		IndexTableRows: 1 << 13,
		DataBlockSize:  1 << 15, // Just save HashLen for test.
		TotalVolumn:    2 << 30, // 1G
	}

	newConfig, err := FinalizeConfig(config)
	if err != nil {
		panic(err)
	}

	return newConfig
}

// ParseConfig parses a json config file and return a valid *Options
func ParseConfig(fileName string) (*Options, error) {
	dat, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	config := Options{}
	err = json.Unmarshal(dat, &config)
	if err != nil {
		return nil, err
	}

	newConfig, err := FinalizeConfig(&config)
	if err != nil {
		return nil, err
	}

	return newConfig, nil
}

// SaveConfig saves config to file.
func SaveConfig(config *Options, fileName string) error {
	return nil
}

// FinalizeConfig finalizes the config, it does following things:
// 1. Do a few calculation according to config setting.
// 2. Check if config setting is valid.
func FinalizeConfig(config *Options) (*Options, error) {
	// check C in range
	maxDiskCapability := uint64(1) << ((uint32)(bits.Len32(config.DataBlockSize)) + (uint32)(32))
	sumT := uint64(0)
	for _, ti := range config.Storages {
		sumT += ti.StorageVolume
	}

	if config.TotalVolumn > maxDiskCapability || config.TotalVolumn < sumT {
		return nil, ErrConfigC
	}

	// calc M, N, D
	c, d, n, m := config.TotalVolumn, (uint64)(config.DataBlockSize), (uint64)(config.IndexTableRows), (uint64)(config.IndexTableCols)
	m = c / (n * d)
	if m < 4 || m >= MaxRangeCoverage {
		return nil, ErrConfigM
	}
	config.IndexTableCols = uint32((float64)(m) * expendRatioM)

	if config.IndexTableRows > MaxRangeNumber || !ytfs.IsPowerOfTwo((uint64)(config.IndexTableRows)) {
		return nil, ErrConfigN
	}

	if !ytfs.IsPowerOfTwo((uint64)(config.SyncPeriod)) {
		return nil, ErrConfigSyncPeriod
	}

	// check if YTFS param consistency with YTFS storage.
	for _, storageOpt := range config.Storages {
		if (storageOpt.DataBlockSize != config.DataBlockSize) || !ytfs.IsPowerOfTwo((uint64)(config.DataBlockSize)) {
			return nil, ErrConfigD
		}

		if !ytfs.IsPowerOfTwo((uint64)(storageOpt.SyncPeriod)) {
			return nil, ErrConfigSyncPeriod
		}
	}

	// TODO: return new object.
	return config, nil
}
