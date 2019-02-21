package ytfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

// YTFS is a data block save/load lib based on key-value styled db APIs.
type YTFS struct {
	// key-value db which saves hash <-> position
	db *IndexDB
	// running context
	context *Context
}

// Open opens or creates a YTFS for the given storage.
// The YTFS will be created if not exist.
//
// The returned YTFS instance is safe for concurrent use.
// The YTFS must be closed after use, by calling Close method.
// Usage Sample, ref to playground.go:
//		...
//		config := opt.DefaultOptions()
//
//		ytfs, err := ytfs.Open(path, config)
//		if err != nil {
//			panic(err)
//		}
//		defer ytfs.Close()
//		err = ytfs.Put(ydcommon.IndexTableKey, ydcommon.IndexTableValue)
///		if err != nil {
//			panic(err)
//		}
//
//		ydcommon.IndexTableValue, err = ytfs.Gut(ydcommon.IndexTableKey)
///		if err != nil {
//			panic(err)
//		}
//		...
func Open(dir string, config *opt.Options) (ytfs *YTFS, err error) {
	settings, err := opt.FinalizeConfig(config)
	return openYTFS(dir, settings)
}

func openYTFS(dir string, config *opt.Options) (*YTFS, error) {
	//TODO: file lock to avoid re-open.
	//1. open system dir for YTFS
	if fi, err := os.Stat(dir); err == nil {
		// dir/file exists, check if it can be reloaded.
		if !fi.IsDir() {
			return nil, ErrDirNameConflict
		}
		err := openYTFSDir(dir, config)
		if err != nil && err != ErrEmptyYTFSDir {
			return nil, err
		}
	} else {
		// create new dir
		if err := os.MkdirAll(dir, os.ModeDir); err != nil {
			return nil, err
		}
	}

	// initial a new ytfs.
	// save config
	configName := path.Join(dir, "config.json")
	err := opt.SaveConfig(config, configName)
	if err != nil {
		return nil, err
	}

	// open index db
	indexDB, err := NewIndexDB(dir, config)
	if err != nil {
		return nil, err
	}

	//3. open storages
	context, err := NewContext(dir, config)
	if err != nil {
		return nil, err
	}

	ytfs := &YTFS{
		db:      indexDB,
		context: context,
	}

	fmt.Println("Open YTFS Success @" + dir)
	return ytfs, nil
}

func openYTFSDir(dir string, config *opt.Options) error {
	configPath := path.Join(dir, "config.json")
	if _, err := os.Stat(configPath); err == nil {
		// TODO: recover data and check config consistency with input.
		oldConfig, err := opt.ParseConfig(configPath)
		if err != nil {
			return err
		}

		if !oldConfig.Equal(config) {
			return ErrSettingMismatch
		}

		return nil
	}

	return ErrEmptyYTFSDir
}

func validateYTFSSchema(meta *ydcommon.Header, opt *opt.Options) (*ydcommon.Header, *opt.Options, error) {
	if meta.YtfsCapability != opt.TotalVolumn || meta.DataBlockSize != opt.DataBlockSize {
		return nil, nil, ErrConfigIndexMismatch
	}
	return meta, opt, nil
}

// Get gets the value for the given key. It returns ErrNotFound if the
// DB does not contains the key.
//
// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after Get returns.
func (ytfs *YTFS) Get(key ydcommon.IndexTableKey) ([]byte, error) {
	pos, err := ytfs.db.Get(key)
	if err != nil {
		return nil, err
	}

	return ytfs.context.Get(pos)
}

// Put sets the value for the given key. It panic if there exists any previous value
// for that key; YottaDisk is not a multi-map.
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (ytfs *YTFS) Put(key ydcommon.IndexTableKey, buf []byte) error {
	pos, err := ytfs.context.Put(buf)
	if err != nil {
		return err
	}

	return ytfs.db.Put(key, ydcommon.IndexTableValue(pos))
}

// Meta reports current meta information.
func (ytfs *YTFS) Meta() *ydcommon.Header {
	return ytfs.db.schema
}

// Close closes the YTFS.
//
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (ytfs *YTFS) Close() {
	ytfs.db.Close()
	ytfs.context.Close()
}

// Reset resets an existed YottaDisk, and make it ready
// for next put/get operation. so far we do quick format which just
// erases the header.
func (ytfs *YTFS) Reset() error {
	ytfs.db.Reset()
	ytfs.context.Reset()
	return nil
}

// Cap report capacity of YTFS, just like cap() of a slice
func (ytfs *YTFS) Cap() uint64 {
	dataCaps := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		dataCaps += uint64(stroageCtx.Cap)
	}
	return dataCaps
}

// Len report len of YTFS, just like len() of a slice
func (ytfs *YTFS) Len() uint64 {
	dataLen := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		dataLen += uint64(stroageCtx.Len)
	}
	return dataLen
}

// String reports current YTFS status.
func (ytfs *YTFS) String() string {
	meta, _ := json.MarshalIndent(ytfs.db.schema, "", "	")
	// min := (int64)(math.MaxInt64)
	// max := (int64)(math.MinInt64)
	// sum := (int64)(0)
	// table := fmt.Sprintf("Total table Count: %d\n"+
	// 	"Total saved items: %d\n"+
	// 	"Maximum table size: %d\n"+
	// 	"Minimum table size: %d\n"+
	// 	"Average table size: %d\n", len(disk.index.sizes), sum, max, min, avg)
	return string(meta) + "\n"
}
