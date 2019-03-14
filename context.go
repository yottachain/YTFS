package ytfs

import (
	"fmt"
	"math"
	"sync"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
)

const (
	debugPrint bool = opt.DebugPrint
)

type storageContext struct {
	// storage name
	Name string
	// full capability of data block
	Cap uint32
	// used data block slot number
	Len uint32
	// Storage
	Disk *storage.YottaDisk
}

type storagePointer struct {
	dev    uint8  // device id, from 0~255
	posIdx uint32 // device inside offset id.
	index  uint32 // global id of data. if one device can hold 1 data, then 0 == [0, 0], 1 == [1, 0], 2 == [2, 0]
}

// Context - the running YTFS context
type Context struct {
	config   *opt.Options
	sp       *storagePointer
	storages []*storageContext
	// cm     		*cache.Manager
	lock sync.RWMutex
}

// NewContext creates a new YTFS context
func NewContext(dir string, config *opt.Options, dataCount uint64) (*Context, error) {
	storages, err := initStorages(config)
	if err != nil {
		return nil, err
	}

	if dataCount > math.MaxUint32 {
		return nil, errors.ErrContextOverflow
	}

	context := &Context{
		config:   config,
		sp:       nil,
		storages: storages,
		lock:     sync.RWMutex{},
	}

	context.SetStoragePointer(uint32(dataCount))
	fmt.Println("Create YTFS content success, current sp = ", context.sp)
	return context, nil
}

func initStorages(config *opt.Options) ([]*storageContext, error) {
	contexts := []*storageContext{}
	for _, storageOpt := range config.Storages {
		disk, err := storage.OpenYottaDisk(&storageOpt)
		if err != nil {
			// TODO: handle error if necessary, like keep using successed storages.
			return nil, err
		}
		contexts = append(contexts, &storageContext{
			Name: storageOpt.StorageName,
			Cap:  disk.Capability(),
			Len:  0,
			Disk: disk,
		})
	}

	return contexts, nil
}

// SetStoragePointer set the storage pointer position of current storage context
func (c *Context) SetStoragePointer(globalID uint32) error {
	sp, err := c.locate(globalID)
	if sp != nil {
		c.sp = sp
	}
	return err
}

// Locate find the correct offset in correct device
func (c *Context) locate(idx uint32) (*storagePointer, error) {
	// TODO: binary search
	var dev, posIdx uint32 = 0, 0
	var devBegin, devEnd uint32 = 0, 0
	for _, s := range c.storages {
		devEnd += s.Cap
		if devBegin <= idx && idx < devEnd {
			posIdx = idx - devBegin
			return &storagePointer{
				uint8(dev),
				posIdx,
				idx,
			}, nil
		}
		devBegin += s.Cap
		dev++
	}

	return &storagePointer{
		uint8(len(c.storages)),
		0,
		0,
	}, errors.ErrContextIDMapping
}

func (c *Context) forward() error {
	sp := c.sp
	sp.posIdx++
	if sp.posIdx == c.storages[sp.dev].Cap {
		if int(sp.dev+1) == len(c.storages) {
			return errors.ErrDataOverflow
		}
		if debugPrint {
			fmt.Println("Move to next dev", sp.dev+1)
		}
		sp.dev++
		sp.posIdx = 0
	}

	sp.index++
	return nil
}

func (c *Context) eof() bool {
	sp := c.sp
	return sp.dev >= uint8(len(c.storages)) || (sp.dev == uint8(len(c.storages)-1) && sp.posIdx == c.storages[sp.dev].Cap)
}

func (c *Context) setEOF() {
	sp := c.sp
	sp.dev = uint8(len(c.storages) - 1)
	sp.posIdx = c.storages[sp.dev].Cap
}

// Get gets the value from offset of the correct device
func (c *Context) Get(globalIdx ydcommon.IndexTableValue) (value []byte, err error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	sp, err := c.locate(uint32(globalIdx))
	if err != nil {
		return nil, err
	}

	if debugPrint {
		fmt.Printf("get data globalId %d @%v\n", globalIdx, sp)
	}

	return c.storages[sp.dev].Disk.ReadData(ydcommon.IndexTableValue(sp.posIdx))
}

// Put puts the vale to offset that current sp points to of the corrent device
func (c *Context) Put(value []byte) (uint32, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	index, err := c.putAt(value, c.sp)
	if err != nil {
		return index, err
	}
	c.forward()
	return index, nil
}

// PutAt puts the vale to specific offset of the corrent device
func (c *Context) PutAt(value []byte, globalID uint32) (uint32, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	sp, err := c.locate(globalID)
	if err != nil {
		return 0, err
	}
	index, err := c.putAt(value, sp)
	if err != nil {
		return index, err
	}
	return index, nil
}

func (c *Context) putAt(value []byte, sp *storagePointer) (uint32, error) {
	if c.eof() {
		return 0, errors.ErrDataOverflow
	}

	if debugPrint {
		fmt.Printf("put data %v @ %v\n", value[:32], sp)
	}

	dataPos := sp.posIdx
	err := c.storages[sp.dev].Disk.WriteData(ydcommon.IndexTableValue(dataPos), value)
	if err != nil {
		return sp.index, err
	}
	return sp.index, nil
}

// Close finishes all actions and close all storages
func (c *Context) Close() {
	for _, storage := range c.storages {
		storage.Disk.Close()
		c.setEOF()
	}
}

// Reset reset current context.
func (c *Context) Reset() {
	c.sp = &storagePointer{0, 0, 0}
	for _, storage := range c.storages {
		storage.Disk.Format()
	}
}
