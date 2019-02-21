package ytfs

import (
	"fmt"
	// "math"
	"sync"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
)

const (
	debugPrint bool = false
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
	sp       storagePointer
	storages []*storageContext
	// cm     		*cache.Manager
	lock sync.RWMutex
}

// NewContext creates a new YTFS context
func NewContext(dir string, config *opt.Options) (*Context, error) {
	storages, sp, err := initStorages(config)
	if err != nil {
		return nil, err
	}

	return &Context{
		config:   config,
		sp:       *sp,
		storages: storages,
		lock:     sync.RWMutex{},
	}, nil
}

func initStorages(config *opt.Options) ([]*storageContext, *storagePointer, error) {
	contexts := []*storageContext{}
	sp := &storagePointer{0, 0, 0}
	moveToNextDev := true
	for devID, storageOpt := range config.Storages {
		disk, err := storage.OpenYottaDisk(&storageOpt)
		if err != nil {
			// TODO: handle error if necessary, like keep using successed storages.
			return nil, nil, err
		}
		contexts = append(contexts, &storageContext{
			Name: storageOpt.StorageName,
			Cap:  disk.Capability(),
			Len:  disk.Stat(),
			Disk: disk,
		})

		if moveToNextDev {
			sp.dev = uint8(devID)
			sp.posIdx = disk.Stat()
			sp.index += disk.Stat()
			moveToNextDev = (sp.posIdx == disk.Capability())
		}
	}

	fmt.Println("Finish init context, current sp = ", sp)
	return contexts, sp, nil
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

	return nil, errors.ErrContextIdMapping
}

func (c *Context) forward() error {
	sp := &c.sp
	sp.posIdx++
	if sp.posIdx == c.storages[sp.dev].Cap {
		fmt.Println("Move to next dev", sp.dev+1)
		if int(sp.dev) == len(c.storages) {
			return ErrDataOverflow
		}
		sp.dev++
		sp.posIdx = 0
	}

	sp.index++
	return nil
}

func (c *Context) eof() bool {
	sp := &c.sp
	return sp.dev == uint8(len(c.storages)-1) && sp.posIdx == c.storages[sp.dev].Cap
}

func (c *Context) setEof() {
	sp := &c.sp
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

// Put puts the vale to offset of the corrent device
func (c *Context) Put(value []byte) (uint32, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.eof() {
		return 0, ErrDataOverflow
	}

	if debugPrint {
		fmt.Printf("put data %v @ %v\n", value[:32], c.sp)
	}

	dataPos := c.sp.posIdx
	err := c.storages[c.sp.dev].Disk.WriteData(ydcommon.IndexTableValue(dataPos), value)
	if err != nil {
		return c.sp.index, err
	}
	index := c.sp.index
	c.forward()
	return index, nil
}

// Close finishes all actions and close all storages
func (c *Context) Close() {
	for _, storage := range c.storages {
		storage.Disk.Close()
		c.setEof()
	}
}

// Reset reset current context.
func (c *Context) Reset() {
	c.sp = storagePointer{0, 0, 0}
}
