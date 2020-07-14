package ytfs

import (
	"fmt"
	"runtime"
	"math"
	"sync"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
	"unsafe"
	"github.com/yottachain/YTFS/getresource"
)

var (
	debugPrint = opt.DebugPrint
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
	// Storage real Capacity
	RealDiskCap  uint32
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

	err = context.SetStoragePointer(uint32(dataCount))
	if err == nil {
		fmt.Println("Create YTFS content success, current sp = ", context.sp)
		return context, nil
	}

	fmt.Println("[error]Create new YTFS content error:",err,"current sp = ", context.sp)
	//maybe err happens, here we still need start storage for read
	err = nil
	return context, err
}

func GetRealDiskCap(path string)uint64{
	  return getresource.GetDiskCap(path)
}

func initStorages(config *opt.Options) ([]*storageContext, error) {
	contexts := []*storageContext{}
	for _, storageOpt := range config.Storages {
		disk, err := storage.OpenYottaDisk(&storageOpt)
		if err != nil {
			// TODO: handle error if necessary, like keep using successed storages.
			return nil, err
		}

		RealCap :=uint64(0)
		if runtime.GOOS == "linux"{
			header := ydcommon.StorageHeader{}
			RealCap = GetRealDiskCap(storageOpt.StorageName)-(uint64)(unsafe.Sizeof(header))
			RealCap = (RealCap/16384)
		}

		contexts = append(contexts, &storageContext{
			Name: storageOpt.StorageName,
			Cap:  disk.Capability(),
			Len:  0,
			Disk: disk,
			RealDiskCap: uint32(RealCap),
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
	if int(sp.dev) >= len(c.storages) {
		fmt.Println("[memtrace] error int(sp.dev) >= len(c.storages)")
		return errors.ErrDataOverflow
	}
	if sp.posIdx >= c.storages[sp.dev].Cap {
		fmt.Println("[memtrace] sp.posIdx >= c.storages[sp.dev].Cap")
		if debugPrint {
			fmt.Println("Move to next dev", sp.dev+1)
		}
		sp.dev++
		sp.posIdx = 0
	}

	sp.index++
	return nil
}

func (c *Context)  fastforward(n int, commit bool) error {
	sp := *c.sp
	var err error
	i := 0
	for i = 0; i < n && err == nil; i++ {
			err = c.forward()
	}
	if !commit {
			*c.sp = sp
	}

	if i <= n && err != nil {
			// last i reach the eof is ok.
		fmt.Println("[memtrace] in fastforward error:",err)
		return err
	}
	return nil
}

func (c *Context) save() *storagePointer {
	saveSP := *c.sp;
	return &saveSP
}

func (c *Context) restore(sp *storagePointer) {
	*c.sp = *sp
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

// BatchPut puts the value array to offset that current sp points to of the corrent device
func (c *Context) BatchPut(cnt int, valueArray []byte) (uint32, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// TODO: Can we leave this check to disk??
	if err := c.fastforward(cnt, false); err != nil {
		return 0, err
	}

	var err error
	var index uint32
	if (c.sp.posIdx + uint32(cnt) <= c.storages[c.sp.dev].Cap) {
		index, err = c.putAt(valueArray, c.sp)
	} else {
		currentSP := *c.sp;
		step1 := c.storages[currentSP.dev].Cap - currentSP.posIdx
		index, err = c.putAt(valueArray[:step1*c.config.DataBlockSize], &currentSP)
		step2 := uint32(cnt) - step1
		currentSP.dev++
		currentSP.posIdx = 0
		currentSP.index += step1
		if (currentSP.posIdx + uint32(step2) > c.storages[currentSP.dev].Cap) {
				return 0, errors.New("Batch across 3 storage devices, not supported")
		}
		_, err = c.putAt(valueArray[step1*c.config.DataBlockSize:], &currentSP)
	}

	if err != nil {
		return 0, err
	}
	c.fastforward(cnt, true)
	return index, nil
}

func (c *Context) putAt(value []byte, sp *storagePointer) (uint32, error) {
	if c.eof() {
		return 0, errors.ErrDataOverflow
	}
	if debugPrint {
		fmt.Printf("put data %x @ %v\n", value[:32], sp)
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
