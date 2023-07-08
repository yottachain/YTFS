package ytfs

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/getresource"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
	"unsafe"
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
	RealDiskCap uint32
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
func NewContext(dir string, config *opt.Options, dataCount uint64, init bool, dnId uint32) (*Context, error) {
	storages, err := initStorages(config, init, dnId)
	if err != nil {
		return nil, err
	}

	if init {
		dataCount = 0
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

	fmt.Println("[error] Create new YTFS content error:", err, "current sp = ", context.sp)

	//maybe err happens, here we still need start storage for read
	err = nil
	return context, err
}

//	func GetRealDiskCap(path string) uint64 {
//		return getresource.GetDiskCap(path)
//	}
func GetRealDiskCap(stor storage.Storage) uint64 {
	return getresource.GetDiskCap(stor)
}

func initStorages(config *opt.Options, init bool, dnId uint32) ([]*storageContext, error) {
	diskCount := len(config.Storages)
	contexts := make([]*storageContext, diskCount, diskCount)
	for idx, storageOpt := range config.Storages {
		fmt.Printf("storage index %d, storage name %s\n", idx, storageOpt.StorageName)
		disk, err := storage.OpenYottaDisk(&storageOpt, init, idx, dnId)
		if err != nil {
			// TODO: handle error if necessary, like keep using successed storages.
			return nil, err
		}

		RealCap := uint64(0)
		if runtime.GOOS == "linux" {
			header := ydcommon.StorageHeader{}
			var size uint64
			if disk.GetStorage().GetFd().Type == ydcommon.BlockStorageType {
				size = GetRealDiskCap(disk.GetStorage())
			} else {
				size = disk.GetStorageHeader().DiskCapacity
			}

			if size == 0 {
				RealCap = 0
			} else {
				RealCap = size - (uint64)(unsafe.Sizeof(header))
				RealCap = RealCap / 16384
			}
		}

		version := disk.GetStorageHeader().Version
		Version001 := [4]byte{0x0, '.', 0x0, 0x1}
		Version002 := [4]byte{0x0, '.', 0x0, 0x2}
		if version == Version001 ||
			string(version[:]) == StoreVersion001 ||
			version == Version002 ||
			string(version[:]) == StoreVersion002 {
			contexts = append(contexts, &storageContext{
				Name:        storageOpt.StorageName,
				Cap:         disk.Capability(),
				Len:         0,
				Disk:        disk,
				RealDiskCap: uint32(RealCap),
			})
		} else {
			realIdx := disk.Index() & storage.DiskIdxMax
			if (disk.Index()&storage.DiskIdxPre == storage.DiskIdxPre) &&
				(realIdx < uint16(diskCount)) {
				contexts[realIdx] = &storageContext{
					Name:        storageOpt.StorageName,
					Cap:         disk.Capability(),
					Len:         0,
					Disk:        disk,
					RealDiskCap: uint32(RealCap),
				}
				fmt.Printf("storage origin index %d, storage name %s\n", idx, storageOpt.StorageName)
			} else {
				return nil, fmt.Errorf("stroage %s, %s",
					storageOpt.StorageName, errors.ErrStorageSerialNumber.Error())
			}

		}
	}

	return contexts, nil
}

//func (c *Context)GetStorageHead(){
//	storage.GetStorageHeader(c.storages[0].Disk)
//
//	return
//}

// SetStoragePointer set the storage pointer position of current storage context
func (c *Context) SetStoragePointer(globalID uint32) error {
	sp, err := c.locate(globalID)
	if sp != nil {
		c.sp = sp
	}
	return err
}

func (c *Context) GetStorageContext() []*storageContext {
	return c.storages
}

func (c *Context) GetAvailablePos(data []byte, writeEndPos uint32) uint32 {
	sp := c.sp
	if sp.index >= writeEndPos {
		return writeEndPos
	}

	if writeEndPos-sp.index < 1024 {
		return writeEndPos
	}

	srcKey := md5.Sum(data)

	//从当前写入位置偏离1024个位置
	var startPos = sp.index + 1024
	var writeAblePos = writeEndPos
	var lastFailPos uint32 = 0
	var lastSucPos uint32 = 0

	for startPos != writeAblePos {
		fmt.Printf("[cap proof] sp.index %d, startWritePos %d, write pos %d\n",
			sp.index, startPos, writeAblePos)
		_, err := c.PutAt(data, writeAblePos)
		if err == nil {
			resdata, err := c.Get(ydcommon.IndexTableValue(writeAblePos))
			if err != nil {
				fmt.Printf("[cap proof] error, date get error, cur data pos %d, get err pos %d\n",
					sp.index, writeAblePos)
				lastFailPos = writeAblePos
				goto con
			}
			resKey := md5.Sum(resdata)
			if !bytes.Equal(srcKey[:], resKey[:]) {
				fmt.Printf("[cap proof] error, data check error, cur data pos %d, check err pos %d\n",
					sp.index, writeAblePos)
				lastFailPos = writeAblePos
				goto con
			}
			lastSucPos = writeAblePos
			if lastSucPos < lastFailPos {
				startPos = lastSucPos
				writeAblePos = lastFailPos
				goto con
			}
			fmt.Printf("[cap proof] success, data write success, cur data pos %d, write suc pos %d\n",
				sp.index, writeAblePos)
			break
		} else {
			lastFailPos = writeAblePos
			fmt.Printf("[cap proof] error:%s, data write error, cur data pos %d, write err pos %d\n",
				err.Error(), sp.index, writeAblePos)
		}
	con:
		writeAblePos = startPos + (writeAblePos-startPos)/2
	}

	return writeAblePos
}

func (c *Context) RandCheckAvailablePos(data []byte, randTimes int, EndPos uint32) uint32 {
	sp := c.sp
	if sp.index >= EndPos {
		return EndPos
	}

	if EndPos-sp.index < 1024 {
		return EndPos
	}

	status := true
	minFailPos := EndPos
	AvaliablePos := EndPos

	var startPos = sp.index + 1024
	var scope = EndPos - startPos
	if scope <= 0 {
		return AvaliablePos
	}

	srcKey := md5.Sum(data)
	for i := 0; i < randTimes; i++ {
		randPos := rand.Int63n(int64(scope))
		writePos := startPos + uint32(randPos)
		_, err := c.PutAt(data, writePos)
		if err == nil {
			resdata, err := c.Get(ydcommon.IndexTableValue(writePos))
			if err != nil {
				fmt.Printf("[cap proof rand RW] error, get data error, cur data pos %d, get err pos %d\n",
					sp.index, writePos)
				status = false
				goto con
			}
			resKey := md5.Sum(resdata)
			if !bytes.Equal(srcKey[:], resKey[:]) {
				fmt.Printf("[cap proof rand RW] error, data check error, cur data pos %d, write err pos %d\n",
					sp.index, writePos)
				status = false
				goto con
			}
			fmt.Printf("[cap proof rand RW], data check success, cur data pos %d, write success pos %d\n",
				sp.index, writePos)
			continue
		} else {
			fmt.Printf("[cap proof rand RW] error, write data error, cur data pos %d, write err pos %d\n",
				sp.index, writePos)
			status = false
		}
	con:
		if !status {
			if uint32(randPos) < minFailPos {
				minFailPos = uint32(randPos)
			}
		}
	}

	if !status {
		AvaliablePos = c.GetAvailablePos(data, minFailPos)
	} else {
		fmt.Printf("[cap proof rand RW] all success\n")
	}

	return AvaliablePos
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

func (c *Context) fastforward(n int, commit bool) error {
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
		fmt.Println("[memtrace] in fastforward error:", err)
		return err
	}
	return nil
}

func (c *Context) save() *storagePointer {
	saveSP := *c.sp
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
	//todo Whether can be unlocked?
	//c.lock.RLock()
	//defer c.lock.RUnlock()

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

//func (c *Context) PutAtPos(value []byte, pos uint32)(uint32, error){
//	index, err := c.putAt(value, pos)
//	if err != nil {
//		return index, err
//	}
//	return index, nil
//}

// PutAt puts the vale to specific offset of the correct device
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
	if c.sp.posIdx+uint32(cnt) <= c.storages[c.sp.dev].Cap {
		index, err = c.putAt(valueArray, c.sp)
		if err != nil {
			return 0, err
		}
	} else {
		currentSP := *c.sp
		step1 := c.storages[currentSP.dev].Cap - currentSP.posIdx
		index, err = c.putAt(valueArray[:step1*c.config.DataBlockSize], &currentSP)
		if err != nil {
			return 0, err
		}
		step2 := uint32(cnt) - step1
		currentSP.dev++
		currentSP.posIdx = 0
		currentSP.index += step1
		if currentSP.posIdx+step2 > c.storages[currentSP.dev].Cap {
			return 0, errors.New("Batch across 3 storage devices, not supported")
		}
		_, err = c.putAt(valueArray[step1*c.config.DataBlockSize:], &currentSP)
		if err != nil {
			return 0, err
		}
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

func (c *Context) GetStorageHeader() ydcommon.StorageHeader {
	return c.storages[0].Disk.GetStorageHeader()
}

func (c *Context) SetDnIdToStor(dnid uint32) error {
	var err error
	Bdn := make([]byte, 4)
	binary.LittleEndian.PutUint32(Bdn, dnid)
	err = c.storages[0].Disk.SetDnIdToStore(Bdn)
	return err
}

func (c *Context) SetVersionToStor(vs [4]byte) error {
	var err error
	err = c.storages[0].Disk.SetVersionToStore(vs[:])
	return err
}

func (c *Context) GetDnIdFromStor() uint32 {
	return c.storages[0].Disk.GetDnIdFromStore()
}

func (c *Context) CheckStorageDnid(dnid uint32) (bool, error) {
	var err error
	var StorDn uint32
	header := c.GetStorageHeader()
	version := header.Version
	fmt.Println("version=", string(version[:]))
	OldVersion := [4]byte{0x0, '.', 0x0, 0x1}
	if version == OldVersion || string(version[:]) == StoreVersion001 {
		err = c.SetDnIdToStor(dnid)
		if err != nil {
			fmt.Println("SetDnIdToIdxDB error:", err.Error())
			return false, err
		}
		_ = c.SetVersionToStor(StoreVersion003)
	} else {
		if version == StoreVersion003 {
			for idx, storage := range c.GetStorageContext() {
				StorDn = storage.Disk.GetDnIdFromStore()
				if StorDn != dnid {
					fmt.Printf("error: dnid not equal,storage idx %d storage name %s storage dnid %d, cfg dnid %d\n",
						idx, storage.Name, StorDn, dnid)
					err = fmt.Errorf("error: dnid not equal,storage idx %d storage name %s storage dnid %d, cfg dnid %d\n",
						idx, storage.Name, StorDn, dnid)
					return false, err
				}
			}
		} else {
			StorDn = c.GetDnIdFromStor()
			if StorDn != dnid {
				fmt.Println("error: dnid not equal,stor=", StorDn, " cfg=", dnid)
				err = fmt.Errorf("dnid not equal,stor=", StorDn, " cfg=", dnid)
				return false, err
			}
		}
	}
	fmt.Println("CheckStorageDnid, stor=", StorDn, " cfg=", dnid)
	return true, nil
}
