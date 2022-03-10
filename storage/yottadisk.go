package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"unsafe"

	// use eth hash related func.
	// "github.com/ethereum/go-ethereum/common"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
)

// YottaDisk main entry of YTFS storage
type YottaDisk struct {
	config *opt.StorageOptions
	meta   ydcommon.StorageHeader
	store  Storage
	stat   diskStatistics
	sync.Mutex
}

type diskStatistics struct {
	writeOps uint32
}

// Capability reports the YottaDisk's capability of datablocks.
func (disk *YottaDisk) Capability() uint32 {
	return disk.meta.DataCapacity
}

// Format formats the YottaDisk and reset header.
func (disk *YottaDisk) Format() error {
	disk.meta.Tag = [4]byte{0, 0, 0, 0}
	return disk.Sync()
}

// Sync syncs all pending meta and unflushed writes
func (disk *YottaDisk) Sync() error {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	writer, err := disk.store.Writer()
	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &disk.meta)
	if err != nil {
		return err
	}

	writer.Sync()
	return nil
}

// Close closes the YottaDisk.
func (disk *YottaDisk) Close() error {
	_ = disk.Sync()
	_ = disk.store.Close()
	return nil
}

// ReadData reads data from low level storage
func (disk *YottaDisk) ReadData(dataIndex ydcommon.IndexTableValue) ([]byte, error) {
	//todo use read lock
	//locker, _ := disk.store.Lock()
	//	//defer locker.Unlock()

	//this lock shouldn't need
	//locker, _ := disk.store.RLock()
	//defer locker.Unlock()

	index := disk.store.ReaderIndex()
	defer disk.store.ReaderIndexClose(index)

	reader, err := disk.store.Reader(index)
	dataBlock := make([]byte, disk.meta.DataBlockSize, disk.meta.DataBlockSize)
	reader.Seek(int64(disk.meta.DataOffset)+int64(disk.meta.DataBlockSize)*int64(dataIndex), io.SeekStart)
	err = binary.Read(reader, binary.LittleEndian, dataBlock)
	//_, err = reader.Read(dataBlock)
	if err != nil {
		return nil, err
	}
	return dataBlock, nil
}

// WriteData writes data to low level storage
func (disk *YottaDisk) WriteData(dataOffsetIndex ydcommon.IndexTableValue, data []byte) error {
	if uint32(dataOffsetIndex) >= disk.meta.DataCapacity {
		fmt.Println("[memtrace] WriteData error dataOffsetIndex out datacapacity")
		return errors.ErrDataOverflow
	}

	//todo use write lock
	//locker, _ := disk.store.RLock()
	//defer locker.Unlock()
	locker, _ := disk.store.WLock()
	defer locker.Unlock()

	writer, err := disk.store.Writer()
	//assert?????
	//ydcommon.YottaAssert(len(data)%(int)(disk.meta.DataBlockSize) == 0)
	if len(data)%(int)(disk.meta.DataBlockSize) != 0 {
		return errors.ErrDataIllegal
	}
	dataBlock := make([]byte, len(data), len(data))
	copy(dataBlock, data)
	writer.Seek(int64(disk.meta.DataOffset)+int64(disk.meta.DataBlockSize)*int64(dataOffsetIndex), io.SeekStart)

	//
	//block := dio.AlignedBlock(dio.BlockSize)
	//_, err = io.ReadFull(bytes.NewReader(dataBlock), block)
	//if err != nil {
	//	return err
	//}

	_, err = writer.Write(dataBlock)
	if err != nil {
		fmt.Println("[memtrace] real write error:", err)
		return err
	}

	disk.stat.writeOps++

	if disk.stat.writeOps&(disk.config.SyncPeriod-1) == 0 {
		return writer.Sync()
	}

	return nil
}

// OpenYottaDisk opens or creates a YottaDisk for the given storage.
// The DB will be created if not exist, unless Error happens.
//
// OpenYottaDisk will return ErrConfigXXX if config is incorrect.
//
// The returned YottaDisk instance is safe for concurrent use.
// The YottaDisk must be closed after use, by calling Close method.
//
func OpenYottaDisk(yottaConfig *opt.StorageOptions, init bool) (*YottaDisk, error) {
	storage, err := openStorage(yottaConfig)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(storage)
	if err != nil || init {
		if init || opt.IgnoreStorageHeaderErr {
			header, err = initializeStorage(storage, yottaConfig)
			if err != nil {
				fmt.Println("initialize storage header err", err.Error())
				return nil, err
			}
		} else {
			fmt.Println("read storage header err:", err.Error())
			return nil, err
		}
	}

	if !validateHeader(header, yottaConfig) {
		if opt.IgnoreStorageHeaderErr {
			header, err = initializeStorage(storage, yottaConfig)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.ErrStorageHeader
		}
	}

	yd := &YottaDisk{
		yottaConfig,
		*header,
		storage,
		diskStatistics{0},
		sync.Mutex{},
	}

	fmt.Println("Open YottaDisk success @" + yottaConfig.StorageName)
	return yd, nil
}

func openStorage(storageConfig *opt.StorageOptions) (Storage, error) {
	var storage Storage
	var err error
	switch storageConfig.StorageType {
	case ydcommon.FileStorageType:
		storage, err = OpenFileStorage(storageConfig)
	case ydcommon.BlockStorageType:
		storage, err = OpenBlockStorage(storageConfig)
	default:
		err = errors.ErrStorageType
	}

	if err != nil {
		return nil, err
	}

	return storage, nil
}

func validateHeader(header *ydcommon.StorageHeader, yottaConfig *opt.StorageOptions) bool {
	if header == nil {
		fmt.Println("validateHeader header is nil")
		return false
	}

	if yottaConfig == nil {
		fmt.Println("validateHeader yottaConfig is nil")
		return false
	}

	return header.DataBlockSize == yottaConfig.DataBlockSize && header.DiskCapacity == yottaConfig.StorageVolume
}

func initializeStorage(store Storage, config *opt.StorageOptions) (*ydcommon.StorageHeader, error) {
	writer, err := store.Writer()
	if err != nil {
		return nil, err
	}

	t, d, h := config.StorageVolume, (uint64)(config.DataBlockSize), (uint64)(unsafe.Sizeof(ydcommon.Header{}))
	// in case data overflows.
	ydcommon.YottaAssertMsg(t > h+d, "t should > h + d")

	// write header.
	dataOffset := uint32(h)
	header := ydcommon.StorageHeader{
		Tag:           [4]byte{'S', 'T', 'O', 'R'},
		Version:       [4]byte{0x0, '.', 0x0, 0x1},
		DiskCapacity:  t,
		DataBlockSize: uint32(d),
		DataOffset:    dataOffset,
		DataCapacity:  uint32((t - h) / d),
		Reserved:      uint32((t - h) % d), // left-overs
		DataNodeId:    0xFFFFFFFF,
	}

	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	writer.Sync()
	return &header, nil
}

func readHeader(store Storage) (*ydcommon.StorageHeader, error) {
	index := store.ReaderIndex()
	defer store.ReaderIndexClose(index)

	reader, err := store.Reader(index)
	if err != nil {
		return nil, err
	}

	header := ydcommon.StorageHeader{}
	reader.Seek(0, io.SeekStart)

	buf := make([]byte, unsafe.Sizeof(header), unsafe.Sizeof(header))
	n, err := reader.Read(buf)
	if (err != nil) || (n != (int)(unsafe.Sizeof(header))) {
		return nil, err
	}
	bufReader := bytes.NewBuffer(buf)
	err = binary.Read(bufReader, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	if header.Tag[0] != 'S' {
		return nil, errors.ErrHeadNotFound
	}

	return &header, nil
}

func (disk *YottaDisk) GetStorageHeader() ydcommon.StorageHeader {
	return disk.meta
}

func (disk *YottaDisk) GetStorage() Storage {
	return disk.store
}

func (disk *YottaDisk) SetDnIdToStore(Bdn []byte) error {
	writer, _ := disk.store.Writer()
	header := disk.meta
	writer.Seek(int64(unsafe.Offsetof(header.DataNodeId)), io.SeekStart)
	_, err := writer.Write(Bdn)
	return err
}

func (disk *YottaDisk) SetVersionToStore(Bvs []byte) error {
	writer, _ := disk.store.Writer()
	header := disk.meta
	writer.Seek(int64(unsafe.Offsetof(header.Version)), io.SeekStart)
	_, err := writer.Write(Bvs)
	return err
}

func (disk *YottaDisk) GetDnIdFromStore() uint32 {
	index := disk.store.ReaderIndex()
	defer disk.store.ReaderIndexClose(index)

	reader, _ := disk.store.Reader(index)
	header := disk.meta
	reader.Seek(int64(unsafe.Offsetof(header.DataNodeId)), io.SeekStart)
	Bdn := make([]byte, 4)
	_, err := reader.Read(Bdn)
	if err != nil {
		fmt.Println("GetDnIdFromStore error:", err.Error())
		return 0
	}

	dnid := binary.LittleEndian.Uint32(Bdn)
	return dnid
}
