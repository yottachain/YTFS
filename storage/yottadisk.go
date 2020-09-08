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
	disk.Sync()
	disk.store.Close()
	return nil
}

// ReadData reads data from low level storage
func (disk *YottaDisk) ReadData(dataIndex ydcommon.IndexTableValue) ([]byte, error) {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	reader, err := disk.store.Reader()
	dataBlock := make([]byte, disk.meta.DataBlockSize, disk.meta.DataBlockSize)
	reader.Seek(int64(disk.meta.DataOffset)+int64(disk.meta.DataBlockSize)*int64(dataIndex), io.SeekStart)
	_, err = reader.Read(dataBlock)
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

	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	writer, err := disk.store.Writer()
	ydcommon.YottaAssert(len(data) % (int)(disk.meta.DataBlockSize) == 0)
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
		fmt.Println("[memtrace] real write error:",err)
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
func OpenYottaDisk(yottaConfig *opt.StorageOptions) (*YottaDisk, error) {
	storage, err := openStorage(yottaConfig)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(storage)
	if err != nil {
		header, err = initializeStorage(storage, yottaConfig)
		if err != nil {
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
	reader, err := store.Reader()
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
