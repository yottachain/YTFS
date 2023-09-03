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

const DiskIdxPre uint16 = 0xFF00
const DiskIdxMax uint16 = 0x00FF

var GlobalCapProofTable CapProofInfo

type CapProofDiskInfo struct {
	Lines uint32 //cur disk lines of cap proof table
}

type CapProofInfo struct {
	SrcSize      uint16 //source data size, 64、128、256 bit
	ValueSize    uint16 //hash value of source data, 64bit
	KvItems      uint16 //kv pair nums(source data and hash value)
	TableRows    uint32
	TableRowSize uint32
	DiskInfo     []CapProofDiskInfo
}

func (cp *CapProofInfo) Check(srcData []byte, value []byte) error {
	if cp.SrcSize != uint16(len(srcData)) ||
		cp.ValueSize != uint16(len(value)) {
		return fmt.Errorf("cap proof info error, src data len should be %d, value len shoule be %d\n",
			cp.SrcSize, cp.ValueSize)
	}

	return nil
}

func (cp *CapProofInfo) GetIndex(valueU64 uint64) (int, uint32) {
	tableIndex := uint32(valueU64 % uint64(GlobalCapProofTable.TableRows))

	diskIdx := 0
	curLines := uint32(0)
	curStartLines := uint32(0)
	for idx, diskInfo := range cp.DiskInfo {
		curLines += diskInfo.Lines
		if tableIndex < curLines {
			diskIdx = idx
			break
		}
		curStartLines += diskInfo.Lines
	}

	tableDiskInnerIdx := tableIndex - curStartLines

	return diskIdx, tableDiskInnerIdx
}

func (cp *CapProofInfo) GetDiskInnerOffset(innerIdx uint32) uint64 {
	return uint64(cp.TableRowSize) * uint64(innerIdx)
}

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

// Index disk serial number
func (disk *YottaDisk) Index() uint16 {
	return disk.meta.DiskIdx
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

	//fmt.Printf("[ytfs] storage %s current cc is %d\n", disk.store.GetFd().Path, disk.store.Readercc())

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
func OpenYottaDisk(yottaConfig *opt.StorageOptions, init bool, idx int, dnId uint32) (*YottaDisk, error) {
	storage, err := openStorage(yottaConfig)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(storage)
	if err != nil || init {
		if init || opt.IgnoreStorageHeaderErr {
			header, err = initializeStorage(storage, yottaConfig, idx, dnId)
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
			header, err = initializeStorage(storage, yottaConfig, idx, dnId)
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

	//磁盘乱序的话，容量可能不一致，不判断容量
	//return header.DataBlockSize == yottaConfig.DataBlockSize && header.DiskCapacity == yottaConfig.StorageVolume
	return header.DataBlockSize == yottaConfig.DataBlockSize
}

func initializeStorage(store Storage, config *opt.StorageOptions, idx int, dnId uint32) (*ydcommon.StorageHeader, error) {
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
		Version:       [4]byte{0x0, '.', 0x0, 0x3},
		DiskCapacity:  t,
		DataBlockSize: uint32(d),
		DataOffset:    dataOffset,
		DataCapacity:  uint32((t - h) / d),
		DiskIdx:       DiskIdxPre + uint16(idx),
		Reserved:      uint16((t - h) % d), // left-overs
		//DataNodeId:    0xFFFFFFFF,
		DataNodeId: dnId,
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

func (disk *YottaDisk) WriteCapProofData(dataOffset uint64, srcData []byte, value []byte) error {
	index := disk.store.ReaderIndex()
	defer disk.store.ReaderIndexClose(index)

	reader, _ := disk.store.Reader(index)
	reader.Seek(int64(dataOffset), io.SeekStart)
	kvNums := make([]byte, 4)
	_, err := reader.Read(kvNums)
	if err != nil {
		fmt.Println("WriteCapProofData error:", err.Error())
		return err
	}

	nums := binary.LittleEndian.Uint32(kvNums)
	if nums >= uint32(GlobalCapProofTable.KvItems) {
		return errors.ErrCapProofLineFull
	}

	writer, _ := disk.store.Writer()
	srcDataOffset := int64(dataOffset) + 4 +
		int64(GlobalCapProofTable.KvItems*GlobalCapProofTable.ValueSize) +
		int64(uint32(GlobalCapProofTable.SrcSize)*nums)
	writer.Seek(srcDataOffset, io.SeekStart)
	_, err = writer.Write(srcData)
	if err != nil {
		return err
	}

	valueOffset := int64(dataOffset) + 4 +
		int64(nums*uint32(GlobalCapProofTable.ValueSize))
	writer.Seek(valueOffset, io.SeekStart)
	_, err = writer.Write(value)
	if err != nil {
		return err
	}

	nums += 1
	binary.LittleEndian.PutUint32(kvNums, nums)
	writer.Seek(int64(dataOffset), io.SeekStart)
	_, err = writer.Write(kvNums)
	if err != nil {
		return err
	}

	writer.Sync()

	return nil
}

func (disk *YottaDisk) GetCapProofSrcData(dataOffset uint64, value []byte) (srcData []byte, err error) {
	index := disk.store.ReaderIndex()
	defer disk.store.ReaderIndexClose(index)

	reader, _ := disk.store.Reader(index)
	reader.Seek(int64(dataOffset), io.SeekStart)
	kvNums := make([]byte, 4)
	_, err = reader.Read(kvNums)
	if err != nil {
		fmt.Println("WriteCapProofData error:", err.Error())
		return
	}

	nums := binary.LittleEndian.Uint32(kvNums)
	if nums > uint32(GlobalCapProofTable.KvItems) {
		err = errors.ErrCapProofMetaErr
		return
	}

	allValueSize := nums * uint32(GlobalCapProofTable.ValueSize)
	allValues := make([]byte, allValueSize)
	_, err = reader.Read(allValues)
	if err != nil {
		fmt.Println("GetCapProofSrcData error:", err.Error())
		return
	}

	start := 0
	end := 0
	for i := 0; i < int(nums); i++ {
		start = i * int(GlobalCapProofTable.ValueSize)
		end = (i + 1) * int(GlobalCapProofTable.ValueSize)
		if bytes.Equal(allValues[start:end], value) {
			srcDataOffset := int64(dataOffset) + 4 +
				int64(GlobalCapProofTable.KvItems*GlobalCapProofTable.ValueSize) +
				int64(int(GlobalCapProofTable.SrcSize)*i)
			reader.Seek(srcDataOffset, io.SeekStart)
			sData := make([]byte, GlobalCapProofTable.SrcSize)
			_, err = reader.Read(sData)
			if err != nil {
				fmt.Println("WriteCapProofData error:", err.Error())
				return
			}

			srcData = sData
			break
		}
	}

	return
}
