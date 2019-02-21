package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	// "math"
	"math/big"
	"sync"
	"unsafe"

	// use eth hash related func.
	"github.com/ethereum/go-ethereum/common"

	"github.com/yottachain/YTFS/cache"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/errors"
)

const (
	unInitializedCount = 0
)

type rangeTableInfo struct {
	sizes	[]uint32		// data len of each table.
}

// YTFSIndexFile main struct of YTFS index
// it defines the read/write logic of index file structure.
type YTFSIndexFile struct {
	meta   *ydcommon.Header
	index  rangeTableInfo
	store  Storage
	cm     *cache.Manager
	sync.Mutex
}

// Stat reports the YTFSIndexFile status.
func (disk *YTFSIndexFile) MetaData() *ydcommon.Header {
	return disk.meta
}

// Stat reports the YTFSIndexFile status.
func (disk *YTFSIndexFile) Stat() uint32 {
	return uint32(disk.meta.DataCount)
}

// Sync syncs all pending meta and unflushed writes
func (disk *YTFSIndexFile) Sync() error {
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

// Close closes the YTFSIndexFile.
func (disk *YTFSIndexFile) Close() error {
	disk.Sync()
	disk.store.Close()
	return nil
}

// Format formats the YTFSIndexFile file struct.
func (disk *YTFSIndexFile) Format() error {
	return disk.clearTableFromStorage()
}

func (disk *YTFSIndexFile) getTableEntryIndex(key ydcommon.IndexTableKey) uint32 {
	msb := (uint32)(big.NewInt(0).SetBytes(key[common.HashLength - 4:]).Uint64())
	return msb & (disk.meta.RangeCaps - 1)
}

// Get gets IndexTableValue from index table file
func (disk *YTFSIndexFile) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	disk.Lock()
	defer disk.Unlock()
	idx := disk.getTableEntryIndex(key)
	table, err := disk.loadTableFromStorage(idx)
	if err != nil {
		return 0, err
	}
	if value, ok := table[key]; ok {
		return value, nil
	}

	return 0, errors.ErrDataNotFound
}

func (disk *YTFSIndexFile) loadTableFromStorage(tbIndex uint32) (map[ydcommon.IndexTableKey]ydcommon.IndexTableValue, error) {
	reader, _ := disk.store.Reader()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := disk.meta.RangeCoverage * itemSize + 4
	reader.Seek((int64)(disk.meta.HashOffset + tbIndex * tableAllocationSize), io.SeekStart)
	sizeBuf := make([]byte, 4)
	reader.Read(sizeBuf)
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	tableBuf := make([]byte, tableSize * itemSize, tableSize * itemSize)
	// skip length of table
	_, err := reader.Read(tableBuf)
	if err != nil {
		return nil, err
	}

	table := map[ydcommon.IndexTableKey]ydcommon.IndexTableValue{}
	for i := uint32(0); i < tableSize; i++ {
		key := common.BytesToHash(tableBuf[i * itemSize : i * itemSize + 32])
		value := binary.LittleEndian.Uint32(tableBuf[i*itemSize + 32 : i*itemSize + 36][:])
		table[ydcommon.IndexTableKey(key)] = ydcommon.IndexTableValue(value)
	}

	return table, nil
}

func (disk *YTFSIndexFile) clearTableFromStorage() error {
	writer, _ := disk.store.Writer()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := disk.meta.RangeCoverage * itemSize + 4
	valueBuf := make([]byte, 4)

	for tbIndex := uint32(0); tbIndex < disk.meta.RangeCaps; tbIndex++ {
		writer.Seek((int64)(disk.meta.HashOffset + tbIndex * tableAllocationSize), io.SeekStart)
		tableSize := 0
		binary.LittleEndian.PutUint32(valueBuf, uint32(tableSize))
		_, err := writer.Write(valueBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Put saves a key value pair.
func (disk *YTFSIndexFile) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	idx := disk.getTableEntryIndex(key)
	rowCount := disk.index.sizes[idx]
	if rowCount >= disk.meta.RangeCoverage {
		return errors.ErrRangeFull
	}

	table, err := disk.loadTableFromStorage(idx)
	if err != nil {
		return err
	}
	// write cnt
	writer, _ := disk.store.Writer()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := disk.meta.RangeCoverage * itemSize + 4
	tableBeginPos := (int64)(disk.meta.HashOffset + idx * tableAllocationSize)

	valueBuf := make([]byte, 4)
	writer.Seek(tableBeginPos, io.SeekStart)
	tableSize := uint32(len(table)) + 1
	binary.LittleEndian.PutUint32(valueBuf, uint32(tableSize))
	_, err = writer.Write(valueBuf)
	if err != nil {
		return err
	}

	// write new item
	tableItemPos := tableBeginPos + 4 + int64(len(table)) * int64(itemSize)
	writer.Seek(tableItemPos, io.SeekStart)
	_, err = writer.Write(key[:])
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(valueBuf, uint32(value))
	_, err = writer.Write(valueBuf)
	if err != nil {
		return err
	}
	writer.Sync()
	return err
}

// OpenYTFSIndexFile opens or creates a YTFSIndexFile for the given storage.
// The DB will be created if not exist, unless Error happens.
//
// OpenYTFSIndexFile will return ErrConfigXXX if config is incorrect.
//
// The returned YTFSIndexFile instance is safe for concurrent use.
// The YTFSIndexFile must be closed after use, by calling Close method.
//
func OpenYTFSIndexFile(path string, yottaConfig *opt.Options) (*YTFSIndexFile, error) {
	storage, err := openIndexStorage(path, yottaConfig)
	if err != nil {
		return nil, err
	}

	header, err := readIndexHeader(storage)
	if err != nil {
		header, err = initializeIndexStorage(storage, yottaConfig)
		if err != nil {
			return nil, err
		}
	}

	yd := &YTFSIndexFile{
		header,
		rangeTableInfo{sizes : make([]uint32, header.RangeCaps, header.RangeCaps)},
		storage,
		nil,
		sync.Mutex{},
	}

	fmt.Println("Open YTFSIndexFile Success @" + path)
	return yd, nil
}

func initializeIndexStorage(store Storage, config *opt.Options) (*ydcommon.Header, error) {
	writer, err := store.Writer()
	if err != nil {
		return nil, err
	}

	m, n := config.IndexTableCols, config.IndexTableRows
	t, d, h := config.TotalVolumn, config.DataBlockSize, uint32(unsafe.Sizeof(ydcommon.Header{}))

	ytfsSize := uint64(0)
	for _, storageOption := range config.Storages {
		ytfsSize += storageOption.StorageVolume
	}

	// write header.
	header := ydcommon.Header{
		Tag            : [4]byte{'Y', 'T', 'F', 'S'},
		Version        : [4]byte{'0', '.', '0', '3'},
		YtfsCapability : t,
		YtfsSize       : ytfsSize,
		DataBlockSize  : d,
		RangeCaps      : n,
		RangeCoverage  : m,
		HashOffset     : h,
		DataCount      : 0,
		ResolveOffset  : 0,
		Reserved       : 0xCDCDCDCDCDCDCDCD,
	}

	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	// file layout
	// +--------------+
	// |    header    |
	// +---+----------+
	// | 4 |  m*36    |  1
	// +---+----------+
	// | 4 |  m*36    |  2
	// +---+----------+
	// | 4 |  ....    |  ...
	// +---+----------+
	// | 4 |  m*36    |  n
	// +---+----------+
	// | 4 |  m*36    |  n+1 for conflict/overflow
	// +---+----------+
	// | TAG: eofPos  |
	// +---+----------+
	eofPos := int64(m * 36 + 4) * int64(n + 1)  + int64(h)
	writer.Seek(eofPos, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &eofPos)
	if err != nil {
		return nil, err
	}
	writer.Sync()
	return &header, nil
}

func openIndexStorage(path string, opt *opt.Options) (Storage, error) {
	fileStorage := FileStorage{
		readOnly:	opt.ReadOnly,
		mu:			sync.RWMutex{},
		fd:			&FileDesc{
						Type:	ydcommon.DummyStorageType,
						Caps:	0,
						Path:	path,
					},
	}

	writer, err := fileStorage.Create(*fileStorage.fd)
	if err != nil {
		return nil, err
	}
	if !opt.ReadOnly {
		fileStorage.writer = writer
	} else {
		writer.Close()
	}

	reader, err := fileStorage.Open(*fileStorage.fd)
	if err != nil {
		return nil, err
	}
	fileStorage.reader = reader

	return &fileStorage, nil
}

func readIndexHeader(store Storage) (*ydcommon.Header, error) {
	reader, err := store.Reader()
	if err != nil {
		return nil, err
	}

	header := ydcommon.Header{}
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

	if header.Tag[0] != 'Y' {
		return nil, errors.ErrHeadNotFound
	}

	return &header, nil
}

