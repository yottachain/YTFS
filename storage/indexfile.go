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

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
)

var (
	debugPrint = opt.DebugPrint
)

type rangeTableInfo struct {
	sizes []uint32 // data len of each table.
}

type indexStatistics struct {
	putCount uint32
	delCount uint32
	getCount uint32
}

// YTFSIndexFile main struct of YTFS index
// it defines the read/write logic of index file structure.
type YTFSIndexFile struct {
	meta   *ydcommon.Header
	index  rangeTableInfo
	store  Storage
	config *opt.Options
	stat   indexStatistics
	sync.Mutex
}

// MetaData reports the YTFSIndexFile status.
func (indexFile *YTFSIndexFile) MetaData() *ydcommon.Header {
	return indexFile.meta
}

// Stat reports the YTFSIndexFile status.
func (indexFile *YTFSIndexFile) Stat() uint32 {
	return uint32(indexFile.meta.DataEndPoint)
}

// Sync syncs all pending meta and unflushed writes
func (indexFile *YTFSIndexFile) Sync() error {
	locker, _ := indexFile.store.Lock()
	defer locker.Unlock()

	writer, err := indexFile.store.Writer()
	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &indexFile.meta)
	if err != nil {
		return err
	}

	writer.Sync()
	return nil
}

// Close closes the YTFSIndexFile.
func (indexFile *YTFSIndexFile) Close() error {
	indexFile.Sync()
	indexFile.store.Close()
	return nil
}

// Format formats the YTFSIndexFile file struct.
func (indexFile *YTFSIndexFile) Format() error {
	return indexFile.clearTableFromStorage()
}

func (indexFile *YTFSIndexFile) getTableEntryIndex(key ydcommon.IndexTableKey) uint32 {
	msb := (uint32)(big.NewInt(0).SetBytes(key[common.HashLength-4:]).Uint64())
	return msb & (indexFile.meta.RangeCapacity - 1)
}

// Get gets IndexTableValue from index table file
func (indexFile *YTFSIndexFile) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	locker, _ := indexFile.store.Lock()
	defer locker.Unlock()
	idx := indexFile.getTableEntryIndex(key)
	table, err := indexFile.loadTableFromStorage(idx)
	if err != nil {
		return 0, err
	}

	if value, ok := table[key]; ok {
		if debugPrint {
			fmt.Println("IndexDB get", key, value)
		}
		return value, nil
	}

	// check overflow region if current region is full
	if uint32(len(table)) == indexFile.meta.RangeCoverage {
		idx := indexFile.meta.RangeCapacity
		table, err = indexFile.loadTableFromStorage(idx)
		if err != nil {
			return 0, err
		}

		if value, ok := table[key]; ok {
			if debugPrint {
				fmt.Println("IndexDB get @overflow table", key, value)
			}
			return value, nil
		}
	}

	if debugPrint {
		fmt.Println("IndexDB get", key, "failed, from table", table)
	}
	return 0, errors.ErrDataNotFound
}

func (indexFile *YTFSIndexFile) loadTableFromStorage(tbIndex uint32) (map[ydcommon.IndexTableKey]ydcommon.IndexTableValue, error) {
	reader, _ := indexFile.store.Reader()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	reader.Seek((int64)(indexFile.meta.HashOffset+tbIndex*tableAllocationSize), io.SeekStart)

	// read len of table
	sizeBuf := make([]byte, 4)
	reader.Read(sizeBuf)
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	if debugPrint {
		fmt.Println("read table size :=", sizeBuf, "from", indexFile.meta.HashOffset+tbIndex*tableAllocationSize)
	}

	// read table contents
	tableBuf := make([]byte, tableSize*itemSize, tableSize*itemSize)
	_, err := reader.Read(tableBuf)
	if err != nil {
		return nil, err
	}

	table := map[ydcommon.IndexTableKey]ydcommon.IndexTableValue{}
	for i := uint32(0); i < tableSize; i++ {
		key := common.BytesToHash(tableBuf[i*itemSize : i*itemSize+32])
		value := binary.LittleEndian.Uint32(tableBuf[i*itemSize+32 : i*itemSize+36][:])
		table[ydcommon.IndexTableKey(key)] = ydcommon.IndexTableValue(value)
	}

	return table, nil
}

func (indexFile *YTFSIndexFile) clearTableFromStorage() error {
	writer, _ := indexFile.store.Writer()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	valueBuf := make([]byte, 4)

	for tbIndex := uint32(0); tbIndex < indexFile.meta.RangeCapacity; tbIndex++ {
		writer.Seek((int64)(indexFile.meta.HashOffset+tbIndex*tableAllocationSize), io.SeekStart)
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
func (indexFile *YTFSIndexFile) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	locker, _ := indexFile.store.Lock()
	defer locker.Unlock()

	idx := indexFile.getTableEntryIndex(key)
	table, err := indexFile.loadTableFromStorage(idx)
	if err != nil {
		return err
	}

	rowCount := uint32(len(table))
	if rowCount >= indexFile.meta.RangeCoverage {
		// move to overflow region
		idx = indexFile.meta.RangeCapacity
		table, err = indexFile.loadTableFromStorage(idx)
		if err != nil {
			return err
		}
		rowCount := uint32(len(table))
		if rowCount >= indexFile.meta.RangeCoverage {
			return errors.ErrRangeFull
		}
	}

	// write cnt
	writer, _ := indexFile.store.Writer()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	tableBeginPos := (int64)(indexFile.meta.HashOffset + idx*tableAllocationSize)

	valueBuf := make([]byte, 4)
	writer.Seek(tableBeginPos, io.SeekStart)
	tableSize := uint32(len(table)) + 1
	binary.LittleEndian.PutUint32(valueBuf, uint32(tableSize))
	_, err = writer.Write(valueBuf)
	if err != nil {
		return err
	}

	// write new item
	tableItemPos := tableBeginPos + 4 + int64(len(table))*int64(itemSize)
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

	indexFile.index.sizes[idx] = tableSize
	indexFile.meta.DataEndPoint++
	binary.LittleEndian.PutUint32(valueBuf, uint32(indexFile.meta.DataEndPoint))
	header := indexFile.meta
	writer.Seek(int64(unsafe.Offsetof(header.DataEndPoint)), io.SeekStart)
	_, err = writer.Write(valueBuf)
	if err != nil {
		return err
	}

	if debugPrint {
		fmt.Println("IndexDB put", key, value)
	}

	if (indexFile.stat.putCount & (indexFile.config.SyncPeriod - 1)) == 0 {
		writer.Sync()
	}
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
func OpenYTFSIndexFile(path string, ytfsConfig *opt.Options) (*YTFSIndexFile, error) {
	storage, err := openIndexStorage(path, ytfsConfig)
	if err != nil {
		return nil, err
	}

	header, err := readIndexHeader(storage)
	if err != nil {
		header, err = initializeIndexStorage(storage, ytfsConfig)
		if err != nil {
			return nil, err
		}
	}

	yd := &YTFSIndexFile{
		header,
		rangeTableInfo{sizes: make([]uint32, header.RangeCapacity+1, header.RangeCapacity+1)}, // +1 for overflow region
		storage,
		ytfsConfig,
		indexStatistics{0, 0, 0},
		sync.Mutex{},
	}

	fmt.Println("Open YTFSIndexFile success @" + path)
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
		Tag:            [4]byte{'Y', 'T', 'F', 'S'},
		Version:        [4]byte{'0', '.', '0', '3'},
		YtfsCapability: t,
		YtfsSize:       ytfsSize,
		DataBlockSize:  d,
		RangeCapacity:  n,
		RangeCoverage:  m,
		HashOffset:     h,
		DataEndPoint:   0,
		RecycleOffset:  uint64(h) + (uint64(n) + 1) * (uint64(m)*36 + 4),
		Reserved:       0xCDCDCDCDCDCDCDCD,
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
	eofPos := int64(m*36+4)*int64(n+1) + int64(h)
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
		readOnly: opt.ReadOnly,
		mu:       sync.RWMutex{},
		fd: &FileDesc{
			Type: ydcommon.DummyStorageType,
			Cap:  0,
			Path: path,
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
