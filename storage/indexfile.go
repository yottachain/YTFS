package storage

import (
	"bytes"
	"crypto"
	"encoding/binary"
	"fmt"
	"github.com/mr-tron/base58/base58"
	"io"

	// "math"
	"math/big"
	"sync"
	"unsafe"

	// use eth hash related func.
	//"github.com/ethereum/go-ethereum/common"

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

func (indexFile *YTFSIndexFile) VerifyVHF(data []byte, vhf []byte) bool {
	sha := crypto.MD5.New()
	sha.Write(data)
	return bytes.Equal(sha.Sum(nil), vhf)
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
	msb := (uint32)(big.NewInt(0).SetBytes(key[ydcommon.HashLength-4:]).Uint64())
	return msb & (indexFile.meta.RangeCapacity - 1)
}
func (indexFile *YTFSIndexFile) GetTableEntryIndex(key ydcommon.IndexTableKey) uint32 {
	msb := (uint32)(big.NewInt(0).SetBytes(key[ydcommon.HashLength-4:]).Uint64())
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
			fmt.Printf("IndexDB get %x:%x\n", key, value)
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
				fmt.Printf("IndexDB get %x:%x @overflow table\n", key, value)
			}
			return value, nil
		}
	}

	if debugPrint {
		fmt.Printf("IndexDB get %x failed, from %d-size table\n", key, len(table))
	}
	return 0, errors.ErrDataNotFound
}

func (indexFile *YTFSIndexFile) GetTableFromStorage(tbIndex uint32) (map[ydcommon.IndexTableKey]ydcommon.IndexTableValue, error) {
	table, err := indexFile.loadTableFromStorage(tbIndex)
	return table, err
}

func (indexFile *YTFSIndexFile) loadTableFromStorage(tbIndex uint32) (map[ydcommon.IndexTableKey]ydcommon.IndexTableValue, error) {
	reader, _ := indexFile.store.Reader()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	reader.Seek(int64(indexFile.meta.HashOffset)+int64(tbIndex)*int64(tableAllocationSize), io.SeekStart)

	// read len of table
	sizeBuf := make([]byte, 4)
	reader.Read(sizeBuf)
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	if debugPrint {
		fmt.Println("read table size :=", tableSize, "from", int64(indexFile.meta.HashOffset)+int64(tbIndex)*int64(tableAllocationSize))
	}

	// read table contents
	tableBuf := make([]byte, tableSize*itemSize, tableSize*itemSize)
	_, err := reader.Read(tableBuf)
	if err != nil {
		return nil, err
	}

	table := map[ydcommon.IndexTableKey]ydcommon.IndexTableValue{}
	for i := uint32(0); i < tableSize; i++ {
		key := ydcommon.BytesToHash(tableBuf[i*itemSize : i*itemSize+16])
		value := binary.LittleEndian.Uint32(tableBuf[i*itemSize+16 : i*itemSize+20][:])
		table[ydcommon.IndexTableKey(key)] = ydcommon.IndexTableValue(value)
	}
	return table, nil
}

func (indexFile *YTFSIndexFile) ClearItemFromTable(tbidx uint32, hashKey ydcommon.IndexTableKey, btCnt uint32, tbItemMap map[uint32]uint32) error {
	var err error
	writer, err := indexFile.store.Writer()
	if err != nil {
		fmt.Println("[ClearItemFromTable] get writer error!")
		return err
	}
	//Sync file to stable storage
	writer.Sync()

	reader, err := indexFile.store.Reader()
	if err != nil {
		fmt.Println("[ClearItemFromTable] get reader error!")
		return err
	}
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	tableOffset := int64(indexFile.meta.HashOffset) + int64(tbidx)*int64(tableAllocationSize)
	reader.Seek(tableOffset, io.SeekStart)

	// read len of table
	sizeBuf := make([]byte, 4)
	reader.Read(sizeBuf)
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	itemBuf := make([]byte, itemSize)
	zeroBuf := make([]byte, itemSize)

	for i := tableSize; 0 <= i; i-- {
		itemOffset := tableOffset + 4 + int64(i*itemSize)
		reader.Seek(itemOffset, io.SeekStart)
		reader.Read(itemBuf)
		key := ydcommon.BytesToHash(itemBuf[0:16])
		if ydcommon.IndexTableKey(key) == hashKey {
			fmt.Printf("[restoreIndex] [ClearItemFromTable] tableindex:%v, reset key %v to zero \n", tbidx, base58.Encode(key[:]))
			writer.Seek(itemOffset, io.SeekStart)
			//clear the item in index.db
			writer.Write(zeroBuf)
			tbItemMap[tbidx] = tbItemMap[tbidx] + 1
			break
		}
	}
	return nil
}

func (indexFile *YTFSIndexFile) ResetTableSize(tbItemMap map[uint32]uint32) error {
	var err error
	writer, _ := indexFile.store.Writer()
	reader, _ := indexFile.store.Reader()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	sizeBuf := make([]byte, 4)
	for tbidx, value := range tbItemMap {
		tableOffset := int64(indexFile.meta.HashOffset) + int64(tbidx)*int64(tableAllocationSize)
		reader.Seek(tableOffset, io.SeekStart)
		reader.Read(sizeBuf)
		tableSize := binary.LittleEndian.Uint32(sizeBuf)
		tableSize = tableSize - value
		writer.Seek(tableOffset, io.SeekStart)
		binary.LittleEndian.PutUint32(sizeBuf, tableSize)
		_, err = writer.Write(sizeBuf)
		if err != nil {
			return err
		}
		reader.Seek(tableOffset, io.SeekStart)
		reader.Read(sizeBuf)
		tableSize = binary.LittleEndian.Uint32(sizeBuf)
		fmt.Printf("[resettablesize]  after reset, tbidx=%v, tablesize=%v \n", tbidx, tableSize)
	}
	return err
}

func (indexFile *YTFSIndexFile) clearTableFromStorage() error {
	writer, _ := indexFile.store.Writer()
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	valueBuf := make([]byte, 4)

	for tbIndex := uint32(0); tbIndex < indexFile.meta.RangeCapacity; tbIndex++ {
		writer.Seek(int64(indexFile.meta.HashOffset)+int64(tbIndex)*int64(tableAllocationSize), io.SeekStart)
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

	if _, ok := table[key]; ok {
		return errors.ErrConflict
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
	tableBeginPos := int64(indexFile.meta.HashOffset) + int64(idx)*int64(tableAllocationSize)

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
		fmt.Printf("IndexDB put %x:%x\n", key, value)
	}

	if (indexFile.stat.putCount & (indexFile.config.SyncPeriod - 1)) == 0 {
		writer.Sync()
	}
	return err
}

// BatchPut saves a key value pair.
func (indexFile *YTFSIndexFile) BatchPut(kvPairs []ydcommon.IndexItem) (map[ydcommon.IndexTableKey]byte, error) {
	locker, _ := indexFile.store.Lock()
	defer locker.Unlock()

	dataWritten := uint64(0)
	conflicts := map[ydcommon.IndexTableKey]byte{}
	for _, kvPair := range kvPairs {
		err := indexFile.updateTable(kvPair.Hash, kvPair.OffsetIdx)
		if err != nil {
			if err == errors.ErrConflict {
				conflicts[kvPair.Hash] = 1
			} else {
				return conflicts, err
			}
		}

		dataWritten++
	}

	//if len(conflicts) != 0 {
	//	return conflicts, errors.ErrConflict
	//}

	return conflicts, indexFile.updateMeta(dataWritten)
}

func (indexFile *YTFSIndexFile) UpdateMeta(dataWritten uint64) error {
	return indexFile.updateMeta(dataWritten)
}

func (indexFile *YTFSIndexFile) updateMeta(dataWritten uint64) error {
	indexFile.meta.DataEndPoint += dataWritten
	valueBuf := make([]byte, 4)
	writer, _ := indexFile.store.Writer()
	binary.LittleEndian.PutUint32(valueBuf, uint32(indexFile.meta.DataEndPoint))
	header := indexFile.meta
	writer.Seek(int64(unsafe.Offsetof(header.DataEndPoint)), io.SeekStart)
	_, err := writer.Write(valueBuf)
	if err != nil {
		return err
	}

	if (indexFile.stat.putCount & (indexFile.config.SyncPeriod - 1)) == 0 {
		err = writer.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (indexFile *YTFSIndexFile) getTableSize(tbIndex uint32) (*uint32, error) {
	reader, err := indexFile.store.Reader()
	if err != nil {
		fmt.Println("get indexFile reader error:", err)
		return nil, err
	}
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	_, err = reader.Seek(int64(indexFile.meta.HashOffset)+int64(tbIndex)*int64(tableAllocationSize), io.SeekStart)
	if err != nil {
		fmt.Println("seek new pos of indexFile for read error:", err)
		return nil, err
	}

	// read len of table
	sizeBuf := make([]byte, 4)
	_, err = reader.Read(sizeBuf)
	if err != nil {
		fmt.Println("read tablesize from indexFile error:", err)
		return nil, err
	}
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	if debugPrint {
		fmt.Println("read table size :=", tableSize, "from", int64(indexFile.meta.HashOffset)+int64(tbIndex)*int64(tableAllocationSize))
	}
	return &tableSize, nil
}

func (indexFile *YTFSIndexFile) updateTable(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	idx := indexFile.getTableEntryIndex(key)
	//table, err := indexFile.loadTableFromStorage(idx)
	//
	//if err != nil {
	//	fmt.Println("[memtrace] loadTableFromStorage err:",err)
	//	return err
	//}
	//
	//if _, ok := table[key]; ok {
	//	fmt.Println("[memtrace] updateTable conflict!!")
	//	return errors.ErrConflict
	//}

	//	rowCount := uint32(len(table))

	rowCountPtr, err := indexFile.getTableSize(idx)
	if err != nil {
		fmt.Printf("get tablesize of indextable=%v, error:%v \n", idx, err)
		return err
	}
	rowCount := *rowCountPtr
	if rowCount >= indexFile.meta.RangeCoverage {
		// move to overflow region
		idx = indexFile.meta.RangeCapacity
		//table, err = indexFile.loadTableFromStorage(idx)
		//if err != nil {
		//	fmt.Println("[memtrace] loadTableFromStorage error:",err)
		//	return err
		//}
		//rowCount := uint32(len(table))
		rowCountPtr, err := indexFile.getTableSize(idx)
		if err != nil {
			fmt.Printf("get tablesize of indextable=%v, error:%v \n", idx, err)
			return err
		}
		rowCount := *rowCountPtr
		if rowCount >= indexFile.meta.RangeCoverage {
			fmt.Println("[memtrace] indexFile.meta.RangeCoverage error:", errors.ErrRangeFull)
			return errors.ErrRangeFull
		}
	}

	// write cnt
	writer, err := indexFile.store.Writer()
	if err != nil {
		fmt.Println("get indexFile writer error:", err)
		return err
	}
	itemSize := uint32(unsafe.Sizeof(ydcommon.IndexTableKey{}) + unsafe.Sizeof(ydcommon.IndexTableValue(0)))
	tableAllocationSize := indexFile.meta.RangeCoverage*itemSize + 4
	tableBeginPos := int64(indexFile.meta.HashOffset) + int64(idx)*int64(tableAllocationSize)

	valueBuf := make([]byte, 4)
	_, err = writer.Seek(tableBeginPos, io.SeekStart)
	if err != nil {
		fmt.Println("seek new pos of indexFile for write error:", err)
		return err
	}
	tableSize := rowCount + 1
	binary.LittleEndian.PutUint32(valueBuf, uint32(tableSize))
	_, err = writer.Write(valueBuf)
	if err != nil {
		fmt.Println("[memtrace] writer.Write(valueBuf) error:", err)
		return err
	}

	// write new item
	tableItemPos := tableBeginPos + 4 + int64(rowCount)*int64(itemSize)
	writer.Seek(tableItemPos, io.SeekStart)
	_, err = writer.Write(key[:])
	if err != nil {
		fmt.Println("[memtrace] writer.Write tableItemPos error:", err)
		return err
	}

	binary.LittleEndian.PutUint32(valueBuf, uint32(value))
	_, err = writer.Write(valueBuf)
	if err != nil {
		fmt.Println("[memtrace] writer.Write valueBuf error:", err)
		return err
	}

	indexFile.index.sizes[idx] = tableSize
	if debugPrint {
		fmt.Printf("IndexDB put %x:%x\n", key, value)
	}

	return nil
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
		fmt.Println("storage open err")
		return nil, err
	}

	header, err := readIndexHeader(storage)
	if err != nil {
		fmt.Println("read storage index header err")
		header, err = initializeIndexStorage(storage, ytfsConfig)
		if err != nil {
			fmt.Println("initialize index header err")
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
		RecycleOffset:  uint64(h) + (uint64(n)+1)*(uint64(m)*36+4),
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
	// | 4 |  m*20    |  1
	// +---+----------+
	// | 4 |  m*20    |  2
	// +---+----------+
	// | 4 |  ....    |  ...
	// +---+----------+
	// | 4 |  m*20    |  n
	// +---+----------+
	// | 4 |  m*20    |  n+1 for conflict/overflow
	// +---+----------+
	// | TAG: eofPos  |
	// +---+----------+
	if !config.UseKvDb {
		eofPos := int64(m*20+4)*int64(n+1) + int64(h)
		writer.Seek(eofPos, io.SeekStart)
		err = binary.Write(writer, binary.LittleEndian, &eofPos)
		if err != nil {
			return nil, err
		}
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
		fmt.Println("open file for writer err: ",err)
		return nil, err
	}
	if !opt.ReadOnly {
		fileStorage.writer = writer
	} else {
		writer.Close()
	}

	reader, err := fileStorage.Open(*fileStorage.fd)
	if err != nil {
		fmt.Println("open file for reader err: ",err)
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
