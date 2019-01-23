package yottadisk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"sync"
	"unsafe"
	
	// use eth hash related func.
	"github.com/ethereum/go-ethereum/common"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"
	"github.com/yottachain/YTFS/cache"
)

type HashRangeIndex struct {
	total	uint32			// total data saved.
	sizes	[]uint16		// data len of each table.
}

type YottaDisk struct {
	config		*opt.Options
	meta		ydcommon.Header
	store		storage.Storage
	index		HashRangeIndex
	cm			*cache.CacheManager
	sync.Mutex
}

func (disk *YottaDisk) Close() {
	if !disk.config.ReadOnly {
		disk.flushMetaAndHashRegion()
	}
	disk.store.Close()
}

func (disk *YottaDisk) Get(key ydcommon.IndexTableKey) ([]byte, error) {
	disk.Lock()
	defer disk.Unlock()
	idx := disk.getTableEntryIndex(key)
	rowCount := disk.index.sizes[idx]

	if rowCount != 0 {
		var table ydcommon.IndexTable

		if disk.cm.Contains(idx) {
			val, _ := disk.cm.Get(idx)
			table = val.(ydcommon.IndexTable)
		} else {
			table = disk.loadTableFromStorage(idx, rowCount)
			disk.cm.Add(idx, table)
		}

		if innerIdx, ok := table[key]; ok {
			return disk.readData(innerIdx)
		} 
	}

	return nil, ErrDataNotFound
}

func (disk *YottaDisk) readData(dataIndex ydcommon.IndexTableValue) ([]byte, error) {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	reader, _ := disk.store.Reader()
	reader.Seek((int64)(disk.meta.DataOffset + (uint64)(dataIndex) * (uint64)(disk.meta.DataBlockSize)), io.SeekStart)
	buf := make([]byte, disk.meta.DataBlockSize, disk.meta.DataBlockSize)
	// binary.Read(reader, binary.LittleEndian, &buf)
	_, err := reader.Read(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (disk *YottaDisk) writeData(idx uint32, key ydcommon.IndexTableKey, dataOffsetIndex ydcommon.IndexTableValue, buf []byte) error {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	// Step 1. write back total count
	writer, err := disk.store.Writer()
	dataCount := (uint32)(dataOffsetIndex) + 1
	// if disk.config.MetaSyncPeriod == 0 then check (dataCount & 0xFFFFFFFF) which always non-0
	// else check (dataCount & 0x000000FF) which happens in fixed period [here takes 256 as a example]
	if (dataCount & (disk.config.MetaSyncPeriod - 1)) == 0 {
		// TODO: if not writeMetaSync, we need to write meta maybe in a fixed period.
		dataCountBuf := []byte{}
		dcBuf := bytes.NewBuffer(dataCountBuf)
		err = binary.Write(dcBuf, binary.LittleEndian, dataCount)
		if err != nil {
			return err
		}

		// Update data count.
		writer.Seek((int64)(unsafe.Offsetof(disk.meta.DataCount)), io.SeekStart)
		_, err = writer.Write(dcBuf.Bytes())
		if err != nil {
			return err
		}

		// Step 2. write RangeLen table
		rowCountBlock := []byte{}
		rcBuf := bytes.NewBuffer(rowCountBlock)
		rowCount := disk.index.sizes[idx]
		err = binary.Write(rcBuf, binary.LittleEndian, rowCount)
		if err != nil {
			return err
		}

		// Update RangeTable sizes
		writer.Seek((int64)(disk.meta.RangeOffset + (uint32)(idx * 2)), io.SeekStart)
		_, err = writer.Write(rcBuf.Bytes())
		if err != nil {
			return err
		}

		// Step 3. Update in range HashIndexTable
		row := ydcommon.IndexItem{
			Hash: 		(ydcommon.IndexTableKey)(key),
			OffsetIdx:  (ydcommon.IndexTableValue)(dataOffsetIndex),
		}
		rowInfoBlock := make([]byte, 0, unsafe.Sizeof(row))
		riBuf := bytes.NewBuffer(rowInfoBlock)
		err = binary.Write(riBuf, binary.LittleEndian, row)
		if err != nil {
			return err
		}
		rowSize := (uint64)(unsafe.Sizeof(row))
		rowIdx	:= rowCount - 1
		writer.Seek((int64)(disk.meta.HashOffset + (uint64)(idx) * rowSize * (uint64)(disk.meta.RangeCoverage) + rowSize * (uint64)(rowIdx)), io.SeekStart)
		_, err = writer.Write(riBuf.Bytes())
		if err != nil {
			return err
		}
	}
	// Step 4. Write Data
	ydcommon.YottaAssert(len(buf) <= (int)(disk.meta.DataBlockSize))
	dataBlock := make([]byte, disk.meta.DataBlockSize, disk.meta.DataBlockSize)
	copy(dataBlock, buf)
	writer.Seek((int64)(disk.meta.DataOffset +
		(uint64)(disk.meta.DataBlockSize) * (uint64)(dataOffsetIndex)), io.SeekStart)
	// fmt.Println("Write", dataBlock, "@", (int64)(disk.meta.DataOffset + (uint64)(disk.meta.DataBlockSize) * (uint64)(dataOffsetIndex)))
	_, err = writer.Write(dataBlock)
	if err != nil {
		return err
	}

	if disk.config.Sync {
		return writer.Sync()
	}

	return nil
}

func (disk *YottaDisk) Put(key ydcommon.IndexTableKey, buf []byte) error {
	if disk.config.ReadOnly {
		return ErrReadOnly
	}

	disk.Lock()
	defer disk.Unlock()
	idx := disk.getTableEntryIndex(key)
	rowCount := disk.index.sizes[idx]
	if rowCount >= (uint16)(disk.meta.RangeCoverage) {
		return ErrRangeFull
	}

	dataCount := disk.index.total
	disk.index.total++
	disk.meta.DataCount++
	if rowCount == 0 {
		table := ydcommon.IndexTable{}
		table[(ydcommon.IndexTableKey)(key)] = (ydcommon.IndexTableValue)(dataCount)
		disk.index.sizes[idx]++
		disk.cm.Add(idx, table)
	} else {
		var table ydcommon.IndexTable
		if disk.cm.Contains(idx) {
			val, _ := disk.cm.Get(idx)
			table = val.(ydcommon.IndexTable)
		} else {
			table = disk.loadTableFromStorage(idx, rowCount)
		}
		ydcommon.YottaAssert(len(table) == (int)(rowCount))
		if _, ok := table[(ydcommon.IndexTableKey)(key)]; ok {
			return ErrConflict
		}
		table[(ydcommon.IndexTableKey)(key)] = (ydcommon.IndexTableValue)(dataCount)
		disk.cm.Add(idx, table)
		disk.index.sizes[idx]++
	}
	return disk.writeData(idx, key, (ydcommon.IndexTableValue)(dataCount), buf)
}

func (disk *YottaDisk) flushMetaAndHashRegion() error {
	locker, _ := disk.store.Lock()
	writer, _ := disk.store.Writer()
	writer.Seek(0, io.SeekStart)
	err := binary.Write(writer, binary.LittleEndian, disk.meta)
	if err != nil {
		return err
	}

	// write ranges
	writer.Seek((int64)(disk.meta.RangeOffset), io.SeekStart)
	// write range hashmap len array
	for i := (uint32)(0); i < disk.meta.RangeCaps; i++ {
		err = binary.Write(writer, binary.LittleEndian, disk.index.sizes[i])
		if err != nil {
			return err
		}
	}
	locker.Unlock()

	// clear cache leads to write, free lock before.
	disk.cm.Purge()

	// force sync
	writer.Sync()
	return nil
}

func OpenYottaDisk(config *opt.Options) (*YottaDisk, error) {
	if !ydcommon.IsPowerOfTwo((uint64)(config.N)) {
		return nil, opt.ErrConfigN
	}

	yottaConfig, err := opt.FinalizeConfig(config)
	if err != nil {
		return nil, err
	}

	storage, err := storage.OpenFileStorage(yottaConfig)
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

	return buildYottaDisk(header, storage, yottaConfig)
}

func buildYottaDisk(header *ydcommon.Header, storage storage.Storage, opt *opt.Options) (*YottaDisk, error) {
	index := HashRangeIndex{
		total:	0,
		sizes:	make([]uint16, header.RangeCaps, header.RangeCaps),
	}

	reader, err := storage.Reader()
	if err != nil {
		return nil, err
	}
	reader.Seek((int64)(header.RangeOffset), io.SeekStart)
	err = binary.Read(reader, binary.LittleEndian, &index.sizes)
	if err != nil {
		return nil, err
	}

	yd := &YottaDisk{
		opt,
		*header,
		storage,
		index,
		nil,
		sync.Mutex{},
	}

	err = yd.initializeCacheManager()
	if err != nil {
		return nil, err
	}

	fmt.Println("Open YottaDisk Success @" + opt.StorageName)
	return yd, nil
}

func initializeStorage(store storage.Storage, config *opt.Options) (*ydcommon.Header, error) {
	writer, err := store.Writer()
	if err != nil {
		return nil, err
	}

	t, d, n, m, h := config.T, config.D, config.N, config.M, (uint64)(unsafe.Sizeof(ydcommon.Header{}))
	rangeEntrySize := (uint64)(unsafe.Sizeof((uint16)(0))) // 2
	ydcommon.YottaAssert(rangeEntrySize == 2)
	hashTableEntrySize := (uint64)(unsafe.Sizeof(ydcommon.IndexItem{})) // 36
	ydcommon.YottaAssert(hashTableEntrySize == 36)

	// in case data overflows.
	ydcommon.YottaAssert((n <= math.MaxUint16 + 1) && (m <= math.MaxUint16))

	// write header.
	hashOffset := h + rangeEntrySize * (uint64)(n)
	// TODO: consider alignment of each segment?
	dataOffset := hashOffset + hashTableEntrySize * (uint64)(m) * (uint64)(n)
	allocOffset := dataOffset + (uint64)(n) * (uint64)(d) * (uint64)(m)
	resolveOffset := allocOffset + (uint64)(n) * (uint64)(m) / 8
	ydcommon.YottaAssert(resolveOffset <= t)

	header := ydcommon.Header{
		Tag:			[4]byte{'Y', 'O', 'T', 'A'},
		Version:		[4]byte{0x0, '.', 0x0, 0x1},
		DiskCaps:		t,
		DataBlockSize:	d,
		RangeCaps:		n,
		RangeCoverage:	m,
		RangeOffset:	(uint32)(h),
		HashOffset:		hashOffset,
		DataOffset:		dataOffset,
		DataCount:		0,
		AllocOffset:	allocOffset,
		ResolveOffset:	resolveOffset,
		Reserved:		(t - resolveOffset) % (uint64)(d),
	}

	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	writer.Seek((int64)(h), io.SeekStart)
	// write range hashmap len array
	for i := (uint32)(0); i < n; i++ {
		err = binary.Write(writer, binary.LittleEndian, (uint16)(0))
		if err != nil {
			return nil, err
		}
	}

	writer.Sync()
	return &header, nil 
}

func readHeader(store storage.Storage) (*ydcommon.Header, error) {
	reader, err := store.Reader()
	if err != nil {
		return nil, err
	}

	header := ydcommon.Header{}
	reader.Seek(0, io.SeekStart)

	buf := make([]byte, unsafe.Sizeof(header), unsafe.Sizeof(header))
	n, err := reader.Read(buf)
	if (err != nil) || (n != (int)(unsafe.Sizeof(header))){
		return nil, err
	}
	bufReader := bytes.NewBuffer(buf)
	err = binary.Read(bufReader, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	if header.Tag[0] != 'Y' {
		return nil, ErrHeaderNotFound
	}

	return &header, nil
}

func (disk *YottaDisk) initializeCacheManager() error {
	maxTableSize := disk.meta.RangeCoverage * (uint32)(unsafe.Sizeof(ydcommon.IndexItem{}))
	cm, err := cache.NewCacheManager(maxTableSize, disk.config.CacheSize, disk.saveTableToStorage)
	if err != nil {
		return err
	}
	disk.cm = cm
	return nil
}

func (disk *YottaDisk) getTableEntryIndex(key ydcommon.IndexTableKey) uint32 {
	msb := (uint32)(big.NewInt(0).SetBytes(key[common.HashLength - 4:]).Uint64())
	return msb & (disk.meta.RangeCaps - 1)
}

func (disk *YottaDisk) saveTableToStorage(key, value interface{}) {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()
	if disk.config.ReadOnly {
		return
	}

	table := value.(ydcommon.IndexTable)
	idx := key.(uint32)
	ydcommon.YottaAssertMsg(len(table) == (int)(disk.index.sizes[idx]),
							fmt.Sprintf("Error in %d entry: table.size(%d) != index.size(%d).", idx, len(table), disk.index.sizes[idx]))
	rowSize := (int)(unsafe.Sizeof(ydcommon.IndexItem{}))
	writer, _ := disk.store.Writer()
	_, err := writer.Seek((int64)(disk.meta.HashOffset + (uint64)(disk.meta.RangeCoverage) * (uint64)(idx) * (uint64)(rowSize)), io.SeekStart)
	if err != nil {
		panic(err)
	}

	buf := []byte{}
	for hash, offsetIdx := range table {
		offsetIdxBytes := []byte{
			(byte)(offsetIdx & 0xFF),
			(byte)((offsetIdx >> 8) & 0xFF),
			(byte)((offsetIdx >> 16) & 0xFF),
			(byte)((offsetIdx >> 24) & 0xFF),
		}
		buf = append(buf, hash[:]...)
		buf = append(buf, offsetIdxBytes...)
	}

	_, err = writer.Write(buf)
	if err != nil {
		panic(err)
	}

	if disk.config.Sync {
		writer.Sync()
	}
}

func (disk *YottaDisk) loadTableFromStorage(idx uint32, rows uint16) ydcommon.IndexTable {
	locker, _ := disk.store.Lock()
	defer locker.Unlock()

	table := ydcommon.IndexTable{}
	rowSize := (uint64)(unsafe.Sizeof(ydcommon.IndexItem{}))
	reader, _ := disk.store.Reader()
	reader.Seek((int64)(disk.meta.HashOffset + (uint64)(disk.meta.RangeCoverage) * (uint64)(idx) * (uint64)(rowSize)), io.SeekStart)
	bufSize := (uint64)(rows) * rowSize
	tableBuf := make([]byte, bufSize, bufSize)
	_, err := reader.Read(tableBuf)
	if err != nil {
		panic(err)
	}
	for i := (uint64)(0); i < (uint64)(rows); i++ {
		table[(ydcommon.IndexTableKey)(common.BytesToHash(tableBuf[i*rowSize : i*rowSize + 32]))] =
			(ydcommon.IndexTableValue)(tableBuf[i*rowSize + 35]) << 24 |
			(ydcommon.IndexTableValue)(tableBuf[i*rowSize + 34]) << 16 |
			(ydcommon.IndexTableValue)(tableBuf[i*rowSize + 33]) <<  8 |
			(ydcommon.IndexTableValue)(tableBuf[i*rowSize + 32])
	}
	return table
}

func (disk *YottaDisk) FormatYottaDisk() error {
	// TODO: implement fully format, so far we just break the header
	disk.meta = ydcommon.Header{}
	return nil
}

func (disk *YottaDisk) Meta() *ydcommon.Header {
	return &disk.meta
}

func (disk *YottaDisk) String() string {
	meta, _ := json.MarshalIndent(disk.meta, "", "	")
	min := (int64)(math.MaxInt64)
	max := (int64)(math.MinInt64)
	sum := (int64)(0)
	for i := 0; i < len(disk.index.sizes); i++ {
		sum += (int64)(disk.index.sizes[i])
		if min > (int64)(disk.index.sizes[i]) {
			min = (int64)(disk.index.sizes[i])
		}
		if max < (int64)(disk.index.sizes[i]) {
			max = (int64)(disk.index.sizes[i])
		}
	}
	avg := sum / (int64)(len(disk.index.sizes))
	table := fmt.Sprintf("Total table Count: %d\n"  +
						 "Total saved items: %d\n" +
						 "Maximum table size: %d\n" +
						 "Minimum table size: %d\n" +
						 "Average table size: %d\n" , len(disk.index.sizes), sum, max, min, avg)
	cache := disk.cm.String()
	return string(meta) + "\n" + table + cache
}
