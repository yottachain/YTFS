package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"os"
	"unsafe"

	"github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

type bytesTable map[common.IndexTableKey][]byte

type TableIterator struct {
	ytfsIndexFile *YTFSIndexFile
	tableIndex    uint32
	writePos      uint32
	options       *opt.Options
}

func RebuildIdxHeader(ytfsIndexFile *YTFSIndexFile, mpath string) error {
	writer, err := os.OpenFile(mpath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Open metadata file err")
		return err
	}
	writer.Seek(0, io.SeekStart)
	err = binary.Write(writer, binary.LittleEndian, ytfsIndexFile.meta)
	if err != nil {
		fmt.Println("write header to file err")
	}
	writer.Sync()
	return err
}

// GetTableIterator 返回Table遍历器
func GetTableIterator(indexpath string, opts *opt.Options) (*TableIterator, error) {
	var ti TableIterator
	ytfsIndexFile, err := OpenYTFSIndexFile(indexpath, opts)
	if err != nil {
		return nil, err
	}

	ti.ytfsIndexFile = ytfsIndexFile
	ti.options = opts
	return &ti, nil
}

// GetTable 获取一个Table，指针后移一位
func (ti *TableIterator) GetTable() (common.IndexTable, error) {
	if ti.tableIndex > ti.options.IndexTableRows {
		return nil, fmt.Errorf("table end")
	}
	table, err := ti.ytfsIndexFile.loadTableFromStorage(ti.tableIndex)
	if err != nil {
		return nil, err
	}
	ti.tableIndex = ti.tableIndex + 1
	return table, nil
}

func (ti *TableIterator) GetNoNilTable() (common.IndexTable, error) {
	for {
		table, err := ti.GetTable()
		if err != nil {
			return nil, err
		}
		if table == nil {
			panic(fmt.Errorf("index.db文件已损坏，无法恢复"))
		}
		if len(table) > 0 {
			return table, nil
		}
	}
}

func (ti *TableIterator) Reset() {
	ti.tableIndex = 0
}

func (ti *TableIterator) Len() uint64 {
	return ti.ytfsIndexFile.meta.DataEndPoint
}

func (ti *TableIterator) BlockSize() uint32 {
	return ti.ytfsIndexFile.meta.DataBlockSize
}

func (ti *TableIterator) LoadTable(tbindex uint32) (bytesTable, error) {
	reader, _ := ti.ytfsIndexFile.store.Reader()
	itemSize := uint32(unsafe.Sizeof(common.IndexTableKey{}) + unsafe.Sizeof(common.IndexTableValue(0)))
	tableAllocationSize := ti.ytfsIndexFile.meta.RangeCoverage*itemSize + 4
	reader.Seek(int64(ti.ytfsIndexFile.meta.HashOffset)+int64(tbindex)*int64(tableAllocationSize), io.SeekStart)

	// read len of table
	sizeBuf := make([]byte, 4)
	reader.Read(sizeBuf)
	tableSize := binary.LittleEndian.Uint32(sizeBuf)
	if debugPrint {
		fmt.Println("read table size :=", tableSize, "from", int64(ti.ytfsIndexFile.meta.HashOffset)+int64(tbindex)*int64(tableAllocationSize))
	}

	// read table contents
	tableBuf := make([]byte, tableSize*itemSize, tableSize*itemSize)
	_, err := reader.Read(tableBuf)
	if err != nil {
		return nil, err
	}

	table := make(map[common.IndexTableKey][]byte)
	for i := uint32(0); i < tableSize; i++ {
		key := common.BytesToHash(tableBuf[i*itemSize : i*itemSize+16])
		value := tableBuf[i*itemSize+16 : i*itemSize+20][:]
		table[common.IndexTableKey(key)] = value
	}
	return table, nil
}

func (ti *TableIterator) GetTableBytes() (bytesTable, error) {
	if ti == nil {
		fmt.Println("ti is nil!!")
		return nil, nil
	}
	if ti.tableIndex > ti.options.IndexTableRows {
		return nil, fmt.Errorf("table_end")
	}
	table, err := ti.LoadTable(ti.tableIndex)
	if err != nil {
		return nil, err
	}
	ti.tableIndex = ti.tableIndex + 1
	return table, nil
}

func (ti *TableIterator) GetNoNilTableBytes() (bytesTable, error) {
	for {
		table, err := ti.GetTableBytes()
		if err != nil {
			return nil, err
		}
		if table == nil {
			panic(fmt.Errorf("index.db文件已损坏，无法恢复"))
		}
		if len(table) > 0 {
			return table, nil
		}
	}
}
