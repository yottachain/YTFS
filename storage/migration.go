package storage

import (
	"fmt"
	"github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

type TableIterator struct {
	ytfsIndexFile *YTFSIndexFile
	tableIndex    uint32
	options       *opt.Options
}

// GetTableIterator 返回Table遍历器
func GetTableIterator(path string, opts *opt.Options) (*TableIterator, error) {
	var ti TableIterator
	ytfsIndexFile, err := OpenYTFSIndexFile(path, opts)
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

func (ti *TableIterator) Reset() {
	ti.tableIndex = 0
}
