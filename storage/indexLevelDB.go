package storage

import (
	ydcommon "github.com/yottachain/YTFS/common"
	""
	"github.com/yottachain/YTFS/opt"
	"sync"
)

type YTFSIndexLevelDB struct {
	meta   *ydcommon.Header
	index  rangeTableInfo
	store  Storage
	config *opt.Options
	stat   indexStatistics
	sync.Mutex
}
