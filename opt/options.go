package opt

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"

	yotta "github.com/yotta-disk/common"
)

const (
	MAX_DISK_CAPABILITY	= 1 << 44 // 16T
	MAX_RANGE_COVERAGE	= 1 << 16 // 64K
	MAX_RANGE_NUMBER	= 1 << 16 // 64K
)

var (
	ErrConfigN          = errors.New("yotta config: config.N should be power of 2 and less than MAX_RANGE")
	ErrConfigM          = errors.New("yotta config: config.M setting is incorrect")
	ErrConfigMetaPeriod	= errors.New("yotta config: Meta sync period should be power of 2")
)

// Options Config options
type Options struct {
	StorageName		string				`json:"storage"`
	StorageType 	yotta.StorageType	`json:"type"`
	ReadOnly		bool				`json:"readonly"`
	Sync			bool				`json:"writesync"`
	MetaSyncPeriod	uint32				`json:"metadatasync"`
	CacheSize		uint64				`json:"cache"`
	M 				uint32 				`json:"M"`
	N 				uint32 				`json:"N"`
	T 				uint64 				`json:"T"`
	D 				uint32 				`json:"D"`
}

// DefaultOptions default config
func DefaultOptions() *Options {
	tmpFile, err := ioutil.TempFile("", "yotta-play")
	if err != nil {
		panic(err)
	}

	config := &Options{
		StorageName: tmpFile.Name(),
		StorageType: yotta.FileStorageType,
		ReadOnly: false,
		Sync: true,
		MetaSyncPeriod: 0,	// write meta every ${MetaSyncPeriod} data arrives. set to 0 if not sync with data write.
		CacheSize: 0,		// Size cache in byte. Can be 0 which means only 1 L1(Range) table entry will be kepted.
		M: 0,
		N: 128,
		T: 1 << 20,  	// 1M for default
		D: 32,  		// Just save HashLen for test.
	}

	newConfig, err := FinalizeConfig(config)
	if err != nil {
		return nil
	}

	return newConfig
}

func ParseConfig(fileName string) (*Options, error) {
	dat, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	config := Options{}
	err = json.Unmarshal(dat, &config)
	if err != nil {
		return nil, err
	}

	newConfig, err := FinalizeConfig(&config)
	if err != nil {
		return nil, err
	}

	return newConfig, nil
}

func SaveConfig(config *Options, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	defer file.Close()
	data, err := json.MarshalIndent(config, "	", "")
	n, err := file.Write(data)
	if n != len(data) || err != nil {
		return err
	}
	file.Sync()
	return nil
}

func FinalizeConfig(config *Options) (*Options, error) {
	if (config.T > MAX_DISK_CAPABILITY) {
		return nil, errors.New("opt.N > MAX_DISK_CAPABILITY")
	}

	t, d, n, m, h := config.T, (uint64)(config.D), (uint64)(config.N), (uint64)(config.M), (uint64)(unsafe.Sizeof(yotta.Header{}))
	// range len array: N size array of uint16, i.e. [uint16, uint16, ...(N)]
	rangeLenSize := (uint64)(unsafe.Sizeof((uint16)(0)))
	// index table item [Hash (32bytes) | OffsetIdx (uint32)]
	indexTableEntrySize := (uint64)(unsafe.Sizeof(common.Hash{})) + (uint64)(unsafe.Sizeof((uint32)(0)))

	// Total disk allocation:
	// --------+-------------------+-----------------------------+------------+----------------+
	// [Header]|[ RangeTableSizes ]|[         HashTable         ]|[    Data  ]|[     BitMap    ]
	// --------+-------------------+-----------------------------+------------+----------------+
	// h       + rangeLenSize * n  + indexTableEntrySize* n * m  + n * d * m  + (m * n + 7)/ 8 = t
	// --------+-------------------+-----------------------------+------------+----------------+
	m = ((t - h - rangeLenSize * n) * 8 - 7) / (indexTableEntrySize * n * 8 + n * d * 8 + n)
	if m < 4 || m >= MAX_RANGE_COVERAGE {
		// m can not == MAX_RANGE_COVERAGE because max uint16 is (MAX_RANGE_COVERAGE - 1)
		// otherwise we shoud keep +1 in mind when calc the index table size, which seems a little bit tricky.
		return nil, ErrConfigM
	}
	config.M = (uint32)(m)

	if (config.N > MAX_RANGE_NUMBER && !yotta.IsPowerOfTwo((uint64)(config.N))) {
		return nil, ErrConfigN
	}

	if (config.MetaSyncPeriod != 0 && !yotta.IsPowerOfTwo((uint64)(config.MetaSyncPeriod))) {
		return nil, ErrConfigMetaPeriod
	}

	// TODO: return new object.
	return config, nil
}