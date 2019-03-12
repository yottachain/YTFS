package ytfs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/errors"
	"github.com/yottachain/YTFS/opt"
)

const (
	dataBlockSize = 1 << 15
)

func makeData(size int) []byte {
	buf := make([]byte, size, size)
	rand.Read(buf)
	return buf
}

func TestNewYTFS(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()
}

func TestErrorOnReadClosedYTFS(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}

	testKey := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", 1)))
	bufIn := makeData(dataBlockSize)
	ytfs.Put(testKey, bufIn)
	ytfs.Close()

	_, err = ytfs.Get(testKey)
	if err == nil {
		t.Fatal(err)
	}
	fmt.Println("expected err:", err)
}

func TestYTFSBasic(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer ytfs.Close()

	testKey := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", 1)))
	bufIn := makeData(dataBlockSize)
	ytfs.Put(testKey, bufIn)

	bufOut, err := ytfs.Get(testKey)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(bufIn, bufOut) != 0 {
		t.Fatal(fmt.Sprintf("Fatal: test fail, want:\n%x\n, get:\n%x\n", bufIn[:10], bufOut[:10]))
	}
}

func TestYTFSRangeOverflow(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()

	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer ytfs.Close()

	dataCaps := uint64(ytfs.Meta().RangeCoverage * 2)
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%X0000000", i)))
		err := ytfs.Put(testHash, testHash[:])
		if err != nil {
			panic(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}

	fmt.Printf("Starting validata %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%X0000000", i)))
		buf, err := ytfs.Get(testHash)
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			t.Fatal(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}

	testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%X0000000", dataCaps+1)))
	err = ytfs.Put(testHash, testHash[:])
	if err != errors.ErrRangeFull {
		t.Fatal(fmt.Sprintf("Error: unmeet expected error RangeFull, but meet %v", err))
	}
}

func TestYTFSFullWriteRead(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()

	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer ytfs.Close()

	dataCaps := ytfs.Cap()
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := ytfs.Put(testHash, testHash[:])
		if err != nil {
			panic(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}

	fmt.Printf("Starting validata %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		buf, err := ytfs.Get(testHash)
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			t.Fatal(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}
}

func TestYTFSConcurrentAccess(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()

	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer ytfs.Close()

	dataCaps := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		dataCaps += uint64(stroageCtx.Cap)
	}

	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	wg := sync.WaitGroup{}
	for i := (uint64)(0); i < dataCaps; i++ {
		wg.Add(1)
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		go func(key types.IndexTableKey, round uint64) {
			err := ytfs.Put(key, key[:])
			if err != nil {
				t.Fatal(fmt.Sprintf("Error: %v in %d insert", err, round))
			}
			wg.Done()
		}(testHash, i)
	}

	wg.Wait()
	fmt.Printf("Starting validata %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		buf, err := ytfs.Get(testHash)
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d-th check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			t.Fatal(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}
}

func TestReloadYTFS(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}

	dataCaps := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		dataCaps += uint64(stroageCtx.Cap)
	}

	for i := (uint64)(0); i < 1; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := ytfs.Put(testHash, testHash[:])
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}
	ytfs.Close()

	ytfsReopen, err := Open(rootDir, config)
	defer ytfsReopen.Close()
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	for i := (uint64)(1); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := ytfsReopen.Put(testHash, testHash[:])
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}

	fmt.Printf("Starting validata %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		buf, err := ytfsReopen.Get(testHash)
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			t.Fatal(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}
}

func TestExpendYTFSConfigCheck(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	validConfig := *config
	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}
	ytfs.Close()

	configNew := opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	dealWithExpensionConfig(t, rootDir, config, nil)

	configNew = opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	config.DataBlockSize = 1 << 16
	dealWithExpensionConfig(t, rootDir, config, opt.ErrConfigD)
	config.DataBlockSize = validConfig.DataBlockSize

	configNew = opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	config.IndexTableRows = config.IndexTableRows * 2
	dealWithExpensionConfig(t, rootDir, config, ErrSettingMismatch)
	config.IndexTableRows = validConfig.IndexTableRows

	configNew = opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	config.TotalVolumn = config.TotalVolumn * 2
	dealWithExpensionConfig(t, rootDir, config, ErrSettingMismatch)
	config.TotalVolumn = validConfig.TotalVolumn

	configNew = opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	config.Storages[len(config.Storages)-1].StorageVolume = config.TotalVolumn
	dealWithExpensionConfig(t, rootDir, config, opt.ErrConfigC)
	config.Storages = config.Storages[:(len(config.Storages) - 1)]

	configNew = opt.DefaultOptions()
	config.Storages = append(config.Storages, configNew.Storages...)
	config.Storages[len(config.Storages)-1].DataBlockSize = 1 << 14
	dealWithExpensionConfig(t, rootDir, config, opt.ErrConfigD)
	config.Storages = config.Storages[:(len(config.Storages) - 1)]
}

func dealWithExpensionConfig(t *testing.T, rootDir string, newConfig *opt.Options, expectErr error) {
	ytfs, err := Open(rootDir, newConfig)
	if ytfs != nil {
		ytfs.Close()
	}
	if err != expectErr {
		t.Fatal(fmt.Errorf("Err: unmet expected err %v, but met %v", expectErr, err))
	}
}

func TestExpendYTFSThenWriteFull(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := opt.DefaultOptions()
	ytfs, err := Open(rootDir, config)
	if err != nil {
		t.Fatal(err)
	}

	dataCaps := uint64(0)
	for _, stroageCtx := range ytfs.context.storages {
		dataCaps += uint64(stroageCtx.Cap)
	}

	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := ytfs.Put(testHash, testHash[:])
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}

	testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", dataCaps)))
	err = ytfs.Put(testHash, testHash[:])
	if err != ErrDataOverflow {
		t.Fatal(fmt.Sprintf("Error: expected error is ErrDataOverflow rather than %v", err))
	}
	ytfs.Close()

	configNew := opt.DefaultOptions()
	// Add one file
	config.Storages = append(config.Storages, configNew.Storages[0])

	ytfsReopen, err := Open(rootDir, config)
	defer ytfsReopen.Close()
	dataCapsNew := ytfsReopen.Cap()
	fmt.Printf("Starting insert other %d data blocks to expend region\n", dataCapsNew-dataCaps)
	for i := dataCaps; i < dataCapsNew; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := ytfsReopen.Put(testHash, testHash[:])
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d insert", err, i))
		}
	}

	fmt.Printf("Starting validata %d data blocks\n", dataCapsNew)
	for i := (uint64)(0); i < dataCapsNew; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		buf, err := ytfsReopen.Get(testHash)
		if err != nil {
			t.Fatal(fmt.Sprintf("Error: %v in %d check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			t.Fatal(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}
}
