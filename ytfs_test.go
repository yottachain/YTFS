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
