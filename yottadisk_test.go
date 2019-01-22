package yottadisk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	// "log"
	// "math/big"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
	// "strconv"
	// "unsafe"

	"github.com/ethereum/go-ethereum/common"

	types "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

const (
	dataBlockSize = 16
)

func testOptions() *opt.Options {
	tmpfile, err := ioutil.TempFile("", "yotta-test")
	if err != nil {
		panic(err)
	}

	return &opt.Options{
		StorageName: tmpfile.Name(),
		StorageType: types.FileStorageType,
		ReadOnly: false,
		Sync: true,
		M: 0,
		N: 1,		// 32
		T: 1 << 30, // 64k
		D: 32768, 		// 16
	}
}

func TestYottaDiskWithFileStorage(t *testing.T) {
	config := testOptions()
	defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()
}

func TestYottaDiskPutGetWithFileStorage(t *testing.T) {
	config := opt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()

	type KeyValuePair struct{
		hash types.IndexTableKey
		buf []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		err := yd.Put(testHash, testHash[:])
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Starting validata %d data blocks\n", dataCaps)
	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := (types.IndexTableKey)(common.HexToHash(fmt.Sprintf("%032X", i)))
		buf, err := yd.Get(testHash)
		if err != nil {
			panic(fmt.Sprintf("Error: %v in %d check", err, i))
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			panic(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", i, testHash, buf[:len(testHash)]))
		}
	}

	fmt.Println(yd)
}

func TestYottaStressConcurrent(t *testing.T) {
	config := opt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := OpenYottaDisk(config)
	if err != nil {
		t.Fatal(err)
	}
	defer yd.Close()

	err = stressWrite(yd)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			fmt.Println("thread i = ", id)
			stressRead(yd)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func stressRead(yd *YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting validata %d data blocks\n", dataCaps)

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for seq, i := range r.Perm((int)(dataCaps)) {
		// printProgress((uint64)(seq), dataCaps - 1)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		buf, err := yd.Get((types.IndexTableKey)(testHash))
		if err != nil {
			fmt.Println(fmt.Sprintf("\nFatal: %d test fail, hash %v, err %v\n\n", seq, testHash, err))
			panic(err)
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			panic(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", seq, testHash, buf[:len(testHash)]))
		}
	}

	fmt.Println(yd)
	return nil
}

func stressWrite(yd *YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)

	for i := (uint64)(0); i < dataCaps; i++ {
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		dataPair := KeyValuePair{
			hash: testHash,
			buf:  testHash[:],
		}
		err := yd.Put((types.IndexTableKey)(dataPair.hash), dataPair.buf[:])
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(yd)
	return nil
}

func TestSaveConfig(t *testing.T) {
	config := opt.DefaultOptions()
	err := opt.SaveConfig(config, "/tmp/yotta-disk.json")
	if err != nil {
		t.Fatal(err)
	}
}
