package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	ytfs "github.com/yottachain/YTFS"
	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
)

var (
	configName string
	home       string
	format     *bool
	testMode   string
	cpuprofile string
	memprofile string
)

func init() {
	flag.StringVar(&configName, "config", "", "Config json file name")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "cpuprofile")
	flag.StringVar(&memprofile, "memprofile", "", "memprofile")
	flag.StringVar(&testMode, "test", "", "Testmode: simple, stress, hybrid")
	flag.StringVar(&home, "home", "", "root directory of YTFS")
	format = flag.Bool("format", false, "format storage")
}

func main() {
	flag.Parse()

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var err error
	var config *opt.Options
	if configName != "" {
		config, err = opt.ParseConfig(configName)
		if err != nil {
			panic(err)
		}
	} else {
		config = opt.DefaultOptions()
	}

	rootDir := home
	if rootDir == "" {
		rootDir, err = ioutil.TempDir("/tmp", "ytfsPlayground")
		if err != nil {
			panic(err)
		}
	}

	ytfs, err := ytfs.Open(rootDir, config)
	if err != nil {
		panic(err)
	}
	defer ytfs.Close()

	if *format {
		err = ytfs.Reset()
	} else {
		switch testMode {
		case "simple":
			err = simpleTest(ytfs)
		case "stress":
			err = stressTestReadAfterWrite(ytfs)
		case "hybrid":
			err = hybridTestReadAfterWrite(ytfs)
		case "read":
			err = stressRead(ytfs)
		case "write":
			err = stressWrite(ytfs)
		case "report":
			err = reportInfo(ytfs)
		default:
			err = simpleTest(ytfs)
		}
	}

	if err != nil {
		panic(err)
	}

	fmt.Println("play completed.")
}

func simpleTest(ytfs *ytfs.YTFS) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	dataPair := []KeyValuePair{}
	for i := 0; i < 10; i++ {
		printProgress((uint64)(i), 9)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		dataPair = append(dataPair, KeyValuePair{
			hash: testHash,
			buf:  testHash[:],
		})
	}

	for i := 0; i < len(dataPair); i++ {
		err := ytfs.Put((ydcommon.IndexTableKey)(dataPair[i].hash), dataPair[i].buf[:])
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < len(dataPair); i++ {
		buf, err := ytfs.Get((ydcommon.IndexTableKey)(dataPair[i].hash))
		if err != nil {
			panic(err)
		}

		if bytes.Compare(buf[:len(dataPair[i].buf)], dataPair[i].buf[:]) != 0 {
			panic(fmt.Sprintf("Fatal: test No.%d fail, want:\n%x\n, get:\n%x\n", i, dataPair[i].buf, buf[:len(dataPair[i].buf)]))
		}
	}

	// fmt.Println(ytfs)
	return nil
}

func printProgress(cursor, volume uint64) {
	bar := [100]byte{}
	percentage := cursor * 100 / volume
	ongoing := "-\\|/"

	for i := 0; i < 100; i++ {
		if i < (int)(percentage) {
			bar[i] = '='
		} else {
			bar[i] = '.'
		}
	}

	if percentage >= 100 {
		fmt.Printf("\033[K[%s] %d%%\n", string(bar[:]), percentage)
	} else {
		fmt.Printf("\033[K[%s] %c %d%%\r", string(bar[:]), ongoing[cursor%4], percentage)
	}
}

func stressWrite(ytfs *ytfs.YTFS) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	dataCaps := ytfs.Cap()
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)

	for i := (uint64)(0); i < dataCaps; i++ {
		printProgress(i, dataCaps-1)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		dataPair := KeyValuePair{
			hash: testHash,
			buf:  testHash[:],
		}
		err := ytfs.Put((ydcommon.IndexTableKey)(dataPair.hash), dataPair.buf[:])
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(ytfs)
	return nil
}

func stressRead(ytfs *ytfs.YTFS) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	dataCaps := ytfs.Cap()
	fmt.Printf("Starting validata %d data blocks\n", dataCaps)

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for seq, i := range r.Perm((int)(dataCaps)) {
		printProgress((uint64)(seq), dataCaps-1)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		buf, err := ytfs.Get((ydcommon.IndexTableKey)(testHash))
		if err != nil {
			fmt.Println(fmt.Sprintf("\nFatal: %d test fail, hash %v, err %v\n\n", seq, testHash, err))
			panic(err)
		}

		if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
			panic(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", seq, testHash, buf[:len(testHash)]))
		}
	}

	return nil
}

func stressTestReadAfterWrite(ytfs *ytfs.YTFS) error {
	err := stressWrite(ytfs)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(id int) {
			err := stressRead(ytfs)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return err
}

func hybridTestReadAfterWrite(ytfs *ytfs.YTFS) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	dataCaps := ytfs.Cap()
	count := 0
	inqueue := 0
	maxQueue := 10000
	exit := make(chan interface{})
	done := make(chan interface{})
	sema := make(chan interface{}, maxQueue)
	go func() {
		for {
			select {
			case <-done:
				count++
				printProgress((uint64)(count), dataCaps)
			case <-exit:
				return
			}
		}
	}()

	fmt.Printf("Starting hybrid test on %d data blocks\n", dataCaps)
	wg := sync.WaitGroup{}
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for _, i := range r.Perm((int)(dataCaps)) {
		wg.Add(1)
		inqueue++
		go func(id uint64) {
			testHash := common.HexToHash(fmt.Sprintf("%032X", id))
			err := ytfs.Put((ydcommon.IndexTableKey)(testHash), testHash[:])
			if err != nil {
				panic(err)
			}
			buf, err := ytfs.Get((ydcommon.IndexTableKey)(testHash))
			if err != nil {
				fmt.Println(fmt.Sprintf("\nFatal: %d test fail, hash %v, err %v\n\n", id, testHash, err))
				panic(err)
			}

			if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
				panic(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", id, testHash, buf[:len(testHash)]))
			}
			done <- struct{}{}
			sema <- struct{}{}
			defer func() { <-sema }()
			wg.Done()
		}((uint64)(i))
	}

	wg.Wait()
	exit <- struct{}{}
	return nil
}

func reportInfo(ytfs *ytfs.YTFS) error {
	fmt.Println(ytfs)
	return nil
}
