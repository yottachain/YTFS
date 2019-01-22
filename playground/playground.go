package main

import (
	"bytes"
	"flag"
	"fmt"
	"runtime/pprof"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/yottachain/YTFS"
	"github.com/yottachain/YTFS/opt"
	ydcommon "github.com/yottachain/YTFS/common"
)

var (
	configName string
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

	yd, err := yottadisk.OpenYottaDisk(config)
	if err != nil {
		panic(err)
	}
	defer yd.Close()

	if *format {
		err = yd.FormatYottaDisk()
	} else {
		switch testMode {
		case "simple":
			err = simpleTest(yd)
		case "stress":
			err = stressTestReadAfterWrite(yd)
		case "hybrid":
			err = hybridTestReadAfterWrite(yd)
		case "read":
			err = stressRead(yd)
		case "write":
			err = stressWrite(yd)
		case "report" :
			err = reportInfo(yd)
		default:
			err = simpleTest(yd)
		}
	}

	if err != nil {
		panic(err)
	}

	fmt.Println("play completed.")
}

func simpleTest(yd *yottadisk.YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	dataPair := []KeyValuePair{}
	for i := 0; i < 256; i++ {
		printProgress((uint64)(i), 255)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		dataPair = append(dataPair, KeyValuePair{
			hash: testHash,
			buf:  testHash[:],
		})
	}

	for i := 0; i < len(dataPair); i++ {
		err := yd.Put((ydcommon.IndexTableKey)(dataPair[i].hash), dataPair[i].buf[:])
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < len(dataPair); i++ {
		buf, err := yd.Get((ydcommon.IndexTableKey)(dataPair[i].hash))
		if err != nil {
			panic(err)
		}

		if bytes.Compare(buf[:len(dataPair[i].buf)], dataPair[i].buf[:]) != 0 {
			panic(fmt.Sprintf("Fatal: test No.%d fail, want:\n%x\n, get:\n%x\n", i, dataPair[i].buf, buf[:len(dataPair[i].buf)]))
		}
	}

	fmt.Println(yd)
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

func stressWrite(yd *yottadisk.YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting insert %d data blocks\n", dataCaps)

	for i := (uint64)(0); i < dataCaps; i++ {
		printProgress(i, dataCaps - 1)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		dataPair := KeyValuePair{
			hash: testHash,
			buf:  testHash[:],
		}
		err := yd.Put((ydcommon.IndexTableKey)(dataPair.hash), dataPair.buf[:])
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(yd)
	return nil
}

func stressRead(yd *yottadisk.YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting validata %d data blocks\n", dataCaps)

	r := rand.New(rand.NewSource(time.Now().Unix()))
	for seq, i := range r.Perm((int)(dataCaps)) {
		printProgress((uint64)(seq), dataCaps - 1)
		testHash := common.HexToHash(fmt.Sprintf("%032X", i))
		buf, err := yd.Get((ydcommon.IndexTableKey)(testHash))
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

func stressTestReadAfterWrite(yd *yottadisk.YottaDisk) error {
	err := stressWrite(yd)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func(id int) {
			err := stressRead(yd)
			if err != nil {
				panic(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return err
}

func hybridTestReadAfterWrite(yd *yottadisk.YottaDisk) error {
	type KeyValuePair struct {
		hash common.Hash
		buf  []byte
	}

	meta := yd.Meta()
	dataCaps := (uint64)(meta.RangeCaps) * (uint64)(meta.RangeCoverage)
	fmt.Printf("Starting hybrid test on %d data blocks\n", dataCaps)
	wg := sync.WaitGroup{}
	done := make(chan interface{})
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for _, i := range r.Perm((int)(dataCaps)) {
		wg.Add(1)
		go func(id uint64) {
			testHash := common.HexToHash(fmt.Sprintf("%032X", id))
			err := yd.Put((ydcommon.IndexTableKey)(testHash), testHash[:])
			if err != nil {
				panic(err)
			}
			buf, err := yd.Get((ydcommon.IndexTableKey)(testHash))
			if err != nil {
				fmt.Println(fmt.Sprintf("\nFatal: %d test fail, hash %v, err %v\n\n", id, testHash, err))
				panic(err)
			}

			if bytes.Compare(buf[:len(testHash)], testHash[:]) != 0 {
				panic(fmt.Sprintf("Fatal: %d test fail, want:\n%x\n, get:\n%x\n", id, testHash, buf[:len(testHash)]))
			}
			done <- struct{}{};
			wg.Done()
		}((uint64)(i))
	}

	count := 0
	exit := make(chan interface{})
	go func() {
		for {
			select {
			case <- done:
				count++
				printProgress((uint64)(count), dataCaps)
			case <- exit:
				return
			}
		}
	}()

	wg.Wait()
	exit <- struct{}{}
	return nil
}

func reportInfo(yd *yottadisk.YottaDisk) error {
	fmt.Println(yd)
	return nil
}