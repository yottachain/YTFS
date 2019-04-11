// Package recovery
package recovery

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/klauspost/reedsolomon"
	"github.com/yottachain/YTFS"
	ytfsCommon "github.com/yottachain/YTFS/common"
	ytfsOpt "github.com/yottachain/YTFS/opt"
)

func TestNewDataRecovery(t *testing.T) {
	_, err := NewDataRecoverEngine(nil, nil, DefaultRecoveryOption())
	if err != nil {
		t.Fail()
	}
}

func randomFill(size uint32) []byte {
	buf := make([]byte, size, size)
	head := make([]byte, 16, 16)
	rand.Seed(int64(time.Now().Nanosecond()))
	rand.Read(head)
	copy(buf, head)
	return buf
}

func createShards(dataShards, parityShards uint32) ([]common.Hash, [][]byte) {
	shards := make([][]byte, dataShards+parityShards)
	hashes := make([]common.Hash, dataShards+parityShards)
	dataBlkSize := ytfsOpt.DefaultOptions().DataBlockSize
	for i := uint32(0); i < dataShards; i++ {
		shards[i] = randomFill(dataBlkSize)
		sum256 := sha256.Sum256(shards[i])
		hashes[i] = common.BytesToHash(sum256[:])
	}

	for i := dataShards; i < dataShards+parityShards; i++ {
		shards[i] = make([]byte, dataBlkSize)
	}

	return hashes, shards
}

func createData(dataShards, parityShards uint32) ([]common.Hash, [][]byte) {
	hashes, shards := createShards(dataShards, parityShards)
	enc, _ := reedsolomon.New(int(dataShards), int(parityShards))
	enc.Encode(shards)
	//update parity hash
	for i := dataShards; i < dataShards+parityShards; i++ {
		sum256 := sha256.Sum256(shards[i])
		hashes[i] = common.BytesToHash(sum256[:])
	}

	return hashes, shards
}

func initailP2PMockWithShards(hashes []common.Hash, shards [][]byte, delays ...time.Duration) (P2PNetwork, []P2PLocation) {
	locations := make([]P2PLocation, len(hashes))
	for i := 0; i < len(hashes); i++ {
		locations[i] = P2PLocation(common.BytesToAddress(hashes[i][:]))
	}

	p2p, _ := InititalP2PMock(locations, shards, delays...)
	return p2p, locations
}

func TestDataRecovery(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := ytfsOpt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := ytfs.Open(rootDir, config)

	recConfig := DefaultRecoveryOption()
	hashes, shards := createData(recConfig.DataShards, recConfig.ParityShards)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, shards)

	for i := 0; i < len(shards); i++ {
		fmt.Printf("Data[%d] = %x:%x\n", i, hashes[i], shards[i][:20])
	}

	dataRecoverEngine, err := NewDataRecoverEngine(yd, p2pNet, recConfig)
	if err != nil {
		t.Fail()
	}

	tdList := []*TaskDescription{}
	for i := 0; i < len(shards); i++ {
		td := &TaskDescription{
			uint64(i),
			hashes,
			p2pNodes,
			[]uint32{uint32(i)},
		}
		dataRecoverEngine.RecoverData(td)
		tdList = append(tdList, td)
	}

	time.Sleep(2 * time.Second)
	for _, td := range tdList {
		tdStatus := dataRecoverEngine.RecoverStatus(td)
		if tdStatus.Status != SuccessTask {
			t.Fatalf("ERROR: td status(%d): %s", tdStatus.Status, tdStatus.Desc)
		} else {
			data, err := yd.Get(ytfsCommon.IndexTableKey(td.Hashes[td.RecoverIDs[0]]))
			if err != nil || bytes.Compare(data, shards[td.RecoverIDs[0]]) != 0 {
				t.Fatalf("Error: err(%v), dataCompare (%d). hash(%v) data(%v) shards(%v)",
					err, bytes.Compare(data, shards[td.RecoverIDs[0]]),
					td.Hashes[td.RecoverIDs[0]],
					data[:20], shards[td.RecoverIDs[0]][:20])
			}
		}
	}
}

func TestMultiplyDataRecovery(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := ytfsOpt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := ytfs.Open(rootDir, config)

	recConfig := DefaultRecoveryOption()
	hashes, shards := createData(recConfig.DataShards, recConfig.ParityShards)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, shards)

	for i := 0; i < len(shards); i++ {
		fmt.Printf("Data[%d] = %x:%x\n", i, hashes[i], shards[i][:20])
	}

	dataRecoverEngine, err := NewDataRecoverEngine(yd, p2pNet, recConfig)
	if err != nil {
		t.Fail()
	}

	td := &TaskDescription{
		uint64(2),
		hashes,
		p2pNodes,
		[]uint32{0, 1, 2},
	}
	dataRecoverEngine.RecoverData(td)

	time.Sleep(2 * time.Second)
	tdStatus := dataRecoverEngine.RecoverStatus(td)
	if tdStatus.Status != SuccessTask {
		t.Fatalf("ERROR: td status(%d): %s", tdStatus.Status, tdStatus.Desc)
	} else {
		for i := 0; i < len(td.RecoverIDs); i++ {
			data, err := yd.Get(ytfsCommon.IndexTableKey(td.Hashes[td.RecoverIDs[i]]))
			if err != nil || bytes.Compare(data, shards[td.RecoverIDs[i]]) != 0 {
				t.Fatalf("Error: err(%v), dataCompare (%d). hash(%v) data(%v) shards(%v)",
					err, bytes.Compare(data, shards[td.RecoverIDs[i]]),
					td.Hashes[td.RecoverIDs[i]],
					data[:20], shards[td.RecoverIDs[i]][:20])
			}
		}
	}
}

func TestDataRecoveryError(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := ytfsOpt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := ytfs.Open(rootDir, config)

	recConfig := DefaultRecoveryOption()
	recConfig.TimeoutInMS = 10
	hashes, shards := createData(recConfig.DataShards, recConfig.ParityShards)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, shards)

	for i := 0; i < len(shards); i++ {
		fmt.Printf("Data[%d] = %x:%x\n", i, hashes[i], shards[i][:20])
	}

	dataRecoverEngine, err := NewDataRecoverEngine(yd, p2pNet, recConfig)
	if err != nil {
		t.Fail()
	}

	recIds := make([]uint32, recConfig.ParityShards+1)
	for i := 0; i < len(recIds); i++ {
		recIds[i] = uint32(i)
	}

	td := &TaskDescription{
		uint64(0),
		hashes,
		p2pNodes,
		recIds,
	}
	dataRecoverEngine.RecoverData(td)

	tdStatus := dataRecoverEngine.RecoverStatus(td)
	if tdStatus.Status != ErrorTask {
		t.Fatalf("ERROR: td status(%d): %s", tdStatus.Status, tdStatus.Desc)
	} else {
		t.Log("Expected error:", tdStatus)
	}

	td = &TaskDescription{
		uint64(1),
		hashes,
		p2pNodes,
		[]uint32{0},
	}
	dataRecoverEngine.RecoverData(td)
	time.Sleep(2 * time.Second)
	tdStatus = dataRecoverEngine.RecoverStatus(td)
	if tdStatus.Status != ErrorTask {
		t.Fatalf("ERROR: td status(%d): %s", tdStatus.Status, tdStatus.Desc)
	} else {
		t.Log("Expected error:", tdStatus)
	}
}

func setupBenchmarkEnv(recConfig *DataRecoverOptions, p2pDelays ...time.Duration) (*DataRecoverEngine, []common.Hash, []P2PLocation) {
	hashes, shards := createData(recConfig.DataShards, recConfig.ParityShards)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, shards, p2pDelays...)

	dataRecoverEngine, _ := NewDataRecoverEngine(nil, p2pNet, recConfig)
	return dataRecoverEngine, hashes, p2pNodes
}

func BenchmarkPureDataRecovery(b *testing.B) {
	var dataShards, parityShards uint32 = 5, 3
	_, shards := createData(dataShards, parityShards)
	rsEnc, err := reedsolomon.New(int(dataShards), int(parityShards))
	if err != nil {
		b.Fatal(err)
	}
	missID := rand.Int() % len(shards)
	shards[missID] = nil

	for n := 0; n < b.N; n++ {
		rsEnc.Reconstruct(shards)
	}
}

func BenchmarkFastP2PDataRecovery(b *testing.B) {
	recConfig := DefaultRecoveryOption()
	dataRecoverEngine, hashes, p2pNodes := setupBenchmarkEnv(recConfig, []time.Duration{250, 250, 250, 250, 250, 250, 250}...)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		done := make(chan interface{}, 1)
		td := &TaskDescription{
			uint64(rand.Int63()),
			hashes,
			p2pNodes,
			[]uint32{uint32(rand.Intn(len(hashes)))},
		}
		dataRecoverEngine.doRecoverData(td, done)
	}
}

func BenchmarkSlowP2PDataRecovery(b *testing.B) {
	recConfig := DefaultRecoveryOption()
	dataRecoverEngine, hashes, p2pNodes := setupBenchmarkEnv(recConfig, []time.Duration{25, 25, 25, 25, 25, 25, 25}...)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		done := make(chan interface{}, 1)
		td := &TaskDescription{
			uint64(rand.Int63()),
			hashes,
			p2pNodes,
			[]uint32{uint32(rand.Intn(len(hashes)))},
		}
		dataRecoverEngine.doRecoverData(td, done)
	}
}

func BenchmarkUnevenP2PDataRecovery(b *testing.B) {
	recConfig := DefaultRecoveryOption()
	dataRecoverEngine, hashes, p2pNodes := setupBenchmarkEnv(recConfig, []time.Duration{250, 211, 173, 136, 99, 62, 25}...)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		done := make(chan interface{}, 1)
		td := &TaskDescription{
			uint64(rand.Int63()),
			hashes,
			p2pNodes,
			[]uint32{uint32(rand.Intn(len(hashes)))},
		}
		dataRecoverEngine.doRecoverData(td, done)
	}
}
