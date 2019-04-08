// Package recovery
package recovery

import (
	"bytes"
	"io/ioutil"
	"crypto/sha256"
	"math/rand"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/ethereum/go-ethereum/common"
	"github.com/yottachain/YTFS"
	ytfsOpt "github.com/yottachain/YTFS/opt"
	ytfsCommon "github.com/yottachain/YTFS/common"
)

func TestNewDataRecovery(t *testing.T) {
	_, err := NewDataCodec(nil, nil, DefaultRecoveryOption())
	if err != nil {
		t.Fail()
	}
}

func randomFill(size uint32) []byte {
	buf := make([]byte, size, size)
	rand.Read(buf)
	return buf
}

func createShards(dataShards, parityShards int) ([]common.Hash, [][]byte) {
	shards := make([][]byte, dataShards + parityShards)
	hashes := make([]common.Hash, dataShards + parityShards)
	dataBlkSize := ytfsOpt.DefaultOptions().DataBlockSize
	for i:=0;i<dataShards;i++{
		shards[i] = randomFill(dataBlkSize)
		sum256 := sha256.Sum256(shards[i])
		hashes[i] = common.BytesToHash(sum256[:])
	}

	for i:=dataShards;i<dataShards+parityShards;i++{
		shards[i] = make([]byte, dataBlkSize)
	}

	return hashes,shards
}

func createAndDistributeData(dataShards, parityShards int) (P2PNetwork, []P2PLocation, []common.Hash, [][]byte) {
	hashes, shards := createShards(dataShards, parityShards)
	locations := make([]P2PLocation, len(hashes))
	enc, _ := reedsolomon.New(dataShards, parityShards)
	enc.Encode(shards)
	//update parity hash
	for i:=dataShards;i<dataShards+parityShards;i++{
		sum256 := sha256.Sum256(shards[i])
		hashes[i] = common.BytesToHash(sum256[:])
	}

	for i:=0;i<dataShards+parityShards;i++{
		locations[i] = P2PLocation(common.BytesToAddress(hashes[i][:]))
	}

	p2p, _ := InititalP2PMock(locations, shards)
	return p2p, locations, hashes, shards
}

func TestDataRecovery(t *testing.T) {
	rootDir, err := ioutil.TempDir("/tmp", "ytfsTest")
	config := ytfsOpt.DefaultOptions()
	// defer os.Remove(config.StorageName)

	yd, err := ytfs.Open(rootDir, config)

	recConfig := DefaultRecoveryOption()
	p2p, locs, hashes, shards := createAndDistributeData(recConfig.DataShards, recConfig.ParityShards)

	codec, err := NewDataCodec(yd, p2p, recConfig)
	if err != nil {
		t.Fail()
	}

	tdList := []TaskDescription{}
	for i:=0;i<1;i++{
// for i:=0;i<len(shards);i++{
		recoverHashes := append([]common.Hash{}, hashes[:i]...)
		recoverHashes = append(recoverHashes, common.Hash{})
		recoverHashes = append(recoverHashes, hashes[i+1:]...)
		td := TaskDescription{
			uint64(i),
			recoverHashes,
			locs,
			uint32(i),
		}
		codec.RecoverData(td)
		tdList = append(tdList, td)
	}

	time.Sleep(10*time.Second)
	for _,td := range tdList{
		tdStatus := codec.RecoverStatus(td)
		if tdStatus.Status != SuccessTask {
			t.Fatalf("ERROR: td status(%d): %s", tdStatus.Status, tdStatus.Desc)
		} else {
			data, err := yd.Get(ytfsCommon.IndexTableKey(td.Hashes[td.Index]))
			if err != nil || bytes.Compare(data, shards[td.Index]) != 0 {
				t.Fatalf("Error: err(%v), dataCompare (%d). data(%v) shards(%v)",
				err, bytes.Compare(data, shards[td.Index]), data[:16], shards[td.Index][:16])
			}
		}
	}
}