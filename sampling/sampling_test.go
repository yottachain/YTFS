package sampling

import (
	"crypto/sha256"
	"math/rand"
	"time"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/recovery"
)

func TestNewEngine(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()
	_, err := NewEngine(nil, config)
	if err != nil {
		t.Fail()
	}
	time.Sleep(3*time.Second)
}

func randomFill(size uint32) []byte {
	buf := make([]byte, size, size)
	head := make([]byte, 16, 16)
	rand.Seed(int64(time.Now().Nanosecond()))
	rand.Read(head)
	copy(buf, head)
	return buf
}

func createData(n int) ([]common.Hash, [][]byte) {
	data := make([][]byte, n)
	hashes := make([]common.Hash, n)
	for i := 0; i < n; i++ {
		data[i] = randomFill(32768)
		sum256 := sha256.Sum256(data[i])
		hashes[i] = common.BytesToHash(sum256[:])
	}

	return hashes, data
}

func initailP2PMockWithShards(hashes []common.Hash, data [][]byte, delays ...time.Duration) (recovery.P2PNetwork, []recovery.P2PLocation) {
	locations := make([]recovery.P2PLocation, len(hashes))
	for i := 0; i < len(hashes); i++ {
		locations[i] = recovery.P2PLocation(common.BytesToAddress(hashes[i][:]))
	}

	p2p, _ := recovery.InititalP2PMock(locations, data, delays...)
	return p2p, locations
}

func TestSampleP2P(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()

	n := 2
	hashes, data := createData(n)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, data)

	se, err := NewEngine(p2pNet, config)
	if err != nil {
		t.Fail()
	}

	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		se.RequestSampling(task)
	}

	time.Sleep(3*time.Second)
	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		if se.ReportTaskStatus(task).Status != SuccessTask {
			t.Fatal(i, "test failed:", se.ReportTaskStatus(task).Desc)
		}
	}
}

func TestParallelP2P(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()
	config.MaxSamplingThread = 7
	n := 20
	hashes, data := createData(n)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, data)

	se, err := NewEngine(p2pNet, config)
	if err != nil {
		t.Fail()
	}

	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		se.RequestSampling(task)
	}

	time.Sleep(4*time.Second)
	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		if se.ReportTaskStatus(task).Status != SuccessTask {
			t.Fatal(i, "test failed:", se.ReportTaskStatus(task).Desc)
		}
	}
}

func TestSampleTaskInQueue(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()
	config.MaxSamplingThread = 1
	config.FrequenceInSecond = 10
	n := 20
	hashes, data := createData(n)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, data)

	se, err := NewEngine(p2pNet, config)
	if err != nil {
		t.Fail()
	}

	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		se.RequestSampling(task)
	}

	time.Sleep(4*time.Second)
	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		if se.ReportTaskStatus(task).Status != PendingTask {
			t.Fatal(i, "test failed:", se.ReportTaskStatus(task).Desc)
		}
	}
}

func TestStatusError(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()
	config.P2PTimeoutInMS = 300
	n := 1
	hashes, data := createData(n)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, data, 500)

	se, err := NewEngine(p2pNet, config)
	if err != nil {
		t.Fail()
	}

	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		se.RequestSampling(task)
	}

	time.Sleep(2*time.Second)
	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[i],
			p2pNodes[i],
		}
		if se.ReportTaskStatus(task).Status != ErrorTask {
			t.Fatal(i, "test failed:", se.ReportTaskStatus(task).Desc)
		} else {
			t.Log("Expected:", se.ReportTaskStatus(task))
		}
	}
}

func TestTaskFanIn(t *testing.T) {
	opt.DebugPrint = true
	config := DefaultOption()
	config.FrequenceInSecond = 300
	n := 10000
	hashes, data := createData(1)
	p2pNet, p2pNodes := initailP2PMockWithShards(hashes, data, 500)

	se, err := NewEngine(p2pNet, config)
	if err != nil {
		t.Fail()
	}

	for i:=0;i<n;i++{
		task := &TaskDescription{
			uint64(i),
			hashes[0],
			p2pNodes[0],
		}
		se.RequestSampling(task)
	}

	t.Log(se)
}