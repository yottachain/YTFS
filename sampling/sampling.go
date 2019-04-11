// Package sampling holds logic of sampling test of YTFS
package sampling

import (
	"bytes"
	"crypto/sha256"
	"sync"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/yottachain/YTFS/recovery"
	"github.com/yottachain/YTFS/opt"

)

// ResponseCode represent a task status.
type ResponseCode int

// task status code.
const (
	SuccessTask ResponseCode = iota
	ProcessingTask
	PendingTask
	ErrorTask
	UnknownTask
)

func (response ResponseCode) String() string {
	switch response {
	case SuccessTask:
		return "SuccessTask"
	case ProcessingTask:
		return "ProcessingTask"
	case PendingTask:
		return "PendingTask"
	case ErrorTask:
		return "ErrorTask"
	case UnknownTask:
		fallthrough
	default:
		return "UnkownStatus"
	}
}

// TaskDescription is the description of sampling task
type TaskDescription struct {
	ID      uint64
	Hash    common.Hash
	Address recovery.P2PLocation
}

// TaskRespond is the response of the task
type TaskRespond struct {
	TaskID uint64
	Status ResponseCode
	Desc   string
}

// DataSampleEngine is the sampling task list manager
type DataSampleEngine struct {
	config     *Config
	p2p        recovery.P2PNetwork
	taskList   []*TaskDescription
	taskCh     chan *TaskDescription
	taskStatus map[uint64]TaskRespond
	lock       sync.Mutex
}

// NewEngine creates a new sampling engine
func NewEngine(p2p recovery.P2PNetwork, config *Config) (*DataSampleEngine, error) {
	engine := &DataSampleEngine{
		config,
		p2p,
		[]*TaskDescription{},
		make(chan *TaskDescription),
		map[uint64]TaskRespond{},
		sync.Mutex{},
	}

	go engine.startTaskHandling()
	return engine, nil
}

// RequestSampling is the interface of others to give in a sampling task
func (engine *DataSampleEngine) RequestSampling(task *TaskDescription) error {
	if !engine.validateTask(task) {
		return fmt.Errorf("ERROR: incorrect sample request %v", task)
	}
	// sequenced op on chan
	engine.taskCh <- task
	engine.recordTaskResponse(task, PendingTask, "Task pending")
	return nil
}

// ReportTaskStatus reports task status
func (engine *DataSampleEngine) ReportTaskStatus(task *TaskDescription) TaskRespond {
	engine.lock.Lock()
	defer engine.lock.Unlock()
	if res, ok := engine.taskStatus[task.ID]; ok {
		return res
	}

	return TaskRespond{task.ID, UnknownTask, UnknownTask.String()}
}

func (engine *DataSampleEngine) validateTask(task *TaskDescription) bool {
	return true
} 

func (engine *DataSampleEngine) startTaskHandling() {
	done := make(chan interface{})
	for {
		select {
		case td := <-engine.taskCh:
			engine.taskList = append(engine.taskList, td)
		case <-time.After(time.Duration(engine.config.FrequenceInSecond)*time.Second):
			if opt.DebugPrint {
				fmt.Println("Time to sample")
			}
			running := uint32(0)
			for len(engine.taskList) != 0 && running < engine.config.MaxSamplingThread {
				running++
				task := engine.taskList[0]
				engine.taskList = engine.taskList[1:]
				go engine.doSampling(task, done)
			}
		}
	}
}

func (engine *DataSampleEngine) doSampling(td *TaskDescription, done chan interface{}) {
	dataCh := make(chan []byte)
	errCh := make(chan error)
	go func() {
		sample, err := engine.p2p.RetrieveData(td.Address, td.Hash)
		if err != nil {
			errCh <- err
		} else {
			dataCh <- sample
		}
	}()

	select {
	case sample := <- dataCh:
		err := engine.checkDataConsistence(td.Hash, sample)
		if err != nil {
			engine.recordTaskResponse(td, ErrorTask, err.Error())
		} else {
			engine.recordTaskResponse(td, SuccessTask, "Sample check passed")
		}
	case err := <- errCh:
		engine.recordTaskResponse(td, ErrorTask, err.Error())
	case <- time.After(time.Duration(engine.config.P2PTimeoutInMS)*time.Millisecond):
		engine.recordTaskResponse(td, ErrorTask, "P2P transfer timeout")
	}
}

func (engine *DataSampleEngine) checkDataConsistence(hash common.Hash, data []byte) error {
	sum256 := sha256.Sum256(data)
	dataHash := common.BytesToHash(sum256[:])
	if bytes.Compare(dataHash[:], hash[:]) == 0 {
		return nil
	}
	return fmt.Errorf("Data hash mismatch %x vs %x of data %x", dataHash, hash, data[:20])
}

func (engine *DataSampleEngine) recordTaskResponse(task *TaskDescription, res ResponseCode, desc string) {
	engine.lock.Lock()
	defer engine.lock.Unlock()

	engine.taskStatus[task.ID] = TaskRespond{
		task.ID,
		res,
		desc,
	}
}