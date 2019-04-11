// Package recovery provides the rs lib to handle data recovery request.
package recovery

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/klauspost/reedsolomon"
	"github.com/yottachain/YTFS"
	ytfsCommon "github.com/yottachain/YTFS/common"
	ytfsOpt "github.com/yottachain/YTFS/opt"
)

// DataRecoverEngine the rs recoverEngine to recovery data
type DataRecoverEngine struct {
	recoveryEnc reedsolomon.Encoder
	config      *DataRecoverOptions
	ytfs        *ytfs.YTFS

	p2p P2PNetwork

	taskList   []*TaskDescription
	taskCh     chan *TaskDescription
	taskStatus map[uint64]TaskResponse

	lock sync.Mutex
}

// TaskDescription describes the recovery task.
type TaskDescription struct {
	// ID of this TaskDescription
	ID uint64
	// [M+N] hashes, and nil indicates those missed
	Hashes []common.Hash
	// [M+N] locations, as Yotta keeps relative data in different location
	Locations []P2PLocation
	// Index of recovery data in shards, should be obey the RS enc law
	RecoverIDs []uint32
}

// ResponseCode represent a task status.
type ResponseCode int

// task status code.
const (
	SuccessTask ResponseCode = iota
	ProcessingTask
	PendingTask
	ErrorTask
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
	default:
		return "UnkownStatus"
	}
}

// TaskResponse descirbes the status of the task
type TaskResponse struct {
	Status ResponseCode
	Desc   string
}

// NewDataRecoverEngine creates recovery data engine
func NewDataRecoverEngine(ytfs *ytfs.YTFS, p2p P2PNetwork, opt *DataRecoverOptions) (*DataRecoverEngine, error) {
	enc, error := reedsolomon.New(int(opt.DataShards), int(opt.ParityShards))
	if error != nil {
		return nil, error
	}

	maxTasks := opt.MaxTaskInParallel
	if maxTasks > uint32(runtime.NumCPU()) {
		maxTasks = uint32(runtime.NumCPU())
	}

	recoverEngine := &DataRecoverEngine{
		enc,
		opt,
		ytfs,
		p2p,
		[]*TaskDescription{},
		make(chan *TaskDescription, maxTasks),
		map[uint64]TaskResponse{},
		sync.Mutex{},
	}

	go recoverEngine.startRecieveTask()
	return recoverEngine, nil
}

func (recoverEngine *DataRecoverEngine) startRecieveTask() {
	running := uint32(0)
	done := make(chan interface{})
	for {
		select {
		case td := <-recoverEngine.taskCh:
			recoverEngine.taskList = append(recoverEngine.taskList, td)
		case <-done:
			running--
		}

		for len(recoverEngine.taskList) != 0 && running < recoverEngine.config.MaxTaskInParallel {
			running++
			task := recoverEngine.taskList[0]
			recoverEngine.taskList = recoverEngine.taskList[1:]
			go recoverEngine.doRecoverData(task, done)
		}
	}
}

// RecoverData recieves a recovery task and start working later on
func (recoverEngine *DataRecoverEngine) RecoverData(td *TaskDescription, successCB ...func() TaskResponse) TaskResponse {
	err := recoverEngine.validateTask(td)
	if err != nil {
		recoverEngine.recordError(td, err)
		return recoverEngine.RecoverStatus(td)
	}

	// sequenced op on chan
	recoverEngine.taskCh <- td
	recoverEngine.recordTaskResponse(td, TaskResponse{PendingTask, "Task is pending"})
	return recoverEngine.RecoverStatus(td)
}

func (recoverEngine *DataRecoverEngine) validateTask(td *TaskDescription) error {
	// verify hash
	if len(td.RecoverIDs) > int(recoverEngine.config.ParityShards) {
		return fmt.Errorf("Recovered data should be < ParityShards")
	}

	if len(td.Hashes) != int(recoverEngine.config.DataShards+recoverEngine.config.ParityShards) {
		return fmt.Errorf("Input hashes length != DataShards+ParityShards")
	}

	// TODO: verify network
	return nil
}

func (recoverEngine *DataRecoverEngine) doRecoverData(td *TaskDescription, done chan interface{}) {
	if ytfsOpt.DebugPrint {
		for i := 0; i < len(td.RecoverIDs); i++ {
			fmt.Printf("Recovery: start working on td(%d), recover hash = %x\n", td.ID, td.Hashes[td.RecoverIDs[i]])
		}
	}

	shards, err := recoverEngine.prepareDataShards(td)
	if err != nil {
		recoverEngine.recordError(td, err)
		return
	}

	recoverEngine.recordTaskResponse(td, TaskResponse{ProcessingTask, "EC recovering"})
	// Reconstruct the shards
	err = recoverEngine.recoveryEnc.Reconstruct(shards)
	if err != nil {
		recoverEngine.recordError(td, err)
		return
	}

	if recoverEngine.ytfs != nil {
		for i := uint32(0); i < uint32(len(td.RecoverIDs)); i++ {
			err = recoverEngine.ytfs.Put(ytfsCommon.IndexTableKey(td.Hashes[td.RecoverIDs[i]]), shards[td.RecoverIDs[i]])
			if err != nil {
				recoverEngine.recordError(td, err)
				return
			}
		}
	}

	recoverEngine.recordTaskResponse(td, TaskResponse{SuccessTask, "Task Success"})
	done <- struct{}{}
}

func (recoverEngine *DataRecoverEngine) prepareDataShards(td *TaskDescription) ([][]byte, error) {
	recoverIndexSet := map[uint32]interface{}{}
	shards := make([][]byte, recoverEngine.config.DataShards+recoverEngine.config.ParityShards)
	for i := uint32(0); i < uint32(len(td.RecoverIDs)); i++ {
		shards[td.RecoverIDs[i]] = nil
		recoverIndexSet[td.RecoverIDs[i]] = struct{}{}
	}

	type P2PDataReceiveResult struct {
		shardSliceID uint32
		data         []byte
	}
	resCh := make(chan *P2PDataReceiveResult, recoverEngine.config.DataShards)
	errCh := make(chan error, 1)
	//Stop those incompleted goroutine by using stopCh
	stopSigCh := make(chan interface{})
	for i := 0; i < len(td.Hashes); i++ {
		if _, ok := recoverIndexSet[uint32(i)]; !ok {
			go func(shardID uint32) {
				hash, loc, timeout := td.Hashes[shardID], td.Locations[shardID], recoverEngine.config.TimeoutInMS
				data, err := recoverEngine.getShardFromNetwork(hash, loc, timeout, stopSigCh)
				if err == nil {
					resCh <- &P2PDataReceiveResult{shardID, data}
				} else {
					errCh <- err
				}
			}(uint32(i))
		}
	}
	recoverEngine.recordTaskResponse(td, TaskResponse{ProcessingTask, "Retrieving data from P2P network"})

	dataReceived := uint32(0)
	for {
		select {
		case res := <-resCh:
			dataReceived++
			shards[res.shardSliceID] = res.data
		case err := <-errCh:
			recoverEngine.recordError(td, fmt.Errorf("ERROR: Retrieve data failed, error %v", err))
			close(stopSigCh)
			return nil, err
		}

		if dataReceived == recoverEngine.config.DataShards {
			close(stopSigCh)
			break
		}
	}

	return shards, nil
}

// RecoverStatus queries the status of a task
func (recoverEngine *DataRecoverEngine) RecoverStatus(td *TaskDescription) TaskResponse {
	recoverEngine.lock.Lock()
	defer recoverEngine.lock.Unlock()
	return recoverEngine.taskStatus[td.ID]
}

func (recoverEngine *DataRecoverEngine) recordError(td *TaskDescription, err error) {
	recoverEngine.recordTaskResponse(td, TaskResponse{ErrorTask, err.Error()})
}

func (recoverEngine *DataRecoverEngine) recordTaskResponse(td *TaskDescription, res TaskResponse) {
	//TODO: link to levelDB
	recoverEngine.lock.Lock()
	defer recoverEngine.lock.Unlock()
	recoverEngine.taskStatus[td.ID] = res
}

func (recoverEngine *DataRecoverEngine) getShardFromNetwork(hash common.Hash, loc P2PLocation, timeoutMS time.Duration, stopSigCh chan interface{}) ([]byte, error) {
	success := make(chan interface{})
	errCh := make(chan error)
	go func() {
		//recieve data
		shard, err := recoverEngine.retrieveData(loc, hash)
		if err != nil {
			errCh <- err
		} else {
			success <- shard
		}
	}()

	select {
	case shard := <-success:
		return shard.([]byte), nil
	case err := <-errCh:
		return nil, err
	case <-time.After(timeoutMS * time.Millisecond):
		return nil, fmt.Errorf("Error: p2p get %x from %x timeout", hash, loc)
	case <-stopSigCh:
		return nil, nil
	}
}

func (recoverEngine *DataRecoverEngine) retrieveData(loc P2PLocation, hash common.Hash) ([]byte, error) {
	// Read p2p network
	data, _ := recoverEngine.p2p.RetrieveData(loc, hash)
	return data, nil
}
