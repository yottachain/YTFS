// Package recovery provides the rs lib to handle data recovery request.
package recovery

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/klauspost/reedsolomon"
	"github.com/yottachain/YTFS"
	ytfsCommon "github.com/yottachain/YTFS/common"
	ytfsOpt "github.com/yottachain/YTFS/opt"
)

// DataRecoverEngine the rs codec to recovery data
type DataRecoverEngine struct {
	recoveryEnc	reedsolomon.Encoder
	config      *DataCodecOptions
	ytfs        *ytfs.YTFS

	p2p         P2PNetwork

	taskList	[]*TaskDescription
	taskCh		chan *TaskDescription
	taskStatus  map[uint64]TaskResponse

	lock        sync.Mutex
}

// TaskDescription describes the recovery task.
type TaskDescription struct {
	// ID of this TaskDescription
	ID         uint64
	// [M+N] hashes, and nil indicates those missed
	Hashes	   []common.Hash
	// [M+N] locations, as Yotta keeps relative data in different location
	Locations  []P2PLocation
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

func (response ResponseCode) String() string{
	switch response{
	case SuccessTask    : return "SuccessTask"
	case ProcessingTask : return "ProcessingTask"
	case PendingTask    : return "PendingTask"
	case ErrorTask      : return "ErrorTask"
	default: return "UnkownStatus"
	}
}

// TaskResponse descirbes the status of the task 
type TaskResponse struct {
	Status ResponseCode
	Desc   string
}

// NewDataCodec creates recovery data codec
func NewDataCodec(ytfs *ytfs.YTFS, p2p P2PNetwork, opt *DataCodecOptions) (*DataRecoverEngine, error) {
	enc, error := reedsolomon.New(opt.DataShards, opt.ParityShards)
	if error != nil {
		return nil, error
	}

	maxTasks := opt.MaxTaskInParallel
	if maxTasks <= 0 || maxTasks > 8 {
		maxTasks = 8
	}

	codec := &DataRecoverEngine{
		enc,
		opt,
		ytfs,
		p2p,
		[]*TaskDescription{},
		make(chan *TaskDescription, maxTasks),
		map[uint64]TaskResponse{},
		sync.Mutex{},
	}

	go codec.startRecieveTask()
	return codec, nil
}

func (codec *DataRecoverEngine) startRecieveTask() {
	running := 0
	done := make(chan interface{})
	for ;; {
		// TODO: use numberred semiphone
		select {
		case td := <- codec.taskCh:
			codec.taskList = append(codec.taskList, td)
		case <- done:
			running--
		}

		for len(codec.taskList) != 0 &&  running < codec.config.MaxTaskInParallel {
			running++
			task := codec.taskList[0]
			codec.taskList = codec.taskList[1:]
			go codec.doRecoverData(task, done)
		}
	}
}

// RecoverData recieves a recovery task and start working later on
func (codec *DataRecoverEngine) RecoverData(td *TaskDescription) TaskResponse {
	err := codec.validateTask(td)
	if err != nil {
		codec.recordError(td, err)
		return codec.RecoverStatus(td)
	}

	// sequenced op on chan
	codec.taskCh <- td
	codec.recordTaskResponse(td, TaskResponse{PendingTask, "Task is pending"})
	return codec.RecoverStatus(td)
}

func (codec *DataRecoverEngine) validateTask(td *TaskDescription) error {
	// verify hash
	if len(td.RecoverIDs) > codec.config.ParityShards {
		return fmt.Errorf("Recovered data should be < ParityShards")
	}

	if len(td.Hashes) != codec.config.DataShards+codec.config.ParityShards {
		return fmt.Errorf("Input hashes length != DataShards+ParityShards")
	}

	// TODO: verify network
	return nil
}

func (codec *DataRecoverEngine) doRecoverData(td *TaskDescription, done chan interface{}) {
	if ytfsOpt.DebugPrint {
		for i:=0;i<len(td.RecoverIDs);i++{
			fmt.Printf("Recovery: start working on td(%d), recover hash = %x\n", td.ID, td.Hashes[td.RecoverIDs[i]])
		}
	}

	shards, err := codec.prepareDataShards(td)
	if err != nil {
		codec.recordError(td, err)
		return
	}

	codec.recordTaskResponse(td, TaskResponse{ProcessingTask, "EC recovering"})
	// Reconstruct the shards
	err = codec.recoveryEnc.Reconstruct(shards)
	if err != nil {
		codec.recordError(td, err)
		return
	}

	if codec.ytfs != nil {
		for i:=uint32(0);i<uint32(len(td.RecoverIDs));i++{
			err = codec.ytfs.Put(ytfsCommon.IndexTableKey(td.Hashes[td.RecoverIDs[i]]), shards[td.RecoverIDs[i]])
			if err != nil {
				codec.recordError(td, err)
				return
			}
		}
	}

	codec.recordTaskResponse(td, TaskResponse{SuccessTask, "Task Success"})
	done <- struct{}{}
}

func (codec *DataRecoverEngine) prepareDataShards(td *TaskDescription) ([][]byte, error) {
	recoverIndexSet := map[uint32]interface{}{}
	shards := make([][]byte, codec.config.DataShards+codec.config.ParityShards)
	for i:=uint32(0);i<uint32(len(td.RecoverIDs));i++{
		shards[td.RecoverIDs[i]] = nil
		recoverIndexSet[td.RecoverIDs[i]] = struct{}{}
	}

	type P2PDataReceiveResult struct {
		shardSliceID uint32
		data         []byte
	}
	resCh := make(chan *P2PDataReceiveResult, codec.config.DataShards)
	errCh := make(chan error, 1)
	//Stop those incompleted goroutine by using stopCh
	stopSigCh := make(chan interface{})
	for i:=0;i<len(td.Hashes);i++{
		if _, ok := recoverIndexSet[uint32(i)]; !ok {
			go func(shardID uint32) {
				hash, loc, timeout := td.Hashes[shardID], td.Locations[shardID], codec.config.TimeoutInMS
				data, err := codec.getShardFromNetwork(hash, loc, timeout, stopSigCh)
				if err == nil {
					resCh <- &P2PDataReceiveResult{shardID, data}
				} else {
					errCh <- err
				}
			}(uint32(i))
		}
	}
	codec.recordTaskResponse(td, TaskResponse{ProcessingTask, "Retrieving data from P2P network"})

	dataReceived := 0
	for ;; {
		select{
		case res := <-resCh:
			dataReceived++
			shards[res.shardSliceID] = res.data
		case err := <-errCh:
			codec.recordError(td, fmt.Errorf("ERROR: Retrieve data failed, error %v", err))
			close(stopSigCh)
			return nil, err
		}

		if dataReceived == codec.config.DataShards {
			close(stopSigCh)
			break
		}
	}

	return shards, nil
}

// RecoverStatus queries the status of a task
func (codec *DataRecoverEngine) RecoverStatus(td *TaskDescription) TaskResponse {
	codec.lock.Lock()
	defer codec.lock.Unlock()
	return codec.taskStatus[td.ID]
}

func (codec *DataRecoverEngine) recordError(td *TaskDescription, err error) {
	codec.recordTaskResponse(td, TaskResponse{ErrorTask, err.Error()})
}

func (codec *DataRecoverEngine) recordTaskResponse(td *TaskDescription, res TaskResponse) {
	//TODO: link to levelDB
	codec.lock.Lock()
	defer codec.lock.Unlock()
	codec.taskStatus[td.ID] = res
}

func (codec *DataRecoverEngine) getShardFromNetwork(hash common.Hash, loc P2PLocation, timeoutMS time.Duration, stopSigCh chan interface{}) ([]byte, error) {
	success := make(chan interface{})
	errCh := make(chan error)
	shard := make([]byte, codec.ytfs.Meta().DataBlockSize)
	go func() {
		//recieve data
		err := codec.retrieveData(loc, hash, shard)
		if err != nil {
			errCh <- err
		} else {
			success <- struct{}{}
		}
	}()

	select {
	case <- success:
		return shard, nil
	case err := <- errCh:
		return nil, err
	case <- time.After(timeoutMS*time.Millisecond):
		return nil, fmt.Errorf("Error: p2p get %x from %x timeout", hash, loc)
	case <- stopSigCh:
		return nil, nil
	}
}

func (codec *DataRecoverEngine) retrieveData(loc P2PLocation, hash common.Hash, data []byte) error {
	// Read p2p network
	codec.p2p.RetrieveData(loc, data)
	return nil
}