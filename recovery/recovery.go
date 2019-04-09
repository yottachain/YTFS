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

	taskList	[]TaskDescription
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
		[]TaskDescription{},
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
			codec.taskList = append(codec.taskList, *td)
		case <- done:
			running--
		}

		for len(codec.taskList) != 0 &&  running < codec.config.MaxTaskInParallel {
			running++
			task := codec.taskList[0]
			codec.taskList = codec.taskList[1:]
			codec.recordTaskResponse(task, TaskResponse{PendingTask, ""})
			go codec.doRecoverData(task, done)
		}
	}
}

// RecoverData recieves a recovery task and start working later on
func (codec *DataRecoverEngine) RecoverData(td TaskDescription) TaskResponse {
	err := codec.validateTask(td)
	if err != nil {
		return TaskResponse{ErrorTask, err.Error()}
	}

	// sequenced op on chan
	codec.taskCh <- &td

	return codec.RecoverStatus(td)
}

func (codec *DataRecoverEngine) validateTask(td TaskDescription) error {
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

func (codec *DataRecoverEngine) doRecoverData(td TaskDescription, done chan interface{}) {
	shardReady := make(chan interface{})
	timeoutCh := make(chan common.Hash)

	if ytfsOpt.DebugPrint {
		for i:=0;i<len(td.RecoverIDs);i++{
			fmt.Printf("Recovery: start working on td(%d), recover hash = %v\n", td.ID, td.Hashes[td.RecoverIDs[i]])
		}
	}

	//TODO: simplify the shards initialization
	shards := make([][]byte, codec.config.DataShards+codec.config.ParityShards)
	for i:=uint32(0);i<uint32(len(shards));i++{
		shards[i] = make([]byte, codec.ytfs.Meta().DataBlockSize)
	}
	for i:=uint32(0);i<uint32(len(td.RecoverIDs));i++{
		shards[i] = nil
	}

	for i:=0;i<len(td.Hashes);i++{
		if shards[i] != nil {
			go codec.getShardFromNetwork(td.Hashes[i], td.Locations[i], shards[i], codec.config.TimeoutInMS, shardReady, timeoutCh)
		}
	}

	codec.recordTaskResponse(td, TaskResponse{ProcessingTask, "Retrieve data from P2P network"})

	dataReceived := 0
	for ;; {
		select{
		case <-shardReady:
			dataReceived++
		case hash := <-timeoutCh:
			codec.recordError(td, fmt.Errorf("Retrieve %x data timeout", hash))
			return
		}

		if dataReceived == codec.config.DataShards {
			break
		}
	}

	codec.recordTaskResponse(td, TaskResponse{ProcessingTask, "EC recovering"})
	// Reconstruct the shards
	err := codec.recoveryEnc.Reconstruct(shards)
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

	codec.recordTaskResponse(td, TaskResponse{SuccessTask, ""})
	done <- struct{}{}
}

// RecoverStatus queries the status of a task
func (codec *DataRecoverEngine) RecoverStatus(td TaskDescription) TaskResponse {
	codec.lock.Lock()
	defer codec.lock.Unlock()
	return codec.taskStatus[td.ID]
}

func (codec *DataRecoverEngine) recordError(td TaskDescription, err error) {
	codec.recordTaskResponse(td, TaskResponse{ErrorTask, err.Error()})
}

func (codec *DataRecoverEngine) recordTaskResponse(td TaskDescription, res TaskResponse) {
	//TODO: link to levelDB
	codec.lock.Lock()
	defer codec.lock.Unlock()
	codec.taskStatus[td.ID] = res
}

func (codec *DataRecoverEngine) getShardFromNetwork(hash common.Hash, loc P2PLocation,
						shard []byte, timeoutMS time.Duration,
						shardReady chan interface{}, timeoutCh chan common.Hash) {
	success := make(chan interface{})
	go func() {
		//recieve data
		codec.retrieveData(loc, hash, shard)
		success <- struct{}{}
	}()

	select {
	case <- success:
		shardReady <- struct{}{}
	case <- time.After(timeoutMS*time.Millisecond):
		timeoutCh <- hash
	}
}

func (codec *DataRecoverEngine) retrieveData(loc P2PLocation, hash common.Hash, data []byte) error {
	// Read p2p network
	// time.Sleep(30*time.Millisecond)
	codec.p2p.RetrieveData(loc, data)
	return nil
}