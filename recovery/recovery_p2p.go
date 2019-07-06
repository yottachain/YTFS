package recovery

import (
	// "fmt"
	// "bytes"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// P2PLocation is the address of the p2p network
type P2PLocation common.Address

// P2PNetwork interface defines the P2P operations and implements mock for test
type P2PNetwork interface {
	RetrieveData(peer P2PLocation, hash common.Hash) ([]byte, error)
}

// P2PMock mocks a p2p network for test
type P2PMock struct {
	networkData  map[P2PLocation][]byte
	networkDelay map[P2PLocation]time.Duration
}

const defaultNetworkDelayInMS time.Duration = 50

// RetrieveData get data from p2p network address
func (mock P2PMock) RetrieveData(peer P2PLocation, hash common.Hash) ([]byte, error) {
	if delay, ok := mock.networkDelay[peer]; ok {
		time.Sleep(delay * time.Millisecond)
	} else {
		time.Sleep(defaultNetworkDelayInMS * time.Millisecond)
	}
	msgByte := make([]byte, len(mock.networkData[peer]))
	copy(msgByte, mock.networkData[peer])
	return msgByte, nil
}

// NodeList reports all node addresses in this p2p network
func (mock P2PMock) NodeList() []P2PLocation {
	nodes := []P2PLocation{}
	for key := range mock.networkData {
		nodes = append(nodes, key)
	}
	return nodes
}

// InititalP2PMock initializes P2P mock module
func InititalP2PMock(peers []P2PLocation, dataBlks [][]byte, networkParams ...time.Duration) (P2PNetwork, error) {
	p2p := P2PMock{
		map[P2PLocation][]byte{},
		map[P2PLocation]time.Duration{},
	}

	for i := 0; i < len(peers); i++ {
		p2p.networkData[peers[i]] = dataBlks[i]
	}

	for i, delay := range networkParams {
		p2p.networkDelay[peers[i]] = delay
	}

	return p2p, nil
}
