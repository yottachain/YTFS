package recovery

import(
	// "fmt"
	// "bytes"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// P2PLocation is the address of the p2p network
type P2PLocation common.Address

// P2PNetwork interface defines the P2P operations and implements mock for test
type P2PNetwork interface {
	RetrieveData(peer P2PLocation, msgByte []byte) error
}

// P2PMock mocks a p2p network for test
type P2PMock struct{
	network map[P2PLocation][]byte
}

// RetrieveData get data from p2p network address
func (mock P2PMock) RetrieveData(peer P2PLocation, msgByte []byte) error {
	time.Sleep(50*time.Millisecond)
	copy(msgByte, mock.network[peer])
	return nil
}

// InititalP2PMock initializes P2P mock module
func InititalP2PMock(peers []P2PLocation, dataBlks [][]byte) (P2PNetwork, error) {
	p2p := P2PMock{
		map[P2PLocation][]byte{},
	}

	for i:=0;i<len(peers);i++{
		p2p.network[peers[i]]=dataBlks[i]
	}
	return p2p, nil
}


