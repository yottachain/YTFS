package recovery

import (
	"time"
)

// DataRecoverOptions describes the options of recovery module
type DataRecoverOptions struct {
	DataShards        uint32
	ParityShards      uint32
	MaxTaskInParallel uint32
	TimeoutInMS       time.Duration
}

// DefaultRecoveryOption gives the default data recovery engine config
func DefaultRecoveryOption() *DataRecoverOptions {
	return &DataRecoverOptions{
		3,
		4,
		12,
		5000,
	}
}
