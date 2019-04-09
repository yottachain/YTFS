package recovery

import (
	"time"
)

// DataCodecOptions describes the options of recovery module
type DataCodecOptions struct {
	DataShards        int
	ParityShards      int
	MaxTaskInParallel int
	TimeoutInMS       time.Duration
}

// DefaultRecoveryOption gives the default data recovery codec config
func DefaultRecoveryOption() *DataCodecOptions {
	return &DataCodecOptions{
		3,
		4,
		12,
		5000,
	}
}
