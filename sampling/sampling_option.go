package sampling

import (
	"fmt"
)

// Config is the config of sampling engine
type Config struct {
	FrequenceInSecond uint32
	MaxSamplingThread uint32
	P2PTimeoutInMS    uint32
}

func (config Config) String() string {
	return fmt.Sprintf("\nFrequenceInSecond: %d second\nMaxSamplingThread: %d\nP2PTimeoutInMS: %d ms\n",
				config.FrequenceInSecond, config.MaxSamplingThread, config.P2PTimeoutInMS)
}

// DefaultOption is the default option of data sample engine
func DefaultOption() *Config {
	return &Config{
		1,
		1,
		500,
	}
}