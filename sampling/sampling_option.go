package sampling

// Config is the config of sampling engine
type Config struct {
	FrequenceInSecond uint32
	MaxSamplingThread uint32
	P2PTimeoutInMS    uint32
}

// DefaultOption is the default option of data sample engine
func DefaultOption() *Config {
	return &Config{
		1,
		1,
		500,
	}
}