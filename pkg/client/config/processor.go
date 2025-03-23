package config

import "time"

// ProcessorConfig defines configuration for the shard processor
type ProcessorConfig struct {
	// MaxConcurrentTransfers is the maximum number of concurrent shard transfers
	MaxConcurrentTransfers int
	// ShardActivationTimeout is the maximum time to wait for a shard to activate
	ShardActivationTimeout time.Duration
	// ShardDeactivationTimeout is the maximum time to wait for a shard to deactivate
	ShardDeactivationTimeout time.Duration
	// AssignmentQueueSize is the size of the assignment queue
	AssignmentQueueSize int
}

// DefaultProcessorConfig provides default configuration values
var DefaultProcessorConfig = ProcessorConfig{
	MaxConcurrentTransfers:   5,
	ShardActivationTimeout:   30 * time.Second,
	ShardDeactivationTimeout: 30 * time.Second,
	AssignmentQueueSize:      100,
}
