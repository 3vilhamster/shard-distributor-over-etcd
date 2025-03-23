package config

import (
	"time"
)

// ServiceConfig defines configuration for the client service
type ServiceConfig struct {
	// ServerAddr is the address of the shard distributor server
	ServerAddr string

	// InstanceID is the unique identifier for this instance
	InstanceID string

	// ReconnectBackoff is the base time to wait before reconnecting
	ReconnectBackoff time.Duration

	// MaxReconnectBackoff is the maximum time to wait before reconnecting
	MaxReconnectBackoff time.Duration

	// ReconnectJitter is the jitter to add to reconnect backoff
	ReconnectJitter float64

	// HeartbeatInterval is the interval between heartbeats
	HeartbeatInterval time.Duration

	// HealthReportInterval is the interval between health reports
	HealthReportInterval time.Duration

	// ShardProcessorConfig is the configuration for the shard processor
	ShardProcessorConfig ProcessorConfig

	Namespaces []string

	ReconcileInterval time.Duration
}

// DefaultServiceConfig provides default configuration values
var DefaultServiceConfig = ServiceConfig{
	ReconnectBackoff:     time.Second,
	MaxReconnectBackoff:  30 * time.Second,
	ReconnectJitter:      0.2,
	HeartbeatInterval:    5 * time.Second,
	HealthReportInterval: 10 * time.Second,
	ShardProcessorConfig: DefaultProcessorConfig,
}
