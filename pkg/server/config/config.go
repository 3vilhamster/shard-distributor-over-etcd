package config

import "time"

// Config represents the configuration for the server
type Config struct {
	EtcdEndpoints        []string
	ListenAddr           string
	LeaderElectionPath   string
	ShardCount           int
	ReconcileInterval    time.Duration
	HealthCheckInterval  time.Duration
	HealthPort           int
	LeaderElectionPrefix string
}
