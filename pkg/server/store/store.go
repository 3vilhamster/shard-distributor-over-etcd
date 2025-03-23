package store

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EventType represents the type of watch event
type EventType int

const (
	// EventTypePut is for key creation or update
	EventTypePut EventType = iota
	// EventTypeDelete is for key deletion
	EventTypeDelete
)

// WatchEvent represents a change in the store
type WatchEvent struct {
	// Type is the type of event (put or delete)
	Type EventType
	// Key is the key that changed
	Key string
	// Value is the new value (empty for delete events)
	Value string
	// Timestamp is when the event occurred
	Timestamp time.Time
}

// WatchCallback is called when a watch event occurs
type WatchCallback func(event WatchEvent)

// Store defines the interface for distributed storage
type Store interface {
	// SaveInstance saves an instance to the store
	SaveInstance(ctx context.Context, instanceID string, endpoint string, leaseID clientv3.LeaseID) error

	// DeleteInstance removes an instance from the store
	DeleteInstance(ctx context.Context, instanceID string) error

	// GetInstances retrieves all instances from the store
	GetInstances(ctx context.Context) (map[string]string, error)

	// SaveShardAssignments saves shard assignments
	SaveShardAssignments(ctx context.Context, assignments map[string]string) error

	// GetShardAssignments retrieves all shard assignments
	GetShardAssignments(ctx context.Context) (map[string]string, error)

	// SaveShardVersion saves a shard version
	SaveShardVersion(ctx context.Context, shardID string, version int64) error

	// GetShardVersion retrieves a shard version
	GetShardVersion(ctx context.Context, shardID string) (int64, error)

	// GetGlobalVersion retrieves the global version
	GetGlobalVersion(ctx context.Context) (int64, error)

	// IncrementGlobalVersion atomically increments the global version
	IncrementGlobalVersion(ctx context.Context) (int64, error)

	// InitializeGlobalVersion initializes the global version if it doesn't exist
	InitializeGlobalVersion(ctx context.Context) error

	// SaveShardGroup saves a workload group definition
	SaveShardGroup(ctx context.Context, groupID string, data string) error

	// GetShardGroups retrieves all workload groups
	GetShardGroups(ctx context.Context) (map[string]string, error)

	// DeleteShardGroup removes a workload group definition
	DeleteShardGroup(ctx context.Context, groupID string) error

	// WatchInstances sets up a watch for instance changes
	WatchInstances(ctx context.Context, callback WatchCallback) (CancelFunc, error)

	// WatchShardAssignments sets up a watch for shard assignment changes
	WatchShardAssignments(ctx context.Context, callback WatchCallback) (CancelFunc, error)

	// WatchGlobalVersion sets up a watch for global version changes
	WatchGlobalVersion(ctx context.Context, callback WatchCallback) (CancelFunc, error)

	// WatchShardGroup sets up a watch for a specific shard group
	WatchShardGroup(ctx context.Context, groupID string, callback WatchCallback) (CancelFunc, error)

	// WatchShardGroups sets up a watch for all shard groups
	WatchShardGroups(ctx context.Context, callback WatchCallback) (CancelFunc, error)

	// CreateLease creates a new lease with the specified TTL
	CreateLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error)

	// RevokeLease revokes a lease
	RevokeLease(ctx context.Context, leaseID clientv3.LeaseID) error

	// KeepAliveLease keeps a lease alive once
	KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) error
}

// CancelFunc is a function that cancels a watch
type CancelFunc func()
