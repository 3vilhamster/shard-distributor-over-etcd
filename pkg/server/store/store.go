package store

import (
	"context"
	"time"
)

// Assignment represents a shard assignment
type Assignment struct {
	Namespace  string `json:"namespace"`
	OwnerID    string `json:"owner_id"`
	NewOwnerID string `json:"new_owner_id,omitempty"`
	State      string `json:"state"`
}

// Namespace represents a group of related shards
type Namespace struct {
	Namespace   string            `json:"namespace"`
	ShardIDs    []string          `json:"shard_ids"`
	Description string            `json:"description"`
	Metadata    map[string]string `json:"metadata"`
}

// Store defines the interface for distributed storage
type Store interface {
	// SaveInstance saves an instance to the store
	SaveInstance(ctx context.Context, instanceID string, endpoint string, ttl time.Duration) error

	// GetInstances retrieves all instances from the store
	GetInstances(ctx context.Context) (map[string]string, error)

	// SaveShardAssignments saves shard assignments
	SaveShardAssignments(ctx context.Context, namespace string, assignments map[string]Assignment) error

	// GetShardAssignments retrieves all shard assignments
	GetShardAssignments(ctx context.Context, namespace string) (map[string]Assignment, error)

	// SaveNamespace saves a workload group definition
	SaveNamespace(ctx context.Context, namespace string, data Namespace) error

	// GetNamespaces retrieves all workload groups
	GetNamespaces(ctx context.Context) (map[string]Namespace, error)

	// GetNamespace retrieves information about a single group
	GetNamespace(ctx context.Context, namespace string) (Namespace, error)
}
