package store

import (
	"context"
	"time"
)

// Assignment represents a shard assignment
type Assignment struct {
	OwnerID    string `json:"owner_id"`
	NewOwnerID string `json:"new_owner_id,omitempty"`
	State      string `json:"state"`
}

// ShardGroup represents a group of related shards
type ShardGroup struct {
	GroupID     string            `json:"group_id"`
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
	SaveShardAssignments(ctx context.Context, groupID string, assignments map[string]Assignment) error

	// GetShardAssignments retrieves all shard assignments
	GetShardAssignments(ctx context.Context, groupID string) (map[string]Assignment, error)

	// SaveShardGroup saves a workload group definition
	SaveShardGroup(ctx context.Context, groupID string, data ShardGroup) error

	// GetShardGroups retrieves all workload groups
	GetShardGroups(ctx context.Context) (map[string]ShardGroup, error)

	// GetShardGroup retrieves information about a single group
	GetShardGroup(ctx context.Context, groupID string) (ShardGroup, error)
}
