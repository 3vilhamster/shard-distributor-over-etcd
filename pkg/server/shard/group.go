package shard

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
)

// Group represents a group of shards for a specific workload type
type Group struct {
	GroupID     string            `json:"group_id"`
	Description string            `json:"description"`
	ShardIDs    []string          `json:"shard_ids"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`

	// Runtime state (not serialized)
	Stats GroupStats `json:"-"`
}

// GroupStats contains runtime statistics for a shard group
type GroupStats struct {
	AssignedCount      int
	UnassignedCount    int
	InstanceCount      int
	TransferCount      int
	LastTransferTime   time.Time
	AverageTransferMs  int64
	FailedTransfers    int
	ReconciliationTime time.Time
}

// NewShardGroup creates a new shard group with generated shard IDs
func NewShardGroup(groupID string, shardCount int) *Group {
	now := time.Now()

	shardIDs := make([]string, shardCount)
	for i := 0; i < shardCount; i++ {
		shardIDs[i] = fmt.Sprintf("%s-shard-%d", groupID, i)
	}

	return &Group{
		GroupID:     groupID,
		Description: fmt.Sprintf("Shard group for %s", groupID),
		ShardIDs:    shardIDs,
		Metadata:    make(map[string]string),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// WithDescription sets the description
func (g *Group) WithDescription(description string) *Group {
	g.Description = description
	g.UpdatedAt = time.Now()
	return g
}

// WithMetadata adds metadata
func (g *Group) WithMetadata(key, value string) *Group {
	g.Metadata[key] = value
	g.UpdatedAt = time.Now()
	return g
}

// AddShards adds additional shards to the group
func (g *Group) AddShards(count int) {
	startIdx := len(g.ShardIDs)
	for i := 0; i < count; i++ {
		g.ShardIDs = append(g.ShardIDs, fmt.Sprintf("%s-shard-%d", g.GroupID, startIdx+i))
	}
	g.UpdatedAt = time.Now()
}

// RemoveShards removes shards from the end of the group
func (g *Group) RemoveShards(count int) error {
	if count <= 0 {
		return fmt.Errorf("count must be positive")
	}

	if count > len(g.ShardIDs) {
		return fmt.Errorf("cannot remove more shards than exist")
	}

	g.ShardIDs = g.ShardIDs[:len(g.ShardIDs)-count]
	g.UpdatedAt = time.Now()
	return nil
}

// UpdateStatsWith updates the stats based on assignments
func (g *Group) UpdateStatsWith(
	assignments map[string]string,
	instances map[string]bool,
) {
	g.Stats.AssignedCount = 0
	g.Stats.UnassignedCount = 0
	g.Stats.InstanceCount = len(instances)

	// Instance distribution (how many shards per instance)
	instanceShards := make(map[string]int)

	for _, shardID := range g.ShardIDs {
		instanceID, exists := assignments[shardID]
		if exists && instanceID != "" {
			g.Stats.AssignedCount++
			instanceShards[instanceID]++
		} else {
			g.Stats.UnassignedCount++
		}
	}
}

// Serialize converts the group to a JSON string
func (g *Group) Serialize() (string, error) {
	data, err := json.Marshal(g)
	if err != nil {
		return "", fmt.Errorf("failed to marshal shard group: %w", err)
	}
	return string(data), nil
}

// DeserializeShardGroup creates a ShardGroup from a JSON string
func DeserializeShardGroup(data string) (*Group, error) {
	var group Group
	err := json.Unmarshal([]byte(data), &group)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal shard group: %w", err)
	}
	return &group, nil
}

// IsShardInGroup checks if a shard ID belongs to this group
func (g *Group) IsShardInGroup(shardID string) bool {
	for _, id := range g.ShardIDs {
		if id == shardID {
			return true
		}
	}
	return false
}

// GetShardCount returns the number of shards in this group
func (g *Group) GetShardCount() int {
	return len(g.ShardIDs)
}

// GetAssignmentPercentage returns the percentage of assigned shards
func (g *Group) GetAssignmentPercentage() float64 {
	if len(g.ShardIDs) == 0 {
		return 0
	}
	return float64(g.Stats.AssignedCount) / float64(len(g.ShardIDs)) * 100
}

// GetInstancesPerShard returns the average number of instances per shard
func (g *Group) GetInstancesPerShard() float64 {
	if g.Stats.AssignedCount == 0 {
		return 0
	}
	return float64(g.Stats.InstanceCount) / float64(g.Stats.AssignedCount)
}

// NewGroupWithClock creates a new shard group with a specific clock
func NewGroupWithClock(groupID string, shardCount int, clock clockwork.Clock) *Group {
	now := clock.Now()

	shardIDs := make([]string, shardCount)
	for i := 0; i < shardCount; i++ {
		shardIDs[i] = fmt.Sprintf("%s-shard-%d", groupID, i)
	}

	return &Group{
		GroupID:     groupID,
		Description: fmt.Sprintf("Shard group for %s", groupID),
		ShardIDs:    shardIDs,
		Metadata:    make(map[string]string),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
