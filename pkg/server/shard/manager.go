package shard

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/distribution"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// StreamProvider provides streams for a given instance
type StreamProvider func(instanceID string) []proto.ShardDistributor_ShardDistributorStreamServer

// Manager manages shard assignments and distribution
type Manager struct {
	mu                   sync.RWMutex
	store                store.Store
	logger               *zap.Logger
	registry             *registry.Registry
	assignments          map[string]string // shardID -> instanceID
	versionManager       *VersionManager
	distributionStrategy distribution.Strategy
	groups               map[string]*Group // groupID -> group
	notifier             *DefaultShardNotifier
	streamProvider       StreamProvider
	clock                clockwork.Clock
	isLeader             bool

	// Transfer tracking
	pendingTransfers    map[string]string    // shardID -> targetInstanceID
	transferStarts      map[string]time.Time // shardID -> startTime
	transferCompletions map[string]time.Time // shardID -> completionTime
	transferLatencies   []time.Duration      // All transfer latencies
}

// ManagerParams defines dependencies for creating a Manager
type ManagerParams struct {
	fx.In

	Store          store.Store
	VersionManager *VersionManager
	Logger         *zap.Logger
	Registry       *registry.Registry
	Strategy       distribution.Strategy `optional:"true"`
	Clock          clockwork.Clock       `optional:"true"`
}

// NewManager creates a new shard manager
func NewManager(params ManagerParams) *Manager {
	// Use defaults for optional dependencies
	strategy := params.Strategy
	if strategy == nil {
		strategy = distribution.NewConsistentHashStrategy(10)
	}

	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	manager := &Manager{
		store:                params.Store,
		logger:               params.Logger,
		registry:             params.Registry,
		assignments:          make(map[string]string),
		versionManager:       params.VersionManager,
		distributionStrategy: strategy,
		groups:               make(map[string]*Group),
		clock:                clock,
		pendingTransfers:     make(map[string]string),
		transferStarts:       make(map[string]time.Time),
		transferCompletions:  make(map[string]time.Time),
		transferLatencies:    make([]time.Duration, 0),
	}

	return manager
}

// SetDistributionStrategy sets the shard distribution strategy
func (m *Manager) SetDistributionStrategy(strategy distribution.Strategy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.distributionStrategy = strategy
}

// SetLeaderStatus updates the leader status
func (m *Manager) SetLeaderStatus(isLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isLeader = isLeader
}

// SetStreamProvider sets the provider for instance streams
func (m *Manager) SetStreamProvider(provider StreamProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamProvider = provider
}

// RegisterShardGroup creates a new shard group
func (m *Manager) RegisterShardGroup(
	ctx context.Context,
	groupID string,
	shardCount int,
	description string,
	metadata map[string]string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if group already exists
	if _, exists := m.groups[groupID]; exists {
		return fmt.Errorf("shard group %s already exists", groupID)
	}

	// Create group
	group := NewShardGroup(groupID, shardCount, m.clock.Now())
	group.Description = description

	// Add metadata if provided
	if metadata != nil {
		for k, v := range metadata {
			group.Metadata[k] = v
		}
	}

	// Save to storage
	data, err := group.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize group: %w", err)
	}

	err = m.store.SaveShardGroup(ctx, groupID, data)
	if err != nil {
		return fmt.Errorf("failed to save group: %w", err)
	}

	// Create shard assignments
	for _, shardID := range group.ShardIDs {
		m.assignments[shardID] = "" // Unassigned initially
	}

	// Add to local state
	m.groups[groupID] = group

	m.logger.Info("Registered shard group",
		zap.String("group_id", groupID),
		zap.Int("shard_count", shardCount))

	return nil
}

// LoadShardGroups loads all shard groups from storage
func (m *Manager) LoadShardGroups(ctx context.Context) error {
	groups, err := m.store.GetShardGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to load shard groups: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for groupID, data := range groups {
		group, err := DeserializeShardGroup(data)
		if err != nil {
			m.logger.Warn("Failed to deserialize shard group",
				zap.String("group_id", groupID),
				zap.Error(err))
			continue
		}

		m.groups[groupID] = group
		m.logger.Info("Loaded shard group",
			zap.String("group_id", groupID),
			zap.Int("shard_count", len(group.ShardIDs)))
	}

	return nil
}

// LoadShardDefinitions loads shard definitions from storage
func (m *Manager) LoadShardDefinitions(ctx context.Context, numShards int) error {
	// Check if shards are already defined
	shards, err := m.store.GetShardAssignments(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shard assignments: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// If shards already exist, load them
	if len(shards) > 0 {
		m.logger.Info("Loading shards", zap.Int("existing_shards", len(shards)))
		m.assignments = shards
		return nil
	}

	// Initialize shards if none exist
	m.logger.Info("Initializing shards", zap.Int("new_shards", numShards))

	// Create empty assignments
	assignments := make(map[string]string)
	for i := 0; i < numShards; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		assignments[shardID] = ""
	}

	// Save to storage
	if err := m.store.SaveShardAssignments(ctx, assignments); err != nil {
		return fmt.Errorf("failed to save shard assignments: %w", err)
	}

	m.assignments = assignments
	return nil
}

// RecalculateDistribution recalculates the shard distribution
func (m *Manager) RecalculateDistribution() {
	m.mu.Lock()

	if !m.isLeader {
		m.logger.Debug("Not leader, skipping distribution recalculation")
		m.mu.Unlock()
		return
	}

	m.logger.Info("Recalculating shard distribution")

	// Get active instances
	instances := m.registry.GetActiveInstances()

	// Convert to the format needed by the distribution strategy
	strategyInstances := make(map[string]distribution.InstanceInfo)
	for id, instance := range instances {
		status := "active"
		if instance.Status.Status == proto.StatusReport_DRAINING {
			status = "draining"
		}

		strategyInstances[id] = distribution.InstanceInfo{
			ID:         id,
			Status:     status,
			LoadFactor: instance.Status.CpuUsage,
			ShardCount: int(instance.Status.ActiveShardCount),
		}
	}

	// Make a copy of assignments for strategy calculation
	currentAssignments := make(map[string]string)
	for k, v := range m.assignments {
		currentAssignments[k] = v
	}

	// Unlock while calculating to avoid holding lock during potentially expensive operation
	m.mu.Unlock()

	// Calculate new distribution
	newDistribution := m.distributionStrategy.CalculateDistribution(
		currentAssignments,
		strategyInstances,
	)

	// Lock again to update state
	m.mu.Lock()
	defer m.mu.Unlock()

	// Skip if no changes
	if m.distributionsEqual(m.assignments, newDistribution) {
		m.logger.Debug("No changes in distribution")
		return
	}

	// Get new version for assignments
	version, err := m.versionManager.IncrementGlobalVersion(context.Background())
	if err != nil {
		m.logger.Error("Failed to increment global version", zap.Error(err))
		return
	}

	// Apply the new distribution
	m.applyDistribution(newDistribution, version)
}

// applyDistribution applies the new distribution and sends notifications
func (m *Manager) applyDistribution(
	newDistribution map[string]string,
	version int64,
) {
	// Save to storage (in background)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.store.SaveShardAssignments(ctx, newDistribution); err != nil {
			m.logger.Error("Failed to save shard assignments", zap.Error(err))
			return
		}

		// Update version for each shard
		for shardID := range newDistribution {
			if err := m.store.SaveShardVersion(ctx, shardID, version); err != nil {
				m.logger.Warn("Failed to save shard version",
					zap.String("shard", shardID),
					zap.Error(err))
			}
		}
	}()

	// Identify changes for notifications
	changes := m.identifyChanges(m.assignments, newDistribution)

	// Get streams for instances
	if m.streamProvider != nil {
		// Process prepare and assign notifications
		for instanceID, notifications := range changes.assigns {
			streams := m.streamProvider(instanceID)
			if len(streams) == 0 {
				m.logger.Warn("No streams for instance, skipping notifications",
					zap.String("instance", instanceID))
				continue
			}

			// Track transfers for latency measurement
			for _, notif := range notifications {
				if notif.Action == proto.ShardAssignmentAction_ASSIGN &&
					notif.SourceInstanceId != "" {
					m.pendingTransfers[notif.ShardId] = instanceID
					m.transferStarts[notif.ShardId] = m.clock.Now()
				}
			}

			// Send to all streams
			for _, stream := range streams {
				for _, notif := range notifications {
					err := stream.Send(notif)
					if err != nil {
						m.logger.Warn("Failed to send notification",
							zap.String("instance", instanceID),
							zap.Error(err))
					}
				}
			}
		}

		// Process revoke notifications after a small delay
		go func() {
			time.Sleep(500 * time.Millisecond)

			m.mu.Lock()
			defer m.mu.Unlock()

			for instanceID, notifications := range changes.revokes {
				streams := m.streamProvider(instanceID)
				if len(streams) == 0 {
					continue
				}

				// Send to all streams
				for _, stream := range streams {
					for _, notif := range notifications {
						err := stream.Send(notif)
						if err != nil {
							m.logger.Warn("Failed to send revoke",
								zap.String("instance", instanceID),
								zap.Error(err))
						}
					}
				}
			}
		}()
	}

	// Update local assignments
	m.assignments = newDistribution
}

func (m *Manager) GetVersions() map[string]int64 {
	return m.versionManager.GetVersions()
}

func (m *Manager) GetVersion(shardID string) int64 {
	return m.versionManager.GetShardVersion(shardID)
}

// Helper methods

// distributionsEqual compares two shard distribution maps
func (m *Manager) distributionsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}

	return true
}

// DistributionChanges holds notifications for distribution changes
type DistributionChanges struct {
	assigns map[string][]*proto.ServerMessage
	revokes map[string][]*proto.ServerMessage
}

// identifyChanges identifies the changes between old and new distributions
func (m *Manager) identifyChanges(
	old, new map[string]string,
) DistributionChanges {
	changes := DistributionChanges{
		assigns: make(map[string][]*proto.ServerMessage),
		revokes: make(map[string][]*proto.ServerMessage),
	}

	// Process additions and changes
	for shardID, newInstanceID := range new {
		oldInstanceID, exists := old[shardID]

		if exists && oldInstanceID != "" && oldInstanceID != newInstanceID {
			// This is a transfer
			if newInstanceID != "" {
				// First prepare
				prepareNotif := &proto.ServerMessage{
					Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_PREPARE,
					SourceInstanceId: oldInstanceID,
					Version:          m.versionManager.GetShardVersion(shardID),
				}

				// Then assign
				assignNotif := &proto.ServerMessage{
					Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_ASSIGN,
					SourceInstanceId: oldInstanceID,
					Version:          m.versionManager.GetShardVersion(shardID),
				}

				// Add to changes
				if _, ok := changes.assigns[newInstanceID]; !ok {
					changes.assigns[newInstanceID] = make([]*proto.ServerMessage, 0)
				}
				changes.assigns[newInstanceID] = append(
					changes.assigns[newInstanceID],
					prepareNotif,
					assignNotif,
				)
			}

			// Revoke from old instance
			revokeNotif := &proto.ServerMessage{
				Type:    proto.ServerMessage_SHARD_ASSIGNMENT,
				ShardId: shardID,
				Action:  proto.ShardAssignmentAction_REVOKE,
				Version: m.versionManager.GetShardVersion(shardID),
			}

			if _, ok := changes.revokes[oldInstanceID]; !ok {
				changes.revokes[oldInstanceID] = make([]*proto.ServerMessage, 0)
			}
			changes.revokes[oldInstanceID] = append(
				changes.revokes[oldInstanceID],
				revokeNotif,
			)
		} else if !exists || oldInstanceID != newInstanceID {
			// This is a new assignment (not a transfer)
			if newInstanceID != "" {
				assignNotif := &proto.ServerMessage{
					Type:    proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId: shardID,
					Action:  proto.ShardAssignmentAction_ASSIGN,
					Version: m.versionManager.GetShardVersion(shardID),
				}

				if _, ok := changes.assigns[newInstanceID]; !ok {
					changes.assigns[newInstanceID] = make([]*proto.ServerMessage, 0)
				}
				changes.assigns[newInstanceID] = append(
					changes.assigns[newInstanceID],
					assignNotif,
				)
			}
		}
	}

	return changes
}

// RecordTransferCompletion records completion of a shard transfer
func (m *Manager) RecordTransferCompletion(shardID string, targetInstanceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if this was a pending transfer
	pendingInstanceID, exists := m.pendingTransfers[shardID]
	if !exists || pendingInstanceID != targetInstanceID {
		return
	}

	// Get start time
	startTime, exists := m.transferStarts[shardID]
	if !exists {
		return
	}

	// Calculate latency
	now := m.clock.Now()
	latency := now.Sub(startTime)

	// Record metrics
	m.transferLatencies = append(m.transferLatencies, latency)
	m.transferCompletions[shardID] = now

	// Clean up tracking maps
	delete(m.pendingTransfers, shardID)
	delete(m.transferStarts, shardID)

	m.logger.Info("Completed shard transfer",
		zap.String("shard", shardID),
		zap.String("instance", targetInstanceID),
		zap.Duration("latency", latency))
}

// GetShardAssignments returns the current shard assignments
func (m *Manager) GetShardAssignments() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	assignments := make(map[string]string)
	for k, v := range m.assignments {
		assignments[k] = v
	}

	return assignments
}

// GetShardGroup returns the specified shard group
func (m *Manager) GetShardGroup(groupID string) (*Group, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.groups[groupID]
	return group, exists
}

// GetAllShardGroups returns all shard groups
func (m *Manager) GetAllShardGroups() map[string]*Group {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	groups := make(map[string]*Group)
	for k, v := range m.groups {
		groups[k] = v
	}

	return groups
}

// GetTransferLatencyStats returns statistics about shard transfers
func (m *Manager) GetTransferLatencyStats() (min, max, avg time.Duration, count int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.transferLatencies) == 0 {
		return 0, 0, 0, 0
	}

	min = m.transferLatencies[0]
	max = m.transferLatencies[0]
	var sum time.Duration

	for _, latency := range m.transferLatencies {
		if latency < min {
			min = latency
		}
		if latency > max {
			max = latency
		}
		sum += latency
	}

	avg = sum / time.Duration(len(m.transferLatencies))
	return min, max, avg, len(m.transferLatencies)
}
