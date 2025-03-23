package reconcile

import (
	"context"
	"encoding/json"
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

// Reconciler periodically reconciles shard assignments with instances
type Reconciler struct {
	mu                   sync.RWMutex
	registry             *registry.Registry
	store                store.Store
	interval             time.Duration
	logger               *zap.Logger
	clock                clockwork.Clock
	isRunning            bool
	stopCh               chan struct{}
	forced               chan struct{}
	isLeader             bool
	distributionStrategy distribution.Strategy
	shardGroups          map[string]bool // Tracks which shard groups to reconcile

	// Metrics for monitoring
	lastReconcileTime time.Time
	reconcileCount    int
	mismatchesFound   int
	mismatchesFixed   int
}

// ReconcilerParams defines dependencies for creating a Reconciler
type ReconcilerParams struct {
	fx.In

	Registry *registry.Registry
	Store    store.Store
	Logger   *zap.Logger
	Clock    clockwork.Clock       `optional:"true"`
	Interval time.Duration         `optional:"true" name:"reconcileInterval"`
	Strategy distribution.Strategy `optional:"true"`
}

// NewReconciler creates a new reconciler
func NewReconciler(params ReconcilerParams) *Reconciler {
	// Use defaults for optional dependencies
	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	interval := params.Interval
	if interval <= 0 {
		interval = 5 * time.Second // Default interval
	}

	strategy := params.Strategy
	if strategy == nil {
		strategy = distribution.NewConsistentHashStrategy(10) // Default strategy
	}

	return &Reconciler{
		registry:             params.Registry,
		store:                params.Store,
		interval:             interval,
		logger:               params.Logger,
		clock:                clock,
		stopCh:               make(chan struct{}),
		forced:               make(chan struct{}, 1),
		distributionStrategy: strategy,
		shardGroups:          make(map[string]bool),
	}
}

// Start starts the reconciler
func (r *Reconciler) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.isRunning {
		r.mu.Unlock()
		return nil // Already running
	}
	r.isRunning = true
	r.mu.Unlock()

	r.logger.Info("Starting reconciler", zap.Duration("interval", r.interval))

	// Load shard groups first
	if err := r.loadShardGroups(ctx); err != nil {
		r.logger.Warn("Failed to load shard groups", zap.Error(err))
		// Continue anyway
	}

	// Run the first reconciliation immediately
	go r.reconcileAllGroups()

	// Start the reconciliation loop
	go r.runReconcileLoop(ctx)

	return nil
}

// loadShardGroups loads all shard groups from the store
func (r *Reconciler) loadShardGroups(ctx context.Context) error {
	r.logger.Info("Loading shard groups")

	groups, err := r.store.GetShardGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to load shard groups: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Clear existing groups
	r.shardGroups = make(map[string]bool)

	// Add each group for reconciliation
	for groupID := range groups {
		r.shardGroups[groupID] = true
	}

	r.logger.Info("Loaded shard groups", zap.Int("count", len(r.shardGroups)))
	return nil
}

// Stop stops the reconciler
func (r *Reconciler) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isRunning {
		return
	}

	close(r.stopCh)
	r.isRunning = false
	r.logger.Info("Reconciler stopped")
}

// SetLeaderStatus updates the leader status
func (r *Reconciler) SetLeaderStatus(isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldIsLeader := r.isLeader
	r.isLeader = isLeader

	if oldIsLeader != isLeader {
		r.logger.Info("Leader status changed", zap.Bool("isLeader", isLeader))
	}
}

// IsRunning returns whether the reconciler is running
func (r *Reconciler) IsRunning() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isRunning
}

// runReconcileLoop runs the reconciliation loop
func (r *Reconciler) runReconcileLoop(ctx context.Context) {
	ticker := r.clock.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ctx.Done():
			return
		case <-r.forced:
			go r.reconcileAllGroups()
		case <-ticker.Chan():
			go r.reconcileAllGroups()
		}
	}
}

// reconcileAllGroups reconciles all shard groups
func (r *Reconciler) reconcileAllGroups() {
	r.mu.Lock()
	if !r.isLeader {
		r.logger.Debug("Not leader, skipping reconciliation")
		r.mu.Unlock()
		return
	}

	r.lastReconcileTime = r.clock.Now()
	r.reconcileCount++

	// Make a copy of the shard groups to reconcile
	groupsToReconcile := make([]string, 0, len(r.shardGroups))
	for groupID := range r.shardGroups {
		groupsToReconcile = append(groupsToReconcile, groupID)
	}
	r.mu.Unlock()

	r.logger.Info("Starting reconciliation of all shard groups",
		zap.Int("groupCount", len(groupsToReconcile)))

	// For each shard group, perform reconciliation
	for _, groupID := range groupsToReconcile {
		if err := r.reconcileShardGroup(groupID); err != nil {
			r.logger.Error("Failed to reconcile shard group",
				zap.String("groupID", groupID),
				zap.Error(err))
		}
	}

	r.logger.Info("Completed reconciliation of all shard groups")
}

// reconcileShardGroup reconciles a specific shard group
func (r *Reconciler) reconcileShardGroup(groupID string) {
	r.logger.Info("Reconciling shard group", zap.String("groupID", groupID))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Load shard group information
	group, err := r.loadShardGroup(ctx, groupID)
	if err != nil {
		r.logger.Fatal("Failed to load shard group", zap.String("groupID", groupID), zap.Error(err))
	}

	// 2. Load current assignments for this group's shards
	currentAssignments, err := r.loadAssignments(ctx, group.ShardIDs)
	if err != nil {
		r.logger.Fatal("Failed to load assignments", zap.String("groupID", groupID), zap.Error(err))
	}

	// 3. Get active instances
	instances := r.registry.GetActiveInstances()
	if len(instances) == 0 {
		r.logger.Warn("No active instances available for reconciliation",
			zap.String("groupID", groupID))
		return nil
	}

	// 4. Convert instances to the format needed for distribution strategy
	strategyInstances := convertInstances(instances)

	// 5. Calculate new distribution using the strategy
	newDistribution := r.distributionStrategy.CalculateDistribution(
		currentAssignments,
		strategyInstances,
	)

	// 6. Compare old and new assignments
	if distributionsEqual(currentAssignments, newDistribution) {
		r.logger.Debug("No changes needed in distribution",
			zap.String("groupID", groupID))
		return nil
	}

	// 7. Apply new assignments to etcd and notify instances
	r.logger.Info("Applying new shard distribution",
		zap.String("groupID", groupID),
		zap.Int("totalShards", len(newDistribution)))

	if err := r.applyNewDistribution(ctx, groupID, currentAssignments, newDistribution); err != nil {
		return fmt.Errorf("failed to apply new distribution: %w", err)
	}

	return nil
}

// loadShardGroup loads a specific shard group
func (r *Reconciler) loadShardGroup(ctx context.Context, groupID string) (*ShardGroup, error) {
	data, err := r.store.GetShardGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard group: %w", err)
	}

	group, err := DeserializeShardGroup(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize shard group: %w", err)
	}

	return group, nil
}

// loadAssignments loads current assignments for the specified shards
func (r *Reconciler) loadAssignments(ctx context.Context, shardIDs []string) (map[string]string, error) {
	// Get all assignments
	allAssignments, err := r.store.GetShardAssignments(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard assignments: %w", err)
	}

	// Filter to just the shards we need
	assignments := make(map[string]string)
	for _, shardID := range shardIDs {
		instanceID, exists := allAssignments[shardID]
		if exists {
			assignments[shardID] = instanceID
		} else {
			// If not assigned, mark as empty
			assignments[shardID] = ""
		}
	}

	return assignments, nil
}

// convertInstances converts registry instances to distribution strategy format
func convertInstances(instances map[string]*registry.InstanceData) map[string]distribution.InstanceInfo {
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

	return strategyInstances
}

// distributionsEqual checks if two distributions are the same
func distributionsEqual(a, b map[string]string) bool {
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

// applyNewDistribution applies the new distribution to etcd and notifies instances
func (r *Reconciler) applyNewDistribution(
	ctx context.Context,
	groupID string,
	currentAssignments, newDistribution map[string]string,
) error {
	// 1. Save the new assignments to etcd
	if err := r.store.SaveShardAssignments(ctx, groupID, newDistribution); err != nil {
		return fmt.Errorf("failed to save shard assignments: %w", err)
	}

	// 2. Get the differences between old and new assignments
	changes := r.identifyChanges(currentAssignments, newDistribution)

	// 3. Send notifications to instances
	r.sendAssignmentNotifications(changes)

	// Record metrics
	r.mu.Lock()
	r.mismatchesFound += len(changes.assigns) + len(changes.revokes)
	r.mismatchesFixed += len(changes.assigns) + len(changes.revokes)
	r.mu.Unlock()

	return nil
}

// DistributionChanges holds notifications for distribution changes
type DistributionChanges struct {
	assigns map[string][]*proto.ServerMessage
	revokes map[string][]*proto.ServerMessage
}

// identifyChanges identifies the changes between old and new distributions
func (r *Reconciler) identifyChanges(
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
				}

				// Then assign
				assignNotif := &proto.ServerMessage{
					Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_ASSIGN,
					SourceInstanceId: oldInstanceID,
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

// sendAssignmentNotifications sends notifications to instances about assignment changes
func (r *Reconciler) sendAssignmentNotifications(changes DistributionChanges) {
	// Process prepare and assign notifications
	for instanceID, notifications := range changes.assigns {
		streams := r.registry.GetInstanceStreams(instanceID)
		if len(streams) == 0 {
			r.logger.Warn("No streams for instance, skipping notifications",
				zap.String("instance", instanceID))
			continue
		}

		// Send to all streams
		for _, stream := range streams {
			for _, notif := range notifications {
				err := stream.Send(notif)
				if err != nil {
					r.logger.Warn("Failed to send notification",
						zap.String("instance", instanceID),
						zap.Error(err))
				}
			}
		}
	}

	// Process revoke notifications after a small delay
	go func() {
		time.Sleep(500 * time.Millisecond)

		for instanceID, notifications := range changes.revokes {
			streams := r.registry.GetInstanceStreams(instanceID)
			if len(streams) == 0 {
				continue
			}

			// Send to all streams
			for _, stream := range streams {
				for _, notif := range notifications {
					err := stream.Send(notif)
					if err != nil {
						r.logger.Warn("Failed to send revoke",
							zap.String("instance", instanceID),
							zap.Error(err))
					}
				}
			}
		}
	}()
}

// ForceReconciliation forces an immediate reconciliation
func (r *Reconciler) ForceReconciliation() {
	go r.reconcileAllGroups()
}

// ForceReconciliationDueToInstanceChange forces reconciliation when an instance changes
func (r *Reconciler) ForceReconciliationDueToInstanceChange(instanceID string) {
	r.logger.Info("Forcing reconciliation due to instance change",
		zap.String("instanceID", instanceID))
	go r.reconcileAllGroups()
}

// GetReconcileMetrics returns reconciliation metrics
func (r *Reconciler) GetReconcileMetrics() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"is_running":       r.isRunning,
		"interval":         r.interval.String(),
		"reconcile_count":  r.reconcileCount,
		"last_reconcile":   r.lastReconcileTime,
		"mismatches_found": r.mismatchesFound,
		"mismatches_fixed": r.mismatchesFixed,
	}
}

// SetInterval updates the reconciliation interval
func (r *Reconciler) SetInterval(interval time.Duration) {
	if interval <= 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.interval = interval
	r.logger.Info("Updated reconciliation interval", zap.Duration("interval", interval))

	// Restart if running
	if r.isRunning {
		close(r.stopCh)
		r.stopCh = make(chan struct{})
		go r.runReconcileLoop(context.Background())
	}
}

// ShardGroup represents a group of related shards
type ShardGroup struct {
	GroupID     string
	ShardIDs    []string
	Description string
	Metadata    map[string]string
}

// DeserializeShardGroup creates a ShardGroup from a storage string
func DeserializeShardGroup(data string) (*ShardGroup, error) {
	// Using a stub implementation for now
	return &ShardGroup{
		GroupID:     "stub",
		ShardIDs:    []string{},
		Description: "Stub group",
		Metadata:    make(map[string]string),
	}, nil
}

// RegisterShardGroup creates a new shard group
func (r *Reconciler) RegisterShardGroup(ctx context.Context, group *ShardGroup) error {
	// Serialize the group
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("marshal group: %w", err)
	}

	// Save to storage
	if err := r.store.SaveShardGroup(ctx, group.GroupID, string(data)); err != nil {
		return fmt.Errorf("failed to save shard group: %w", err)
	}

	// Add to local tracking
	r.mu.Lock()
	r.shardGroups[group.GroupID] = true
	r.mu.Unlock()

	// Force immediate reconciliation
	go r.reconcileShardGroup(group.GroupID)

	return nil
}
