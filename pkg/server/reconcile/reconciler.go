package reconcile

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
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
	namespace            string

	// Metrics for monitoring
	lastReconcileTime time.Time
	reconcileCount    int
	mismatchesFound   int
	mismatchesFixed   int
}

// ReconcilerParams defines dependencies for creating a Reconciler
type ReconcilerParams struct {
	Namespace string
	Registry  *registry.Registry
	Store     store.Store
	Logger    *zap.Logger
	Clock     clockwork.Clock
	Interval  time.Duration
	Strategy  distribution.Strategy
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

	return &Reconciler{
		registry:             params.Registry,
		store:                params.Store,
		interval:             interval,
		logger:               params.Logger,
		clock:                clock,
		stopCh:               make(chan struct{}),
		forced:               make(chan struct{}, 1),
		distributionStrategy: params.Strategy,
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

	// Start the reconciliation loop
	go r.runReconcileLoop(ctx)

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
			go r.reconcileShardGroup()
		case <-ticker.Chan():
			go r.reconcileShardGroup()
		}
	}
}

// reconcileShardGroup reconciles a specific shard group
func (r *Reconciler) reconcileShardGroup() {
	r.logger.Info("Reconciling shard group", zap.String("namespace", r.namespace))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Load shard group information
	group, err := r.loadShardGroup(ctx, r.namespace)
	if err != nil {
		r.logger.Fatal("Failed to load shard group", zap.String("namespace", r.namespace), zap.Error(err))
	}

	// 2. Load current assignments for this group's shards
	currentAssignments, err := r.loadAssignments(ctx, group.ShardIDs)
	if err != nil {
		r.logger.Fatal("Failed to load assignments", zap.String("namespace", r.namespace), zap.Error(err))
	}

	// 3. Get active instances
	instances := r.registry.GetActiveInstances()
	if len(instances) == 0 {
		r.logger.Warn("No active instances available for reconciliation",
			zap.String("namespace", r.namespace))
		return
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
			zap.String("namespace", r.namespace))
		return
	}

	// 7. Apply new assignments to etcd and notify instances
	r.logger.Info("Applying new shard distribution",
		zap.String("namespace", r.namespace),
		zap.Int("totalShards", len(newDistribution)))

	if err := r.applyNewDistribution(ctx, r.namespace, currentAssignments, newDistribution); err != nil {
		r.logger.Warn("Failed to apply new shard distribution", zap.String("namespace", r.namespace), zap.Error(err))
		return
	}

	return
}

// loadShardGroup loads a specific shard group
func (r *Reconciler) loadShardGroup(ctx context.Context, groupID string) (Namespace, error) {
	data, err := r.store.GetNamespace(ctx, groupID)
	if err != nil {
		return Namespace{}, fmt.Errorf("failed to get shard group: %w", err)
	}

	return Namespace{
		Name:        data.Namespace,
		ShardIDs:    data.ShardIDs,
		Description: data.Description,
		Metadata:    data.Metadata,
	}, nil
}

// loadAssignments loads current assignments for the specified shards
func (r *Reconciler) loadAssignments(ctx context.Context, shardIDs []string) (map[string]store.Assignment, error) {
	// Get all assignments
	allAssignments, err := r.store.GetShardAssignments(ctx, r.namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard assignments: %w", err)
	}

	// Filter to just the shards we need
	assignments := make(map[string]store.Assignment)
	for _, shardID := range shardIDs {
		instanceID, exists := allAssignments[shardID]
		if exists {
			assignments[shardID] = instanceID
		}
	}

	return assignments, nil
}

// convertInstances converts registry instances to distribution strategy format
func convertInstances(instances map[string]*registry.InstanceData) map[string]distribution.InstanceInfo {
	strategyInstances := make(map[string]distribution.InstanceInfo)

	for id, instance := range instances {
		status := "active"
		if instance.Status.Status == proto.StatusReport_STATUS_DRAINING {
			status = "draining"
		}

		strategyInstances[id] = distribution.InstanceInfo{
			ID:     id,
			Status: status,
		}
	}

	return strategyInstances
}

// distributionsEqual checks if two distributions are the same
func distributionsEqual(a, b map[string]store.Assignment) bool {
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
	currentAssignments, newDistribution map[string]store.Assignment,
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
	assigns map[string][]*proto.ShardDistributorStreamResponse
	revokes map[string][]*proto.ShardDistributorStreamResponse
}

// identifyChanges identifies the changes between old and new distributions
func (r *Reconciler) identifyChanges(
	old, new map[string]store.Assignment,
) DistributionChanges {
	changes := DistributionChanges{
		assigns: make(map[string][]*proto.ShardDistributorStreamResponse),
		revokes: make(map[string][]*proto.ShardDistributorStreamResponse),
	}

	// Process additions and changes
	for shardID, newAssignment := range new {
		oldAssignment, exists := old[shardID]

		if exists && oldAssignment.OwnerID != newAssignment.OwnerID {
			// This is a transfer
			if newAssignment.NewOwnerID != "" {
				// First prepare
				prepareNotif := &proto.ShardDistributorStreamResponse{
					Type:             proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_PREPARE_ADD,
					SourceInstanceId: newAssignment.OwnerID,
				}

				// Then assign
				assignNotif := &proto.ShardDistributorStreamResponse{
					Type:             proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_ADD,
					SourceInstanceId: oldAssignment.NewOwnerID,
				}

				// Add to changes
				if _, ok := changes.assigns[newAssignment.OwnerID]; !ok {
					changes.assigns[newAssignment.OwnerID] = make([]*proto.ShardDistributorStreamResponse, 0)
				}
				changes.assigns[newAssignment.OwnerID] = append(
					changes.assigns[newAssignment.OwnerID],
					prepareNotif,
					assignNotif,
				)
			}

			// Revoke from old instance
			revokeNotif := &proto.ShardDistributorStreamResponse{
				Type:    proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT,
				ShardId: shardID,
				Action:  proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_DROP,
			}

			if _, ok := changes.revokes[oldAssignment.OwnerID]; !ok {
				changes.revokes[oldAssignment.OwnerID] = make([]*proto.ShardDistributorStreamResponse, 0)
			}
			changes.revokes[oldAssignment.OwnerID] = append(
				changes.revokes[oldAssignment.OwnerID],
				revokeNotif,
			)
		} else if !exists || oldAssignment.OwnerID != newAssignment.OwnerID {
			// This is a new assignment (not a transfer)
			if newAssignment.OwnerID != "" {
				assignNotif := &proto.ShardDistributorStreamResponse{
					Type:    proto.ShardDistributorStreamResponse_MESSAGE_TYPE_SHARD_ASSIGNMENT,
					ShardId: shardID,
					Action:  proto.ShardAssignmentAction_SHARD_ASSIGNMENT_ACTION_ADD,
				}

				if _, ok := changes.assigns[newAssignment.OwnerID]; !ok {
					changes.assigns[newAssignment.OwnerID] = make([]*proto.ShardDistributorStreamResponse, 0)
				}
				changes.assigns[newAssignment.OwnerID] = append(
					changes.assigns[newAssignment.OwnerID],
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

// Namespace represents a group of related shards
type Namespace struct {
	Name        string
	ShardIDs    []string
	Description string
	Metadata    map[string]string
}
