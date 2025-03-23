package reconcile

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/shard"
)

// Reconciler periodically reconciles shard assignments with instances
type Reconciler struct {
	mu           sync.RWMutex
	registry     *registry.Registry
	shardManager *shard.Manager
	interval     time.Duration
	logger       *zap.Logger
	clock        clockwork.Clock
	isRunning    bool
	stopCh       chan struct{}

	// Metrics for monitoring
	lastReconcileTime time.Time
	reconcileCount    int
	mismatchesFound   int
	mismatchesFixed   int
}

// ReconcilerParams defines dependencies for creating a Reconciler
type ReconcilerParams struct {
	fx.In

	Registry     *registry.Registry
	ShardManager *shard.Manager
	Logger       *zap.Logger
	Clock        clockwork.Clock `optional:"true"`
	Interval     time.Duration   `optional:"true" name:"reconcileInterval"`
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
		interval = 30 * time.Second // Default interval
	}

	return &Reconciler{
		registry:     params.Registry,
		shardManager: params.ShardManager,
		interval:     interval,
		logger:       params.Logger,
		clock:        clock,
		stopCh:       make(chan struct{}),
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

	// Run the first reconciliation immediately
	go r.reconcileShardOwnership()

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
		case <-ticker.Chan():
			go r.reconcileShardOwnership()
		}
	}
}

// reconcileShardOwnership reconciles shard ownership across instances
func (r *Reconciler) reconcileShardOwnership() {
	r.mu.Lock()
	r.lastReconcileTime = r.clock.Now()
	r.reconcileCount++
	r.mu.Unlock()

	r.logger.Info("Starting shard ownership reconciliation")

	// Get current assignments and instances
	assignments := r.shardManager.GetShardAssignments()
	instances := r.registry.GetAllInstances()
	versions := r.shardManager.GetVersions()

	// Track statistics
	mismatches := 0
	fixed := 0

	// For each instance, send a reconciliation message
	for instanceID, instance := range instances {
		// Skip if no active streams
		if len(instance.Streams) == 0 {
			continue
		}

		// Get shards assigned to this instance
		instanceShards := make(map[string]bool)
		for shardID, assignedInstanceID := range assignments {
			if assignedInstanceID == instanceID {
				instanceShards[shardID] = true
			}
		}

		// Skip if no shards assigned
		if len(instanceShards) == 0 {
			continue
		}

		// Send reconciliation to all streams for this instance
		for _, stream := range instance.Streams {
			// Process reconciliation
			result, err := r.reconcileInstance(stream, instanceShards, versions)
			if err != nil {
				r.logger.Warn("Failed to reconcile instance",
					zap.String("instance", instanceID),
					zap.Error(err))
				continue
			}

			mismatches += result.MismatchesFound
			fixed += result.MismatchesFixed
		}
	}

	// Update metrics
	r.mu.Lock()
	r.mismatchesFound += mismatches
	r.mismatchesFixed += fixed
	r.mu.Unlock()

	r.logger.Info("Completed shard ownership reconciliation",
		zap.Int("mismatches_found", mismatches),
		zap.Int("mismatches_fixed", fixed))
}

// ReconcileResult contains the results of a reconciliation
type ReconcileResult struct {
	MismatchesFound int
	MismatchesFixed int
}

// reconcileInstance reconciles shard ownership for a specific instance
func (r *Reconciler) reconcileInstance(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	instanceShards map[string]bool,
	versions map[string]int64,
) (*ReconcileResult, error) {
	result := &ReconcileResult{}

	// Begin reconciliation
	err := stream.Send(&proto.ServerMessage{
		Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
		ShardId:          "reconciliation-start",
		Action:           proto.ShardAssignmentAction_RECONCILE,
		IsReconciliation: true,
	})

	if err != nil {
		return result, err
	}

	// Send all shard assignments
	for shardID := range instanceShards {
		version := versions[shardID]

		// Send assignment
		err := stream.Send(&proto.ServerMessage{
			Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
			ShardId:          shardID,
			Action:           proto.ShardAssignmentAction_ASSIGN,
			IsReconciliation: true,
			Version:          version,
		})

		if err != nil {
			return result, err
		}

		// Count as fixed (optimistic assumption)
		result.MismatchesFound++
		result.MismatchesFixed++
	}

	// End reconciliation
	err = stream.Send(&proto.ServerMessage{
		Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
		ShardId:          "reconciliation-end",
		Action:           proto.ShardAssignmentAction_RECONCILE,
		IsReconciliation: true,
	})

	if err != nil {
		return result, err
	}

	return result, nil
}

// ForceReconciliation forces an immediate reconciliation
func (r *Reconciler) ForceReconciliation() {
	go r.reconcileShardOwnership()
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
