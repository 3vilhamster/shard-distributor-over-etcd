package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// Registry manages service instance registrations
type Registry struct {
	mu           sync.RWMutex
	instances    map[string]*InstanceData
	store        store.Store
	logger       *zap.Logger
	heartbeatTTL time.Duration
	clock        clockwork.Clock

	// Callback functions for instance lifecycle events
	onInstanceRegistered   func(instanceID string)
	onInstanceDeregistered func(instanceID string)
	onInstanceDraining     func(instanceID string)

	// Workload assignment tracking
	workloadAssignments map[string]map[string]bool // map[workloadType]map[instanceID]bool
}

// Params defines dependencies for the registry
type Params struct {
	fx.In

	Store  store.Store
	Logger *zap.Logger
	Clock  clockwork.Clock
}

// NewRegistry creates a new instance registry
func NewRegistry(params Params) *Registry {
	// If clock wasn't provided, use the real clock
	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	return &Registry{
		instances:           make(map[string]*InstanceData),
		store:               params.Store,
		logger:              params.Logger,
		heartbeatTTL:        10 * time.Second,
		workloadAssignments: make(map[string]map[string]bool),
		clock:               clock,
	}
}

// RegisterInstance registers a new service instance
func (r *Registry) RegisterInstance(
	ctx context.Context,
	instanceInfo *proto.InstanceInfo,
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	peerAddr string,
) (*proto.ServerMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instanceID := instanceInfo.InstanceId
	r.logger.Info("Instance registering", zap.String("instance", instanceID))

	// Register in etcd
	if err := r.store.SaveInstance(ctx, instanceID, peerAddr, r.heartbeatTTL); err != nil {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_REGISTER_RESPONSE,
			Success: false,
			Message: fmt.Sprintf("Failed to register in etcd: %v", err),
		}, nil
	}

	// Create or update instance data
	if existingInstance, exists := r.instances[instanceID]; exists {
		// Update existing instance
		existingInstance.Info = instanceInfo
		existingInstance.UpdateHeartbeat()
		existingInstance.AddStream(stream)

		// Log reconnection
		existingInstance.Stats.ReconnectCount++
		r.logger.Info("Instance reconnected",
			zap.String("instance", instanceID),
			zap.Int("reconnect_count", existingInstance.Stats.ReconnectCount))
	} else {
		// Create new instance
		instance := NewInstanceData(instanceID, instanceInfo, peerAddr, r.clock)
		instance.AddStream(stream)
		r.instances[instanceID] = instance

		// Notify callback if this is a new registration
		if r.onInstanceRegistered != nil {
			go r.onInstanceRegistered(instanceID)
		}
	}

	// Return success response
	return &proto.ServerMessage{
		Type:    proto.ServerMessage_REGISTER_RESPONSE,
		Success: true,
		Message: "Instance registered successfully",
	}, nil
}

// UpdateInstanceStatus updates the status of a service instance
func (r *Registry) UpdateInstanceStatus(
	ctx context.Context,
	status *proto.StatusReport,
) (*proto.ServerMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instanceID := status.InstanceId

	// Find the instance
	instance, exists := r.instances[instanceID]
	if !exists {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_STATUS_RESPONSE,
			Success: false,
			Message: "Instance not found",
		}, nil
	}

	// Check if transitioning to DRAINING
	wasActive := instance.Status.Status == proto.StatusReport_ACTIVE
	isDraining := status.Status == proto.StatusReport_DRAINING
	needsCallback := wasActive && isDraining

	// Update status
	instance.UpdateStatus(status)

	// Call draining callback if needed
	if needsCallback && r.onInstanceDraining != nil {
		go r.onInstanceDraining(instanceID)
	}

	return &proto.ServerMessage{
		Type:    proto.ServerMessage_STATUS_RESPONSE,
		Success: true,
		Message: "Status updated",
	}, nil
}

// HandleInstanceDisconnect processes an instance disconnection
func (r *Registry) HandleInstanceDisconnect(instanceID string, stream proto.ShardDistributor_ShardDistributorStreamServer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	instance, exists := r.instances[instanceID]
	if !exists {
		return
	}

	// Remove this stream and update stats
	if instance.RemoveStream(stream) {
		r.logger.Info("Stream disconnected from instance",
			zap.String("instance", instanceID),
			zap.Int("remaining_streams", instance.GetStreamCount()))
	}

	// If no streams remain, update last disconnect time
	if instance.GetStreamCount() == 0 {
		instance.Stats.LastDisconnectAt = r.clock.Now()
	}
}

// GetActiveInstances returns all active instances
func (r *Registry) GetActiveInstances() map[string]*InstanceData {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	instances := make(map[string]*InstanceData)
	for id, instance := range r.instances {
		// Skip instances that are draining
		if instance.IsDraining() {
			continue
		}
		instances[id] = instance
	}

	return instances
}

// GetAllInstances returns all instances (including draining ones)
func (r *Registry) GetAllInstances() map[string]*InstanceData {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	instances := make(map[string]*InstanceData)
	for id, instance := range r.instances {
		instances[id] = instance
	}

	return instances
}

// GetInstance returns the instance data for a given ID
func (r *Registry) GetInstance(instanceID string) (*InstanceData, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instance, exists := r.instances[instanceID]
	return instance, exists
}

// GetInstanceStreams returns the streams for a given instance
func (r *Registry) GetInstanceStreams(instanceID string) []proto.ShardDistributor_ShardDistributorStreamServer {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instance, exists := r.instances[instanceID]
	if !exists {
		return nil
	}

	// Return a copy to avoid concurrent modification
	streams := make([]proto.ShardDistributor_ShardDistributorStreamServer, len(instance.Streams))
	copy(streams, instance.Streams)

	return streams
}

// SetCallbacks sets the callbacks for instance events
func (r *Registry) SetCallbacks(
	onRegistered func(string),
	onDeregistered func(string),
	onDraining func(string),
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onInstanceRegistered = onRegistered
	r.onInstanceDeregistered = onDeregistered
	r.onInstanceDraining = onDraining
}

// RemoveUnhealthyInstances removes instances that haven't sent heartbeats
func (r *Registry) RemoveUnhealthyInstances(timeout time.Duration) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clock.Now()
	removed := []string{}

	for id, instance := range r.instances {
		if instance.TimeSinceHeartbeat() > timeout {
			// Update stats before removal
			instance.Stats.LastDisconnectAt = now

			// Remove from map
			delete(r.instances, id)
			removed = append(removed, id)

			// Log removal
			r.logger.Info("Removed unhealthy instance",
				zap.String("instance", id),
				zap.Duration("timeout", timeout),
				zap.Duration("time_since_heartbeat", instance.TimeSinceHeartbeat()))

			// Delete from etcd in background
			go func(instanceID string) {
				if err := r.store.DeleteInstance(context.Background(), instanceID); err != nil {
					r.logger.Warn("Failed to remove unhealthy instance from etcd",
						zap.String("instance", instanceID),
						zap.Error(err))
				}
			}(id)

			// Notify callback
			if r.onInstanceDeregistered != nil {
				go r.onInstanceDeregistered(id)
			}
		}
	}

	return removed
}

// AssignInstanceToWorkload assigns an instance to a workload type
func (r *Registry) AssignInstanceToWorkload(instanceID, workloadType string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Ensure the instance exists
	if _, exists := r.instances[instanceID]; !exists {
		return
	}

	// Ensure the workload type map exists
	if _, exists := r.workloadAssignments[workloadType]; !exists {
		r.workloadAssignments[workloadType] = make(map[string]bool)
	}

	// Assign instance to workload
	r.workloadAssignments[workloadType][instanceID] = true
}

// RemoveInstanceFromWorkload removes an instance from a workload type
func (r *Registry) RemoveInstanceFromWorkload(instanceID, workloadType string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove from the workload assignments
	if assignments, exists := r.workloadAssignments[workloadType]; exists {
		delete(assignments, instanceID)

		// If no instances left for this workload, clean it up
		if len(assignments) == 0 {
			delete(r.workloadAssignments, workloadType)
		}
	}
}

// GetInstancesForWorkload returns all instances assigned to a workload type
func (r *Registry) GetInstancesForWorkload(workloadType string) map[string]*InstanceData {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*InstanceData)

	// Get instances assigned to this workload
	if assignments, exists := r.workloadAssignments[workloadType]; exists {
		for instanceID := range assignments {
			if instance, instanceExists := r.instances[instanceID]; instanceExists && !instance.IsDraining() {
				result[instanceID] = instance
			}
		}
	}

	return result
}

// GetInstanceCount returns the total number of registered instances
func (r *Registry) GetInstanceCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.instances)
}

// GetActiveInstanceCount returns the number of active instances
func (r *Registry) GetActiveInstanceCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, instance := range r.instances {
		if !instance.IsDraining() {
			count++
		}
	}

	return count
}

// SetClock sets the clock for testing purposes
func (r *Registry) SetClock(clock clockwork.Clock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock = clock

	// Update clock in all instances too
	for _, instance := range r.instances {
		instance.clock = clock
	}
}
