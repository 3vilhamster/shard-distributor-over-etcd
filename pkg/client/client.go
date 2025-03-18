package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
)

// ServiceInstance represents a service instance that handles shards
type ServiceInstance struct {
	mu              sync.RWMutex
	instanceID      string
	endpoint        string
	distributorAddr string
	client          proto.ShardDistributorClient
	conn            *grpc.ClientConn
	activeShards    map[string]*ShardHandler
	standbyShards   map[string]*ShardHandler
	status          proto.StatusReport_Status
	isShuttingDown  bool
	stopCh          chan struct{}

	logger *zap.Logger
}

// ShardHandler manages processing for a single shard
type ShardHandler struct {
	shardID     string
	isActive    bool
	isStandby   bool
	dataStore   map[string]interface{}
	lastUpdated time.Time
}

// NewServiceInstance creates a new service instance
func NewServiceInstance(instanceID, endpoint, distributorAddr string, logger *zap.Logger) (*ServiceInstance, error) {
	// Create a connection to the distributor
	conn, err := grpc.NewClient(distributorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to distributor: %v", err)
	}

	client := proto.NewShardDistributorClient(conn)

	instance := &ServiceInstance{
		instanceID:      instanceID,
		endpoint:        endpoint,
		distributorAddr: distributorAddr,
		client:          client,
		conn:            conn,
		activeShards:    make(map[string]*ShardHandler),
		standbyShards:   make(map[string]*ShardHandler),
		status:          proto.StatusReport_ACTIVE,
		stopCh:          make(chan struct{}),
		logger:          logger,
	}

	return instance, nil
}

func (si *ServiceInstance) InstanceID() string {
	return si.instanceID
}

// Start registers the instance and starts watching for shard assignments
func (si *ServiceInstance) Start(ctx context.Context) error {
	// Register with the distributor
	resp, err := si.client.RegisterInstance(ctx, &proto.InstanceInfo{
		InstanceId: si.instanceID,
		Endpoint:   si.endpoint,
		Capacity:   100,
		Metadata: map[string]string{
			"region": "us-west",
			"zone":   "us-west-1a",
		},
	})

	if err != nil {
		return fmt.Errorf("register instance: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	si.logger.Info("Instance registered successfully", zap.String("instance", si.instanceID))

	// Start watching for shard assignments
	go si.watchShardAssignments(ctx)

	// Start reporting status periodically
	go si.reportStatus(ctx)

	// Activate any initially assigned shards
	for _, shardID := range resp.AssignedShards {
		si.activateShard(shardID)
	}

	return nil
}

// watchShardAssignments opens a stream to receive shard assignments
func (si *ServiceInstance) watchShardAssignments(ctx context.Context) {
	for {
		select {
		case <-si.stopCh:
			return
		default:
			// Create watch stream
			stream, err := si.client.WatchShardAssignments(ctx, &proto.InstanceInfo{
				InstanceId: si.instanceID,
				Endpoint:   si.endpoint,
			})

			if err != nil {
				si.logger.Warn("Error creating shard assignment stream", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			si.logger.Info("Started watching for shard assignments")

			// Process shard assignments
			for {
				assignment, err := stream.Recv()
				if err != nil {
					si.logger.Warn("Error receiving shard assignment", zap.Error(err))
					break
				}

				// Special "shutdown" marker
				if assignment.ShardId == "shutdown" {
					si.logger.Info("Received shutdown signal from distributor")
					return
				}

				// Process the assignment
				si.logger.Info("Received shard assignment",
					zap.String("shard", assignment.ShardId), zap.Stringer("action", assignment.Action))

				switch assignment.Action {
				case proto.ShardAssignment_ASSIGN:
					si.activateShard(assignment.ShardId)
				case proto.ShardAssignment_PREPARE:
					si.prepareShard(assignment.ShardId)
				case proto.ShardAssignment_REVOKE:
					si.deactivateShard(assignment.ShardId)
				}
			}

			// If we get here, the stream was broken - try to reconnect after a delay
			time.Sleep(time.Second)
		}
	}
}

// reportStatus periodically reports instance status to the distributor
func (si *ServiceInstance) reportStatus(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-si.stopCh:
			return
		case <-ticker.C:
			si.mu.RLock()
			activeCount := len(si.activeShards)
			standbyCount := len(si.standbyShards)
			status := si.status
			si.mu.RUnlock()

			// Send status report
			_, err := si.client.ReportStatus(ctx, &proto.StatusReport{
				InstanceId:        si.instanceID,
				Status:            status,
				CpuUsage:          0.5, // Simulated CPU usage
				MemoryUsage:       0.4, // Simulated memory usage
				ActiveShardCount:  int32(activeCount),
				StandbyShardCount: int32(standbyCount),
				CustomMetrics: map[string]float64{
					"qps": 100.0, // Simulated QPS
				},
			})

			if err != nil {
				si.logger.Error("Error reporting status", zap.Error(err))
			}
		}
	}
}

// activateShard activates a shard for processing
func (si *ServiceInstance) activateShard(shardID string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Skip if already active
	if _, exists := si.activeShards[shardID]; exists {
		return
	}

	var handler *ShardHandler
	startTime := time.Now()

	// Check if we have a standby handler
	if standby, exists := si.standbyShards[shardID]; exists {
		// Promote standby to active
		handler = standby
		handler.isActive = true
		handler.isStandby = false
		delete(si.standbyShards, shardID)
		si.logger.Info("Fast activation of shard",
			zap.String("shard", shardID), zap.Duration("latency", time.Since(startTime)))
	} else {
		// Create new handler
		handler = &ShardHandler{
			shardID:     shardID,
			isActive:    true,
			isStandby:   false,
			dataStore:   make(map[string]interface{}),
			lastUpdated: time.Now(),
		}
		si.logger.Info("Regular activation of shard %s", zap.String("shard", shardID))

		// Simulate loading initial state
		time.Sleep(50 * time.Millisecond)
	}

	si.activeShards[shardID] = handler

	// Start simulated processing in a goroutine
	go si.processShard(shardID)
}

// prepareShard prepares a shard for fast activation
func (si *ServiceInstance) prepareShard(shardID string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Skip if already active or standby
	if _, exists := si.activeShards[shardID]; exists {
		return
	}
	if _, exists := si.standbyShards[shardID]; exists {
		return
	}

	// Create standby handler
	handler := &ShardHandler{
		shardID:     shardID,
		isActive:    false,
		isStandby:   true,
		dataStore:   make(map[string]interface{}),
		lastUpdated: time.Now(),
	}

	si.standbyShards[shardID] = handler
	si.logger.Info("Prepared shard for fast activation", zap.String("shard", shardID))

	// Simulate pre-loading data in background
	go func() {
		// Simulate work
		time.Sleep(100 * time.Millisecond)

		si.mu.Lock()
		defer si.mu.Unlock()

		if h, exists := si.standbyShards[shardID]; exists {
			h.dataStore["preloaded"] = true
		}
	}()
}

// deactivateShard deactivates a shard
func (si *ServiceInstance) deactivateShard(shardID string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	handler, exists := si.activeShards[shardID]
	if !exists {
		return
	}

	// Mark as inactive
	handler.isActive = false
	delete(si.activeShards, shardID)

	// Keep in standby for a short time in case of rapid reassignment
	handler.isStandby = true
	si.standbyShards[shardID] = handler
	si.logger.Info("Deactivated shard (keeping in standby)", zap.String("shard", shardID))

	// Schedule cleanup after a delay
	go func() {
		time.Sleep(30 * time.Second)

		si.mu.Lock()
		defer si.mu.Unlock()

		// Clean up if still in standby
		if standby, exists := si.standbyShards[shardID]; exists && standby.isStandby {
			delete(si.standbyShards, shardID)
			si.logger.Info("Cleaned up standby shard", zap.String("shard", shardID))
		}
	}()
}

// processShard simulates processing work for a shard
func (si *ServiceInstance) processShard(shardID string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-si.stopCh:
			return
		case <-ticker.C:
			si.mu.RLock()
			handler, exists := si.activeShards[shardID]
			isActive := exists && handler.isActive
			si.mu.RUnlock()

			if !isActive {
				return
			}

			// Simulate work
			si.logger.Info("Processing work for shard", zap.String("shard", shardID))
		}
	}
}

// Shutdown gracefully shuts down the instance
func (si *ServiceInstance) Shutdown(ctx context.Context) error {
	si.mu.Lock()
	if si.isShuttingDown {
		si.mu.Unlock()
		return nil
	}

	si.isShuttingDown = true
	si.status = proto.StatusReport_DRAINING
	si.mu.Unlock()

	si.logger.Info("Beginning graceful shutdown of instance", zap.String("instance", si.instanceID))

	// Report draining status
	_, err := si.client.ReportStatus(ctx, &proto.StatusReport{
		InstanceId:        si.instanceID,
		Status:            proto.StatusReport_DRAINING,
		ActiveShardCount:  int32(len(si.activeShards)),
		StandbyShardCount: int32(len(si.standbyShards)),
	})

	if err != nil {
		si.logger.Warn("Error reporting draining status", zap.Error(err))
	}

	// Wait for shards to be reassigned or timeout
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		si.mu.RLock()
		shardCount := len(si.activeShards)
		si.mu.RUnlock()

		if shardCount == 0 {
			break
		}

		// Brief wait
		time.Sleep(100 * time.Millisecond)
	}

	// Signal all goroutines to stop
	close(si.stopCh)

	// Deregister from distributor
	_, err = si.client.DeregisterInstance(ctx, &proto.InstanceInfo{
		InstanceId: si.instanceID,
		Endpoint:   si.endpoint,
	})

	if err != nil {
		si.logger.Error("Error deregistering instance", zap.Error(err))
	}

	// Close connection
	if si.conn != nil {
		si.conn.Close()
	}

	si.logger.Info("Instance shut down", zap.String("instance", si.instanceID))
	return nil
}
