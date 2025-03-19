package client

import (
	"context"
	"fmt"
	"io"
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
	distributorAddr string
	client          proto.ShardDistributorClient
	conn            *grpc.ClientConn
	stream          proto.ShardDistributor_ShardDistributorStreamClient
	activeShards    map[string]*ShardHandler
	standbyShards   map[string]*ShardHandler
	shardVersions   map[string]int64
	status          proto.StatusReport_Status
	isShuttingDown  bool
	stopCh          chan struct{}
	streamDoneCh    chan struct{}
	reconnectCh     chan struct{}
	handoverTimings map[string]time.Time // Track handover timings

	// Metrics for graceful transfer
	transferLatencies []time.Duration

	logger *zap.Logger
}

// ShardHandler manages processing for a single shard
type ShardHandler struct {
	shardID      string
	isActive     bool
	isStandby    bool
	dataStore    map[string]interface{}
	lastUpdated  time.Time
	preparedAt   time.Time       // When this shard was prepared in standby mode
	activatedAt  time.Time       // When this shard was activated
	workItems    map[string]bool // Simulate work items in progress
	processingMu sync.Mutex      // Mutex for work items
	version      int64           // Current version of this shard assignment
}

// NewServiceInstance creates a new service instance
func NewServiceInstance(instanceID, distributorAddr string, logger *zap.Logger) (*ServiceInstance, error) {
	// Create a connection to the distributor
	conn, err := grpc.Dial(
		distributorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to distributor: %v", err)
	}

	client := proto.NewShardDistributorClient(conn)

	instance := &ServiceInstance{
		instanceID:        instanceID,
		distributorAddr:   distributorAddr,
		client:            client,
		conn:              conn,
		activeShards:      make(map[string]*ShardHandler),
		standbyShards:     make(map[string]*ShardHandler),
		shardVersions:     make(map[string]int64),
		status:            proto.StatusReport_ACTIVE,
		stopCh:            make(chan struct{}),
		streamDoneCh:      make(chan struct{}),
		reconnectCh:       make(chan struct{}, 1),
		handoverTimings:   make(map[string]time.Time),
		transferLatencies: make([]time.Duration, 0),
		logger:            logger,
	}

	return instance, nil
}

func (si *ServiceInstance) InstanceID() string {
	return si.instanceID
}

// Start registers the instance and starts the stream
func (si *ServiceInstance) Start(ctx context.Context) error {
	// Start the streaming connection
	if err := si.startStream(ctx); err != nil {
		return err
	}

	// Start the goroutine to handle reconnections
	go si.reconnectLoop(ctx)

	// Start the goroutine to send periodic heartbeats
	go si.heartbeatLoop(ctx)

	// Start sending periodic status reports
	go si.reportStatus(ctx)

	return nil
}

// startStream establishes the bidirectional stream and registers the instance
func (si *ServiceInstance) startStream(ctx context.Context) error {
	// Create the stream
	stream, err := si.client.ShardDistributorStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	si.stream = stream

	// Register with the server
	err = stream.Send(&proto.ClientMessage{
		InstanceId: si.instanceID,
		Type:       proto.ClientMessage_REGISTER,
		InstanceInfo: &proto.InstanceInfo{
			InstanceId: si.instanceID,
			Capacity:   100,
			Metadata: map[string]string{
				"region": "us-west",
				"zone":   "us-west-1a",
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to send registration: %w", err)
	}

	// Wait for registration response
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive registration response: %w", err)
	}

	if resp.Type != proto.ServerMessage_REGISTER_RESPONSE {
		return fmt.Errorf("unexpected response type: %v", resp.Type)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	si.logger.Info("Instance registered successfully",
		zap.String("instance", si.instanceID),
		zap.Int64("lease_id", resp.LeaseId))

	// Start watching for shard assignments
	err = stream.Send(&proto.ClientMessage{
		InstanceId: si.instanceID,
		Type:       proto.ClientMessage_WATCH,
	})

	if err != nil {
		return fmt.Errorf("failed to send watch request: %w", err)
	}

	si.logger.Info("Started watching for shard assignments")

	// Start a goroutine to handle messages from the server
	go si.handleServerMessages(stream)

	// Activate any initially assigned shards
	for _, shardID := range resp.AssignedShards {
		si.activateShard(shardID, 0) // Use version 0 for initial assignments
	}

	return nil
}

// handleServerMessages processes messages coming from the server
func (si *ServiceInstance) handleServerMessages(stream proto.ShardDistributor_ShardDistributorStreamClient) {
	defer close(si.streamDoneCh)

	var inReconciliation bool

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				si.logger.Info("Stream closed by server")
			} else {
				si.logger.Warn("Error receiving from server", zap.Error(err))
			}
			select {
			case <-si.stopCh:
				// We're shutting down, so this is expected
				return
			default:
				// Signal reconnection needed
				select {
				case si.reconnectCh <- struct{}{}:
				default:
					// Channel already has a pending reconnect
				}
				return
			}
		}

		// Process different message types
		switch msg.Type {
		case proto.ServerMessage_SHARD_ASSIGNMENT:
			// Handle special reconciliation messages
			if msg.IsReconciliation {
				if msg.ShardId == "reconciliation-start" {
					inReconciliation = true
					si.logger.Info("Starting shard ownership reconciliation")
					continue
				} else if msg.ShardId == "reconciliation-end" {
					inReconciliation = false
					si.logger.Info("Completed shard ownership reconciliation")
					continue
				}
			}

			// Log appropriately based on reconciliation status
			if inReconciliation {
				si.logger.Info("Received reconciliation assignment",
					zap.String("shard", msg.ShardId),
					zap.Stringer("action", msg.Action),
					zap.Int64("version", msg.Version))
			} else {
				si.logger.Info("Received shard assignment",
					zap.String("shard", msg.ShardId),
					zap.Stringer("action", msg.Action),
					zap.String("source", msg.SourceInstanceId),
					zap.Int64("version", msg.Version))
			}

			// Process the assignment based on action
			switch msg.Action {
			case proto.ShardAssignmentAction_ASSIGN:
				// Check version before processing
				si.mu.RLock()
				currentVersion, exists := si.shardVersions[msg.ShardId]
				si.mu.RUnlock()

				if exists && currentVersion > msg.Version && !msg.IsReconciliation {
					si.logger.Info("Ignoring outdated shard assignment",
						zap.String("shard", msg.ShardId),
						zap.Int64("current_version", currentVersion),
						zap.Int64("received_version", msg.Version))
					continue
				}

				// Update version
				si.mu.Lock()
				si.shardVersions[msg.ShardId] = msg.Version
				si.mu.Unlock()

				// Record handover timing if this is a transfer
				if msg.SourceInstanceId != "" {
					si.recordHandoverStart(msg.ShardId)
				}

				// Activate shard
				si.activateShard(msg.ShardId, msg.Version)

				// Send acknowledgment
				err = stream.Send(&proto.ClientMessage{
					InstanceId: si.instanceID,
					Type:       proto.ClientMessage_ACK,
					ShardId:    msg.ShardId,
				})
				if err != nil {
					si.logger.Warn("Failed to send assignment ACK", zap.Error(err))
				}

			case proto.ShardAssignmentAction_PREPARE:
				si.prepareShard(msg.ShardId, msg.Version)

				// Send acknowledgment
				err = stream.Send(&proto.ClientMessage{
					InstanceId: si.instanceID,
					Type:       proto.ClientMessage_ACK,
					ShardId:    msg.ShardId,
				})
				if err != nil {
					si.logger.Warn("Failed to send prepare ACK", zap.Error(err))
				}

			case proto.ShardAssignmentAction_REVOKE:
				si.deactivateShard(msg.ShardId)

				// Send acknowledgment
				err = stream.Send(&proto.ClientMessage{
					InstanceId: si.instanceID,
					Type:       proto.ClientMessage_ACK,
					ShardId:    msg.ShardId,
				})
				if err != nil {
					si.logger.Warn("Failed to send revoke ACK", zap.Error(err))
				}
			}

		case proto.ServerMessage_HEARTBEAT_ACK:
			// Nothing to do, heartbeat acknowledged

		case proto.ServerMessage_STATUS_RESPONSE:
			si.logger.Debug("Received status response",
				zap.Bool("success", msg.Success),
				zap.String("message", msg.Message))

		default:
			si.logger.Warn("Received unexpected message type",
				zap.Int32("type", int32(msg.Type)))
		}
	}
}

// reconnectLoop handles reconnection when the stream is interrupted
func (si *ServiceInstance) reconnectLoop(ctx context.Context) {
	for {
		select {
		case <-si.stopCh:
			return
		case <-si.reconnectCh:
			// Wait for the current stream handling to finish
			<-si.streamDoneCh

			// Don't reconnect if we're shutting down
			if si.isShuttingDown {
				return
			}

			// Exponential backoff for reconnection
			backoff := 1 * time.Second
			maxBackoff := 30 * time.Second
			maxAttempts := 10
			attempts := 0

			for attempts < maxAttempts {
				attempts++
				si.logger.Info("Attempting to reconnect",
					zap.Int("attempt", attempts),
					zap.Duration("backoff", backoff))

				// Try to reconnect
				err := si.startStream(ctx)
				if err == nil {
					si.logger.Info("Reconnected successfully")
					break
				}

				si.logger.Warn("Failed to reconnect",
					zap.Error(err),
					zap.Int("attempt", attempts))

				// If max attempts reached, give up
				if attempts >= maxAttempts {
					si.logger.Error("Max reconnection attempts reached, giving up")
					return
				}

				// Wait before next attempt
				select {
				case <-si.stopCh:
					return
				case <-time.After(backoff):
					// Increase backoff for next attempt
					backoff = min(backoff*2, maxBackoff)
				}
			}
		}
	}
}

// heartbeatLoop sends periodic heartbeats to keep the connection alive
func (si *ServiceInstance) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-si.stopCh:
			return
		case <-ticker.C:
			if si.stream == nil {
				continue
			}

			err := si.stream.Send(&proto.ClientMessage{
				InstanceId: si.instanceID,
				Type:       proto.ClientMessage_HEARTBEAT,
			})

			if err != nil {
				si.logger.Warn("Failed to send heartbeat", zap.Error(err))
				// Signal reconnection needed
				select {
				case si.reconnectCh <- struct{}{}:
				default:
					// Channel already has a pending reconnect
				}
			}
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
			if si.stream == nil {
				continue
			}

			si.mu.RLock()
			activeCount := len(si.activeShards)
			standbyCount := len(si.standbyShards)
			status := si.status
			si.mu.RUnlock()

			// Send status report
			err := si.stream.Send(&proto.ClientMessage{
				InstanceId: si.instanceID,
				Type:       proto.ClientMessage_STATUS_REPORT,
				Status: &proto.StatusReport{
					InstanceId:        si.instanceID,
					Status:            status,
					CpuUsage:          0.5, // Simulated CPU usage
					MemoryUsage:       0.4, // Simulated memory usage
					ActiveShardCount:  int32(activeCount),
					StandbyShardCount: int32(standbyCount),
					CustomMetrics: map[string]float64{
						"qps": 100.0, // Simulated QPS
					},
				},
			})

			if err != nil {
				si.logger.Warn("Failed to send status report", zap.Error(err))
				// Signal reconnection needed
				select {
				case si.reconnectCh <- struct{}{}:
				default:
					// Channel already has a pending reconnect
				}
			}
		}
	}
}

// recordHandoverStart records the start time of a handover
func (si *ServiceInstance) recordHandoverStart(shardID string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.handoverTimings[shardID] = time.Now()
}

// recordHandoverCompletion calculates and records the handover latency
func (si *ServiceInstance) recordHandoverCompletion(shardID string) {
	si.mu.Lock()
	defer si.mu.Unlock()

	if startTime, exists := si.handoverTimings[shardID]; exists {
		latency := time.Since(startTime)
		si.transferLatencies = append(si.transferLatencies, latency)
		si.logger.Info("Completed shard handover",
			zap.String("shard", shardID),
			zap.Duration("latency", latency))
		delete(si.handoverTimings, shardID)
	}
}

// activateShard activates a shard for processing
func (si *ServiceInstance) activateShard(shardID string, version int64) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Skip if already active with same or newer version
	if handler, exists := si.activeShards[shardID]; exists {
		if handler.version >= version {
			return
		}
	}

	var handler *ShardHandler
	startTime := time.Now()

	// Check if we have a standby handler
	if standby, exists := si.standbyShards[shardID]; exists {
		// Promote standby to active
		handler = standby
		handler.isActive = true
		handler.isStandby = false
		handler.activatedAt = time.Now()
		handler.version = version
		delete(si.standbyShards, shardID)
		si.logger.Info("Fast activation of shard",
			zap.String("shard", shardID),
			zap.Duration("prepare_to_active_latency", time.Since(handler.preparedAt)),
			zap.Duration("activation_latency", time.Since(startTime)))

		// Record handover completion
		si.recordHandoverCompletion(shardID)
	} else {
		// Create new handler
		handler = &ShardHandler{
			shardID:     shardID,
			isActive:    true,
			isStandby:   false,
			dataStore:   make(map[string]interface{}),
			lastUpdated: time.Now(),
			activatedAt: time.Now(),
			workItems:   make(map[string]bool),
			version:     version,
		}
		si.logger.Info("Regular activation of shard", zap.String("shard", shardID))

		// Simulate loading initial state
		time.Sleep(50 * time.Millisecond)
	}

	si.activeShards[shardID] = handler

	// Start simulated processing in a goroutine
	go si.processShard(shardID)
}

// prepareShard prepares a shard for fast activation
func (si *ServiceInstance) prepareShard(shardID string, version int64) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Skip if already active or standby with same or newer version
	if _, exists := si.activeShards[shardID]; exists {
		return
	}
	if standby, exists := si.standbyShards[shardID]; exists && standby.version >= version {
		return
	}

	// Create standby handler
	handler := &ShardHandler{
		shardID:     shardID,
		isActive:    false,
		isStandby:   true,
		dataStore:   make(map[string]interface{}),
		lastUpdated: time.Now(),
		preparedAt:  time.Now(),
		workItems:   make(map[string]bool),
		version:     version,
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
	ticker := time.NewTicker(500 * time.Millisecond) // Process work more frequently
	defer ticker.Stop()

	workItemCounter := 0

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

			// Simulate creating new work items
			handler.processingMu.Lock()
			workItemID := fmt.Sprintf("%s-work-%d", shardID, workItemCounter)
			handler.workItems[workItemID] = true
			workItemCounter++

			// Simulate completing old work items
			for itemID := range handler.workItems {
				// 50% chance to complete a work item
				if len(handler.workItems) > 5 && workItemCounter%2 == 0 {
					delete(handler.workItems, itemID)
					break
				}
			}

			inProgressCount := len(handler.workItems)
			handler.processingMu.Unlock()

			// Log work processing
			si.logger.Info("Processing work for shard",
				zap.String("shard", shardID),
				zap.Int("work_items_in_progress", inProgressCount))
		}
	}
}

// GetTransferLatencyStats returns statistics about shard transfers
func (si *ServiceInstance) GetTransferLatencyStats() (min, max, avg time.Duration, count int) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	if len(si.transferLatencies) == 0 {
		return 0, 0, 0, 0
	}

	min = si.transferLatencies[0]
	max = si.transferLatencies[0]
	var sum time.Duration

	for _, latency := range si.transferLatencies {
		if latency < min {
			min = latency
		}
		if latency > max {
			max = latency
		}
		sum += latency
	}

	avg = sum / time.Duration(len(si.transferLatencies))
	return min, max, avg, len(si.transferLatencies)
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
	if si.stream != nil {
		err := si.stream.Send(&proto.ClientMessage{
			InstanceId: si.instanceID,
			Type:       proto.ClientMessage_STATUS_REPORT,
			Status: &proto.StatusReport{
				InstanceId:        si.instanceID,
				Status:            proto.StatusReport_DRAINING,
				ActiveShardCount:  int32(len(si.activeShards)),
				StandbyShardCount: int32(len(si.standbyShards)),
			},
		})

		if err != nil {
			si.logger.Warn("Error reporting draining status", zap.Error(err))
		}
	}

	// Print transfer latency stats
	min, max, avg, count := si.GetTransferLatencyStats()
	if count > 0 {
		si.logger.Info("Shard transfer statistics",
			zap.Int("count", count),
			zap.Duration("min_latency", min),
			zap.Duration("max_latency", max),
			zap.Duration("avg_latency", avg))
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

	// Send deregistration message
	if si.stream != nil {
		err := si.stream.Send(&proto.ClientMessage{
			InstanceId: si.instanceID,
			Type:       proto.ClientMessage_DEREGISTER,
		})

		if err != nil {
			si.logger.Warn("Error sending deregistration", zap.Error(err))
		} else {
			// Wait for deregistration response
			resp, err := si.stream.Recv()
			if err == nil && resp.Type == proto.ServerMessage_DEREGISTER_RESPONSE {
				si.logger.Info("Deregistration response received",
					zap.Bool("success", resp.Success),
					zap.String("message", resp.Message))
			}
		}
	}

	// Signal all goroutines to stop
	close(si.stopCh)

	// Close connection
	if si.conn != nil {
		si.conn.Close()
	}

	si.logger.Info("Instance shut down", zap.String("instance", si.instanceID))
	return nil
}
