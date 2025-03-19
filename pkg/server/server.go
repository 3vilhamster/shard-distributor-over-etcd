package server

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/sharding"
)

type Distributor interface {
	CalculateDistribution(currentMap map[string]string, instances map[string]sharding.InstanceInfo) map[string]string
}

// ShardDistributorServer implements the ShardDistributor gRPC service
type ShardDistributorServer struct {
	proto.UnimplementedShardDistributorServer
	mu               sync.RWMutex
	etcdClient       *clientv3.Client
	instanceStreams  map[string][]proto.ShardDistributor_ShardDistributorStreamServer
	instances        map[string]*InstanceData
	shardAssignments map[string]string // shardID -> instanceID
	hashStrategy     Distributor
	isLeader         bool
	election         *concurrency.Election
	leaderChan       chan bool
	session          *concurrency.Session
	logger           *zap.Logger
	stopChan         chan struct{}

	// Version tracking
	globalVersionKey string           // etcd key for storing the global version
	shardVersions    map[string]int64 // Local cache of shard versions
}

// InstanceData contains information about a service instance
type InstanceData struct {
	Info          *proto.InstanceInfo
	Status        *proto.StatusReport
	LastHeartbeat time.Time
	LeaseID       clientv3.LeaseID
}

// initializeGlobalVersion initializes the global version in etcd if it doesn't exist
func initializeGlobalVersion(etcdClient *clientv3.Client, logger *zap.Logger, key string) error {
	// Use a transaction for atomic check-and-set
	txn := etcdClient.Txn(context.Background())

	// If the key doesn't exist
	txn = txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0))

	// Then create it with initial value "0"
	txn = txn.Then(clientv3.OpPut(key, "0"))

	// Otherwise do nothing
	txn = txn.Else()

	// Execute the transaction
	txnResp, err := txn.Commit()
	if err != nil {
		logger.Warn("Failed to initialize global version", zap.Error(err))
		return err
	}

	if txnResp.Succeeded {
		logger.Info("Initialized global version to 0")
	} else {
		// Key already exists, get its current value for logging
		resp, err := etcdClient.Get(context.Background(), key)
		if err != nil {
			logger.Warn("Failed to get existing global version", zap.Error(err))
		} else if len(resp.Kvs) > 0 {
			logger.Info("Global version already initialized",
				zap.String("value", string(resp.Kvs[0].Value)))
		}
	}

	return nil
}

// NewShardDistributorServer creates a new shard distributor server
func NewShardDistributorServer(logger *zap.Logger, etcdClient *clientv3.Client) (*ShardDistributorServer, error) {
	// Create a session for leader election with longer TTL for stability
	session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(15))
	if err != nil {
		return nil, fmt.Errorf("create etcd session: %w", err)
	}

	// Create election
	election := concurrency.NewElection(session, "/shard-distributor/leader")

	server := &ShardDistributorServer{
		etcdClient:       etcdClient,
		instanceStreams:  make(map[string][]proto.ShardDistributor_ShardDistributorStreamServer),
		instances:        make(map[string]*InstanceData),
		shardAssignments: make(map[string]string),
		hashStrategy:     sharding.NewConsistentHashStrategy(10), // Use consistent hashing with 10 virtual nodes
		election:         election,
		leaderChan:       make(chan bool, 1),
		session:          session,
		logger:           logger,
		stopChan:         make(chan struct{}),
		globalVersionKey: "/shard-distributor/global-version",
		shardVersions:    make(map[string]int64),
	}

	// Initialize global version atomically
	if err := initializeGlobalVersion(etcdClient, logger, server.globalVersionKey); err != nil {
		logger.Warn("Could not initialize global version", zap.Error(err))
		// Continue anyway, as we'll read the value later
	}

	// Load current version from etcd
	resp, err := etcdClient.Get(context.Background(), server.globalVersionKey)
	if err != nil {
		logger.Warn("Failed to get global version from etcd", zap.Error(err))
	} else if len(resp.Kvs) > 0 {
		globalVersion, err := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			logger.Warn("Failed to parse global version", zap.Error(err))
		} else {
			logger.Info("Loaded global version from etcd", zap.Int64("version", globalVersion))
		}
	}

	// Load existing instances from etcd
	instancesResp, err := etcdClient.Get(context.Background(), "/services/", clientv3.WithPrefix())
	if err != nil {
		logger.Warn("Failed to load existing instances", zap.Error(err))
	} else {
		for _, kv := range instancesResp.Kvs {
			instanceID := string(kv.Key)[len("/services/"):]
			endpoint := string(kv.Value)

			server.instances[instanceID] = &InstanceData{
				Info: &proto.InstanceInfo{
					InstanceId: instanceID,
					Capacity:   100, // Default until instance reports
				},
				Status: &proto.StatusReport{
					InstanceId: instanceID,
					Status:     proto.StatusReport_ACTIVE,
				},
				LastHeartbeat: time.Now(),
			}

			logger.Info("Loaded existing instance",
				zap.String("instance", instanceID),
				zap.String("endpoint", endpoint))
		}
	}

	// Start leadership campaign
	go server.campaignForLeadership()

	// Start instance health check
	go server.checkInstanceHealth()

	// Start watching for instance changes in etcd
	go server.watchInstances(context.Background())

	// Start watching global version changes
	go server.watchGlobalVersion(context.Background())

	// Start periodic reconciliation
	go server.reconcileShardOwnership()

	return server, nil
}

// ShardDistributorStream implements the unified bidirectional streaming RPC
func (s *ShardDistributorServer) ShardDistributorStream(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
) error {
	var instanceID string
	var registered bool

	// Extract peer information for logging
	peer, _ := peer.FromContext(stream.Context())
	peerAddr := "unknown"
	if peer != nil {
		peerAddr = peer.Addr.String()
	}

	s.logger.Info("New client stream connection", zap.String("peer", peerAddr))

	// Process messages from the client
	for {
		// Receive the next message
		clientMsg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.logger.Info("Client closed stream",
					zap.String("instance", instanceID),
					zap.String("peer", peerAddr))
			} else {
				s.logger.Warn("Error receiving from client stream",
					zap.String("instance", instanceID),
					zap.String("peer", peerAddr),
					zap.Error(err))
			}

			// If this was a registered instance, handle the disconnection
			if registered && instanceID != "" {
				s.handleInstanceDisconnect(instanceID)
			}

			return err
		}

		// Get the instance ID from the message
		if clientMsg.InstanceId != "" {
			if instanceID == "" {
				instanceID = clientMsg.InstanceId
			} else if instanceID != clientMsg.InstanceId {
				s.logger.Warn("Instance ID changed in stream",
					zap.String("old_id", instanceID),
					zap.String("new_id", clientMsg.InstanceId),
					zap.String("peer", peerAddr))
				return status.Error(codes.InvalidArgument, "instance ID cannot change within a stream")
			}
		}

		// Process the message based on its type
		switch clientMsg.Type {
		case proto.ClientMessage_REGISTER:
			resp, err := s.handleRegister(stream.Context(), clientMsg, stream, peerAddr)
			if err != nil {
				s.logger.Error("Failed to handle registration",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			if resp.Success {
				registered = true

				// Store the stream for this instance
				s.mu.Lock()
				if _, exists := s.instanceStreams[instanceID]; !exists {
					s.instanceStreams[instanceID] = make([]proto.ShardDistributor_ShardDistributorStreamServer, 0)
				}
				s.instanceStreams[instanceID] = append(s.instanceStreams[instanceID], stream)
				s.mu.Unlock()
			}

		case proto.ClientMessage_DEREGISTER:
			resp, err := s.handleDeregister(stream.Context(), clientMsg)
			if err != nil {
				s.logger.Error("Failed to handle deregistration",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			if resp.Success {
				registered = false

				// Send the response
				err = stream.Send(&proto.ServerMessage{
					Type:    proto.ServerMessage_DEREGISTER_RESPONSE,
					Success: true,
					Message: "Instance deregistered successfully",
				})

				// Disconnect after successful deregistration
				return nil
			}

		case proto.ClientMessage_WATCH:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before watching")
			}

			err = s.handleWatch(clientMsg, stream)
			if err != nil {
				s.logger.Error("Failed to handle watch",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

		case proto.ClientMessage_HEARTBEAT:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before sending heartbeats")
			}

			s.handleHeartbeat(instanceID)

			// Send heartbeat acknowledgment
			err = stream.Send(&proto.ServerMessage{
				Type: proto.ServerMessage_HEARTBEAT_ACK,
			})
			if err != nil {
				s.logger.Warn("Failed to send heartbeat acknowledgment",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

		case proto.ClientMessage_ACK:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before sending ACKs")
			}

			s.handleAck(instanceID, clientMsg.ShardId)

		case proto.ClientMessage_STATUS_REPORT:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before reporting status")
			}

			resp, err := s.handleStatusReport(stream.Context(), clientMsg.Status)
			if err != nil {
				s.logger.Error("Failed to handle status report",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			// Send the response
			err = stream.Send(&proto.ServerMessage{
				Type:    proto.ServerMessage_STATUS_RESPONSE,
				Success: resp.Success,
				Message: resp.Message,
			})
			if err != nil {
				s.logger.Warn("Failed to send status response",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}
		}
	}
}

// handleRegister processes a register message
func (s *ShardDistributorServer) handleRegister(
	ctx context.Context,
	msg *proto.ClientMessage,
	stream proto.ShardDistributor_ShardDistributorStreamServer,
	peerAddr string,
) (*proto.ServerMessage, error) {
	instanceID := msg.InstanceId
	instanceInfo := msg.InstanceInfo

	if instanceInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "instance_info required for registration")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Instance registering",
		zap.String("instance", instanceID),
		zap.String("peer", peerAddr))

	// Create lease with TTL from metadata or default to 30s
	ttl := int64(30)
	if instanceInfo.Metadata != nil {
		if ttlStr, ok := instanceInfo.Metadata["leaseTTL"]; ok {
			if parsedTTL, err := strconv.ParseInt(ttlStr, 10, 64); err == nil {
				ttl = parsedTTL
			}
		}
	}

	lease, err := s.etcdClient.Grant(ctx, ttl)
	if err != nil {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_REGISTER_RESPONSE,
			Success: false,
			Message: fmt.Sprintf("Failed to create lease in etcd: %v", err),
		}, nil
	}

	// Store instance information with lease ID
	s.instances[instanceID] = &InstanceData{
		Info:          instanceInfo,
		Status:        &proto.StatusReport{InstanceId: instanceID, Status: proto.StatusReport_ACTIVE},
		LastHeartbeat: time.Now(),
		LeaseID:       lease.ID,
	}

	// Register in etcd
	key := fmt.Sprintf("/services/%s", instanceID)
	_, err = s.etcdClient.Put(ctx, key, peerAddr, clientv3.WithLease(lease.ID))
	if err != nil {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_REGISTER_RESPONSE,
			Success: false,
			Message: fmt.Sprintf("Failed to register in etcd: %v", err),
		}, nil
	}

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		go func() {
			// Small delay to ensure registration is complete
			time.Sleep(100 * time.Millisecond)
			s.recalculateShardDistribution()
		}()
	}

	// Find currently assigned shards for this instance
	assignedShards := []string{}
	for shardID, assignedInstanceID := range s.shardAssignments {
		if assignedInstanceID == instanceID {
			assignedShards = append(assignedShards, shardID)
		}
	}

	// Create and send response
	response := &proto.ServerMessage{
		Type:           proto.ServerMessage_REGISTER_RESPONSE,
		Success:        true,
		Message:        "Instance registered successfully",
		AssignedShards: assignedShards,
		LeaseId:        int64(lease.ID),
	}

	// Send the response
	err = stream.Send(response)
	if err != nil {
		s.logger.Warn("Failed to send registration response",
			zap.String("instance", instanceID),
			zap.Error(err))
	}

	return response, nil
}

// handleDeregister processes a deregister message
func (s *ShardDistributorServer) handleDeregister(
	ctx context.Context,
	msg *proto.ClientMessage,
) (*proto.ServerMessage, error) {
	instanceID := msg.InstanceId

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Instance deregistering", zap.String("instance", instanceID))

	// Remove from etcd
	key := fmt.Sprintf("/services/%s", instanceID)
	_, err := s.etcdClient.Delete(ctx, key)
	if err != nil {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_DEREGISTER_RESPONSE,
			Success: false,
			Message: fmt.Sprintf("Failed to deregister in etcd: %v", err),
		}, nil
	}

	// Remove from our instances map
	delete(s.instances, instanceID)

	// Close all streams to this instance
	delete(s.instanceStreams, instanceID)

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		go s.recalculateShardDistribution()
	}

	return &proto.ServerMessage{
		Type:    proto.ServerMessage_DEREGISTER_RESPONSE,
		Success: true,
		Message: "Instance deregistered successfully",
	}, nil
}

// handleWatch processes a watch request and sends initial assignments
func (s *ShardDistributorServer) handleWatch(
	msg *proto.ClientMessage,
	stream proto.ShardDistributor_ShardDistributorStreamServer,
) error {
	instanceID := msg.InstanceId

	s.logger.Info("Instance watching for shard assignments", zap.String("instance", instanceID))

	// Get current assignments for this instance
	s.mu.RLock()
	assignments := []string{}
	for shardID, assignedInstanceID := range s.shardAssignments {
		if assignedInstanceID == instanceID {
			assignments = append(assignments, shardID)
		}
	}
	s.mu.RUnlock()

	// Send initial assignments
	for _, shardID := range assignments {
		// Get the current version
		versionKey := fmt.Sprintf("/shard-versions/%s", shardID)
		versionResp, err := s.etcdClient.Get(context.Background(), versionKey)

		var version int64 = 0
		if err == nil && len(versionResp.Kvs) > 0 {
			version, _ = strconv.ParseInt(string(versionResp.Kvs[0].Value), 10, 64)
		}

		// Send the assignment
		err = stream.Send(&proto.ServerMessage{
			Type:    proto.ServerMessage_SHARD_ASSIGNMENT,
			ShardId: shardID,
			Action:  proto.ShardAssignmentAction_ASSIGN,
			Version: version,
		})

		if err != nil {
			s.logger.Error("Error sending initial assignment",
				zap.String("instance", instanceID),
				zap.String("shard", shardID),
				zap.Error(err))
			return err
		}
	}

	return nil
}

// handleHeartbeat updates the heartbeat timestamp for an instance
func (s *ShardDistributorServer) handleHeartbeat(instanceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update heartbeat timestamp
	if instance, exists := s.instances[instanceID]; exists {
		instance.LastHeartbeat = time.Now()

		// Refresh lease
		if instance.LeaseID != 0 {
			go func() {
				_, err := s.etcdClient.KeepAliveOnce(context.Background(), instance.LeaseID)
				if err != nil {
					s.logger.Warn("Failed to keep lease alive",
						zap.String("instance", instanceID),
						zap.Error(err))
				}
			}()
		}
	}
}

// handleAck processes an acknowledgment for a shard assignment
func (s *ShardDistributorServer) handleAck(instanceID string, shardID string) {
	s.logger.Debug("Received assignment acknowledgment",
		zap.String("instance", instanceID),
		zap.String("shard", shardID))

	// Here you could implement additional logic such as:
	// - Tracking confirmed assignments
	// - Updating assignment state in etcd
	// - Triggering follow-up actions
}

// handleStatusReport processes a status report
func (s *ShardDistributorServer) handleStatusReport(
	ctx context.Context,
	status *proto.StatusReport,
) (*proto.ServerMessage, error) {
	instanceID := status.InstanceId

	s.mu.Lock()
	defer s.mu.Unlock()

	// Update instance status
	instance, exists := s.instances[instanceID]
	if !exists {
		return &proto.ServerMessage{
			Type:    proto.ServerMessage_STATUS_RESPONSE,
			Success: false,
			Message: "Instance not found",
		}, nil
	}

	// Check if instance is transitioning to DRAINING
	needsRecalculation := instance.Status.Status != proto.StatusReport_DRAINING &&
		status.Status == proto.StatusReport_DRAINING

	if needsRecalculation {
		s.logger.Info("Instance transitioning to DRAINING state", zap.String("instance", instanceID))
	}

	// Update status and heartbeat
	instance.Status = status
	instance.LastHeartbeat = time.Now()

	// If instance is draining and we're the leader, recalculate distribution
	if needsRecalculation && s.isLeader {
		go s.recalculateShardDistribution()
	}

	return &proto.ServerMessage{
		Type:    proto.ServerMessage_STATUS_RESPONSE,
		Success: true,
		Message: "Status updated",
	}, nil
}

// handleInstanceDisconnect handles cleanup when an instance disconnects
func (s *ShardDistributorServer) handleInstanceDisconnect(instanceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Instance disconnected", zap.String("instance", instanceID))

	// Remove from streams map
	delete(s.instanceStreams, instanceID)

	// Note: We don't immediately remove from instances map or etcd
	// Health check will handle instance removal if it doesn't reconnect
}

// watchInstances watches for changes to registered instances in etcd
func (s *ShardDistributorServer) watchInstances(ctx context.Context) {
	watchPrefix := "/services/"
	s.logger.Info("Starting watch on instances", zap.String("prefix", watchPrefix))

	watchChan := s.etcdClient.Watch(ctx, watchPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-s.stopChan:
			return
		case watchResp := <-watchChan:
			if watchResp.Canceled {
				s.logger.Warn("Instance watch canceled, restarting...")
				// Reconnect the watch after a short delay
				time.Sleep(time.Second)
				watchChan = s.etcdClient.Watch(ctx, watchPrefix, clientv3.WithPrefix())
				continue
			}

			instancesChanged := false

			// Process events
			for _, event := range watchResp.Events {
				key := string(event.Kv.Key)
				instanceID := key[len(watchPrefix):]

				switch event.Type {
				case clientv3.EventTypePut:
					// New or updated instance
					s.mu.Lock()
					if _, exists := s.instances[instanceID]; !exists {
						// If this is a new instance we don't know about yet
						endpoint := string(event.Kv.Value)
						s.logger.Info("Discovered new instance from etcd",
							zap.String("instance", instanceID),
							zap.String("endpoint", endpoint))

						// Create a placeholder instance until it reports status
						s.instances[instanceID] = &InstanceData{
							Info: &proto.InstanceInfo{
								InstanceId: instanceID,
								Capacity:   100, // Default capacity
							},
							Status: &proto.StatusReport{
								InstanceId: instanceID,
								Status:     proto.StatusReport_ACTIVE,
							},
							LastHeartbeat: time.Now(),
						}
						instancesChanged = true
					}
					s.mu.Unlock()

				case clientv3.EventTypeDelete:
					// Instance removed
					s.mu.Lock()
					if _, exists := s.instances[instanceID]; exists {
						s.logger.Info("Instance removed from etcd", zap.String("instance", instanceID))
						delete(s.instances, instanceID)
						instancesChanged = true
					}
					s.mu.Unlock()
				}
			}

			// If we're the leader and instances changed, recalculate distribution
			if instancesChanged && s.isLeader {
				go s.recalculateShardDistribution()
			}
		}
	}
}

// watchGlobalVersion watches for changes to the global version in etcd
func (s *ShardDistributorServer) watchGlobalVersion(ctx context.Context) {
	s.logger.Info("Starting watch on global version")

	// Watch for changes to the global version
	watchChan := s.etcdClient.Watch(ctx, s.globalVersionKey)

	for {
		select {
		case <-s.stopChan:
			return
		case watchResp := <-watchChan:
			if watchResp.Canceled {
				s.logger.Warn("Global version watch canceled, restarting...")
				time.Sleep(time.Second)
				watchChan = s.etcdClient.Watch(ctx, s.globalVersionKey)
				continue
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					newVersion, err := strconv.ParseInt(string(event.Kv.Value), 10, 64)
					if err != nil {
						s.logger.Error("Failed to parse updated global version", zap.Error(err))
						continue
					}

					s.logger.Info("Global version updated", zap.Int64("new_version", newVersion))

					// If this is not the leader, refresh our view of shard assignments
					if !s.isLeader {
						go s.refreshShardAssignments(ctx)
					}
				}
			}
		}
	}
}

// refreshShardAssignments refreshes the local shard assignment view from etcd
func (s *ShardDistributorServer) refreshShardAssignments(ctx context.Context) {
	s.logger.Debug("Refreshing shard assignments from etcd")

	// Get all shard assignments
	resp, err := s.etcdClient.Get(ctx, "/shards/", clientv3.WithPrefix())
	if err != nil {
		s.logger.Error("Failed to refresh shard assignments", zap.Error(err))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Build new assignments map
	newAssignments := make(map[string]string)
	for _, kv := range resp.Kvs {
		shardID := string(kv.Key)[len("/shards/"):]
		instanceID := string(kv.Value)
		newAssignments[shardID] = instanceID
	}

	// Update local state
	s.shardAssignments = newAssignments

	s.logger.Info("Refreshed shard assignments", zap.Int("shard_count", len(newAssignments)))
}

// reconcileShardOwnership periodically sends the current shard ownership to all clients
func (s *ShardDistributorServer) reconcileShardOwnership() {
	ticker := time.NewTicker(30 * time.Second) // Reconcile every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.mu.RLock()
			// Skip if not leader
			if !s.isLeader {
				s.mu.RUnlock()
				continue
			}

			s.logger.Debug("Starting periodic shard ownership reconciliation")

			// Create a map of instance -> assigned shards for efficiency
			instanceShards := make(map[string][]string)
			for shardID, instanceID := range s.shardAssignments {
				if instanceID != "" {
					if _, exists := instanceShards[instanceID]; !exists {
						instanceShards[instanceID] = make([]string, 0)
					}
					instanceShards[instanceID] = append(instanceShards[instanceID], shardID)
				}
			}

			// Get all shard versions
			versionPrefix := "/shard-versions/"
			versionResp, err := s.etcdClient.Get(context.Background(), versionPrefix, clientv3.WithPrefix())

			// Build version map
			shardVersions := make(map[string]int64)
			if err == nil {
				for _, kv := range versionResp.Kvs {
					shardID := string(kv.Key)[len(versionPrefix):]
					version, err := strconv.ParseInt(string(kv.Value), 10, 64)
					if err == nil {
						shardVersions[shardID] = version
					}
				}
			}

			// For each instance, send its complete set of assigned shards
			for instanceID, shards := range instanceShards {
				streams, exists := s.instanceStreams[instanceID]
				if !exists || len(streams) == 0 {
					continue
				}

				// Create reconciliation message for this instance
				s.logger.Debug("Sending reconciliation to instance",
					zap.String("instance", instanceID),
					zap.Int("shard_count", len(shards)))

				// Send to all streams for this instance
				for _, stream := range streams {
					// First send a message indicating reconciliation start
					reconcileStartMsg := &proto.ServerMessage{
						Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
						ShardId:          "reconciliation-start",
						Action:           proto.ShardAssignmentAction_RECONCILE,
						IsReconciliation: true,
					}

					err := stream.Send(reconcileStartMsg)

					if err != nil {
						s.logger.Warn("Error sending reconciliation start",
							zap.String("instance", instanceID),
							zap.Error(err))
						continue
					}

					// Send all shard assignments
					for _, shardID := range shards {
						version := shardVersions[shardID] // Get version from map, defaults to 0 if not found

						assignmentMsg := &proto.ServerMessage{
							Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
							ShardId:          shardID,
							Action:           proto.ShardAssignmentAction_ASSIGN,
							IsReconciliation: true,
							Version:          version,
						}

						err := stream.Send(assignmentMsg)

						if err != nil {
							s.logger.Warn("Error sending reconciliation assignment",
								zap.String("instance", instanceID),
								zap.Error(err))
							break
						}
					}

					// End reconciliation
					reconcileEndMsg := &proto.ServerMessage{
						Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
						ShardId:          "reconciliation-end",
						Action:           proto.ShardAssignmentAction_RECONCILE,
						IsReconciliation: true,
					}

					err = stream.Send(reconcileEndMsg)

					if err != nil {
						s.logger.Warn("Error sending reconciliation end",
							zap.String("instance", instanceID),
							zap.Error(err))
					}
				}
			}

			s.mu.RUnlock()
		}
	}
}

// campaignForLeadership tries to become the leader for shard distribution
func (s *ShardDistributorServer) campaignForLeadership() {
	backoff := 1 * time.Second
	maxBackoff := 10 * time.Second

	for {
		select {
		case <-s.stopChan:
			return
		default:
			// Try to become leader
			s.logger.Debug("Campaigning for leadership")

			err := s.election.Campaign(context.Background(), "candidate")
			if err != nil {
				s.logger.Warn("Failed to campaign for leadership", zap.Error(err))
				// Use exponential backoff for retry
				time.Sleep(backoff)
				backoff = min(backoff*2, maxBackoff)
				continue
			}

			// Reset backoff on success
			backoff = 1 * time.Second

			// Get our leader key to identify our own leadership
			leaderResp, err := s.election.Leader(context.Background())
			if err != nil {
				s.logger.Warn("Failed to get leader key", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			myLeaderKey := string(leaderResp.Kvs[0].Key)

			// Successfully became leader
			s.mu.Lock()
			s.isLeader = true
			s.mu.Unlock()

			s.logger.Info("Became leader for shard distribution")

			// Recalculate distribution
			s.recalculateShardDistribution()

			// Watch for leadership changes
			watchCh := s.election.Observe(context.Background())
			leaderChanged := false
			for !leaderChanged {
				select {
				case <-s.stopChan:
					return
				case resp, ok := <-watchCh:
					if !ok {
						s.logger.Info("Watcher closed")
						leaderChanged = true
					}

					// Check if we're still the leader by comparing keys
					currentLeaderKey := string(resp.Kvs[0].Key)
					if currentLeaderKey != myLeaderKey {
						s.logger.Info("Leadership transferred to another instance",
							zap.String("current_key", currentLeaderKey),
							zap.String("my_key", myLeaderKey))
						leaderChanged = true
					}
				}
			}

			s.logger.Info("No longer leader for shard distribution")

			s.mu.Lock()
			s.isLeader = false
			s.mu.Unlock()

			// Wait before trying again to avoid rapid oscillation
			time.Sleep(3 * time.Second)
		}
	}
}

// distributionsEqual compares two shard distribution maps for equality
func distributionsEqual(a, b map[string]string) bool {
	return reflect.DeepEqual(a, b)
}

// recalculateShardDistribution recalculates shard distribution after changes
func (s *ShardDistributorServer) recalculateShardDistribution() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if not leader
	if !s.isLeader {
		return
	}

	// Debug print current assignments and instances
	s.logger.Debug("Current shard assignments", zap.Any("assignments", s.shardAssignments))

	// Log instance count and IDs for debugging
	instanceIDs := make([]string, 0, len(s.instances))
	for id := range s.instances {
		instanceIDs = append(instanceIDs, id)
	}
	s.logger.Debug("Available instances", zap.Strings("instance_ids", instanceIDs))

	// Convert instances to the format needed by the hash strategy
	activeInstances := make(map[string]sharding.InstanceInfo)
	for id, instance := range s.instances {
		status := "active"
		if instance.Status.Status == proto.StatusReport_DRAINING {
			status = "draining"
		}

		activeInstances[id] = sharding.InstanceInfo{
			ID:         id,
			Status:     status,
			LoadFactor: instance.Status.CpuUsage,
			ShardCount: int(instance.Status.ActiveShardCount),
		}
	}

	// Calculate new distribution
	newDistribution := s.hashStrategy.CalculateDistribution(s.shardAssignments, activeInstances)

	// Debug print new distribution
	s.logger.Debug("New distribution calculated", zap.Any("new_assignments", newDistribution))

	// Count assignments per instance for better visibility
	instanceAssignments := make(map[string]int)
	for _, instanceID := range newDistribution {
		instanceAssignments[instanceID]++
	}
	s.logger.Info("Shard assignment counts", zap.Any("counts", instanceAssignments))

	// Check if the distribution changed before proceeding
	if distributionsEqual(s.shardAssignments, newDistribution) {
		// No changes in distribution, skip the rest
		return
	}

	// Get the current global version from etcd
	resp, err := s.etcdClient.Get(context.Background(), s.globalVersionKey)
	if err != nil {
		s.logger.Error("Failed to get global version", zap.Error(err))
		return
	}

	var currentGlobalVersion int64 = 0
	if len(resp.Kvs) > 0 {
		currentGlobalVersion, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			s.logger.Error("Failed to parse global version", zap.Error(err))
			return
		}
	}

	// Increment global version
	newGlobalVersion := currentGlobalVersion + 1

	// Prepare transaction to update both assignments and version
	txn := s.etcdClient.Txn(context.Background())

	// First ensure the version hasn't changed (optimistic concurrency control)
	txn = txn.If(clientv3.Compare(clientv3.Value(s.globalVersionKey), "=", strconv.FormatInt(currentGlobalVersion, 10)))

	// Then update all assignments and the version
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(s.globalVersionKey, strconv.FormatInt(newGlobalVersion, 10)))

	// Add shard assignment updates
	for shardID, instanceID := range newDistribution {
		key := fmt.Sprintf("/shards/%s", shardID)
		ops = append(ops, clientv3.OpPut(key, instanceID))

		// Update version for this shard
		versionKey := fmt.Sprintf("/shard-versions/%s", shardID)
		ops = append(ops, clientv3.OpPut(versionKey, strconv.FormatInt(newGlobalVersion, 10)))
	}

	// Execute transaction
	txnResp, err := txn.Then(ops...).Commit()
	if err != nil {
		s.logger.Error("Failed to update distribution in etcd", zap.Error(err))
		return
	}

	if !txnResp.Succeeded {
		s.logger.Warn("Distribution update transaction failed, likely due to version conflict")
		// Could retry here if needed
		return
	}

	// Log distribution change summary
	changes := 0
	for shardID, newInstanceID := range newDistribution {
		oldInstanceID, exists := s.shardAssignments[shardID]
		if !exists || oldInstanceID != newInstanceID {
			changes++
		}
	}

	s.logger.Info("Distribution changed",
		zap.Int("shard_affected", changes),
		zap.Int64("new_global_version", newGlobalVersion))

	// Identify changes
	assignmentsToSend := make(map[string][]*proto.ServerMessage)
	pendingRevokes := make(map[string][]string) // instanceID -> []shardID

	// First, process new assignments
	for shardID, newInstanceID := range newDistribution {
		oldInstanceID, exists := s.shardAssignments[shardID]

		if exists && oldInstanceID != "" && oldInstanceID != newInstanceID {
			// This is a transfer - prepare new instance
			if newInstanceID != "" {
				if _, ok := assignmentsToSend[newInstanceID]; !ok {
					assignmentsToSend[newInstanceID] = make([]*proto.ServerMessage, 0)
				}

				// First prepare
				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ServerMessage{
					Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_PREPARE,
					SourceInstanceId: oldInstanceID,
					Version:          newGlobalVersion,
				})

				// Then assign
				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ServerMessage{
					Type:             proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId:          shardID,
					Action:           proto.ShardAssignmentAction_ASSIGN,
					SourceInstanceId: oldInstanceID,
					Version:          newGlobalVersion,
				})
			}

			// Mark old instance for revoke
			if _, ok := pendingRevokes[oldInstanceID]; !ok {
				pendingRevokes[oldInstanceID] = make([]string, 0)
			}
			pendingRevokes[oldInstanceID] = append(pendingRevokes[oldInstanceID], shardID)
		} else if !exists || oldInstanceID != newInstanceID {
			// This is a new assignment (not a transfer)
			if newInstanceID != "" {
				if _, ok := assignmentsToSend[newInstanceID]; !ok {
					assignmentsToSend[newInstanceID] = make([]*proto.ServerMessage, 0)
				}

				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ServerMessage{
					Type:    proto.ServerMessage_SHARD_ASSIGNMENT,
					ShardId: shardID,
					Action:  proto.ShardAssignmentAction_ASSIGN,
					Version: newGlobalVersion,
				})
			}
		}
	}

	// Update shard assignments map
	s.shardAssignments = newDistribution

	// Send notifications through gRPC streams (PREPAREs and ASSIGNs first)
	for instanceID, assignments := range assignmentsToSend {
		streams, exists := s.instanceStreams[instanceID]
		if !exists || len(streams) == 0 {
			continue
		}

		// Send to all streams for this instance
		for _, stream := range streams {
			for _, assignment := range assignments {
				err := stream.Send(assignment)
				if err != nil {
					s.logger.Warn("Error sending assignment", zap.String("instance", instanceID), zap.Error(err))
				}
			}
		}
	}

	// Send REVOKEs after a small delay to ensure new instances had time to activate
	go func(pendingRevokes map[string][]string, newGlobalVersion int64) {
		time.Sleep(500 * time.Millisecond)

		s.mu.Lock()
		defer s.mu.Unlock()

		// Now send all REVOKE messages
		for instanceID, shardIDs := range pendingRevokes {
			streams, exists := s.instanceStreams[instanceID]
			if !exists || len(streams) == 0 {
				continue
			}

			for _, shardID := range shardIDs {
				// Send to all streams for this instance
				for _, stream := range streams {
					err := stream.Send(&proto.ServerMessage{
						Type:    proto.ServerMessage_SHARD_ASSIGNMENT,
						ShardId: shardID,
						Action:  proto.ShardAssignmentAction_REVOKE,
						Version: newGlobalVersion,
					})
					if err != nil {
						s.logger.Warn("Error sending revoke", zap.String("instance", instanceID), zap.Error(err))
					}
				}
			}
		}
	}(pendingRevokes, newGlobalVersion)
}

// checkInstanceHealth periodically checks for unhealthy instances
func (s *ShardDistributorServer) checkInstanceHealth() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			unhealthy := 0

			// Check for instances that haven't sent a heartbeat recently
			for id, instance := range s.instances {
				if now.Sub(instance.LastHeartbeat) > time.Second*15 {
					// Remove from our instances map
					delete(s.instances, id)
					unhealthy++

					// Remove from etcd
					key := fmt.Sprintf("/services/%s", id)
					_, err := s.etcdClient.Delete(context.Background(), key)
					if err != nil {
						s.logger.Warn("Error removing unhealthy instance from etcd", zap.Error(err))
					}

					// Close streams
					delete(s.instanceStreams, id)
				}
			}

			needsRecalculation := unhealthy > 0 && s.isLeader
			s.mu.Unlock()

			// Recalculate if we found unhealthy instances and we're the leader
			if needsRecalculation {
				s.logger.Info("Recalculating distribution", zap.Int("unhealthy_instances", unhealthy))
				s.recalculateShardDistribution()
			}
		}
	}
}

// LoadShardDefinitions loads shard definitions from etcd or initializes them
func (s *ShardDistributorServer) LoadShardDefinitions(ctx context.Context, numShards int) error {
	// Check if shards are already defined
	resp, err := s.etcdClient.Get(ctx, "/shards/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("checking for existing shards: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If shards already exist, load them
	if len(resp.Kvs) > 0 {
		s.logger.Info("Loading shards", zap.Int("existing_shards", len(resp.Kvs)))
		for _, kv := range resp.Kvs {
			shardID := string(kv.Key)[len("/shards/"):]
			instanceID := string(kv.Value)
			s.shardAssignments[shardID] = instanceID
		}
		return nil
	}

	s.logger.Info("Initializing shards", zap.Int("new_shards", numShards))

	// Initialize shards
	var ops []clientv3.Op
	for i := 0; i < numShards; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		// Don't assign to any instance yet
		s.shardAssignments[shardID] = ""

		// Add to batch operation
		key := fmt.Sprintf("/shards/%s", shardID)
		ops = append(ops, clientv3.OpPut(key, ""))
	}

	// Execute batch operation
	_, err = s.etcdClient.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("initializing shards: %w", err)
	}

	return nil
}

// Shutdown gracefully stops the server
func (s *ShardDistributorServer) Shutdown() {
	close(s.stopChan)
}
