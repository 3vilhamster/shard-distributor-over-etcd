package server

import (
	"context"
	"fmt"
	"reflect"
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
	instanceStreams  map[string][]proto.ShardDistributor_WatchShardAssignmentsServer
	instances        map[string]*InstanceData
	shardAssignments map[string]string // shardID -> instanceID
	hashStrategy     Distributor
	isLeader         bool
	election         *concurrency.Election
	leaderChan       chan bool
	session          *concurrency.Session
	logger           *zap.Logger
	stopChan         chan struct{}
}

// InstanceData contains information about a service instance
type InstanceData struct {
	Info          *proto.InstanceInfo
	Status        *proto.StatusReport
	LastHeartbeat time.Time
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
		instanceStreams:  make(map[string][]proto.ShardDistributor_WatchShardAssignmentsServer),
		instances:        make(map[string]*InstanceData),
		shardAssignments: make(map[string]string),
		hashStrategy:     sharding.NewSimpleHashDistributor(),
		election:         election,
		leaderChan:       make(chan bool, 1),
		session:          session,
		logger:           logger,
		stopChan:         make(chan struct{}),
	}

	// Start leadership campaign
	go server.campaignForLeadership()

	// Start instance health check
	go server.checkInstanceHealth()

	return server, nil
}

// RegisterInstance implements the gRPC RegisterInstance method
func (s *ShardDistributorServer) RegisterInstance(
	ctx context.Context,
	req *proto.InstanceInfo,
) (*proto.RegisterResponse, error) {
	// Extract peer information if available
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "failed to get peer from context")
	}

	addr := peer.Addr.String()
	s.logger.Info("Using peer address as endpoint", zap.String("instance", req.InstanceId), zap.String("peer_addr", addr))

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Instance registered", zap.String("instance", req.InstanceId), zap.String("endpoint", addr))

	// Store instance information
	s.instances[req.InstanceId] = &InstanceData{
		Info:          req,
		Status:        &proto.StatusReport{InstanceId: req.InstanceId, Status: proto.StatusReport_ACTIVE},
		LastHeartbeat: time.Now(),
	}

	// Register in etcd with TTL lease for automatic cleanup if instance fails
	lease, err := s.etcdClient.Grant(ctx, 30) // 30-second TTL
	if err != nil {
		return &proto.RegisterResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to create lease in etcd: %v", err),
		}, nil
	}

	key := fmt.Sprintf("/services/%s", req.InstanceId)
	_, err = s.etcdClient.Put(ctx, key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		return &proto.RegisterResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to register in etcd: %v", err),
		}, nil
	}

	go func() {
		// Keep lease alive in background
		ch, err := s.etcdClient.KeepAlive(context.Background(), lease.ID)
		if err != nil {
			s.logger.Error("Failed to keep lease alive for instance", zap.String("instance", req.InstanceId), zap.Error(err))
			return
		}

		// Consume keep alive responses
		for range ch {
			// Just consume to keep channel unblocked
		}
	}()

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		go s.recalculateShardDistribution()
	}

	// Return the currently assigned shards to this instance
	assignedShards := []string{}
	for shardID, instanceID := range s.shardAssignments {
		if instanceID == req.InstanceId {
			assignedShards = append(assignedShards, shardID)
		}
	}

	return &proto.RegisterResponse{
		Success:        true,
		Message:        "Instance registered successfully",
		AssignedShards: assignedShards,
	}, nil
}

// DeregisterInstance implements the gRPC DeregisterInstance method
func (s *ShardDistributorServer) DeregisterInstance(
	ctx context.Context,
	req *proto.InstanceInfo,
) (*proto.DeregisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Instance deregistered", zap.String("instance", req.InstanceId))

	// Remove from etcd
	key := fmt.Sprintf("/services/%s", req.InstanceId)
	_, err := s.etcdClient.Delete(ctx, key)
	if err != nil {
		return &proto.DeregisterResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to deregister in etcd: %v", err),
		}, nil
	}

	// Remove from our instances map
	delete(s.instances, req.InstanceId)

	// Close all streams to this instance
	streams := s.instanceStreams[req.InstanceId]
	for _, stream := range streams {
		// Notifying that we're closing the stream
		stream.Send(&proto.ShardAssignment{
			ShardId: "shutdown",
			Action:  proto.ShardAssignment_REVOKE,
		})
	}
	delete(s.instanceStreams, req.InstanceId)

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		go s.recalculateShardDistribution()
	}

	return &proto.DeregisterResponse{
		Success: true,
		Message: "Instance deregistered successfully",
	}, nil
}

// ReportStatus implements the gRPC ReportStatus method
func (s *ShardDistributorServer) ReportStatus(
	ctx context.Context,
	req *proto.StatusReport,
) (*proto.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update instance status
	instance, exists := s.instances[req.InstanceId]
	if !exists {
		return &proto.StatusResponse{
			Acknowledged: false,
			Message:      "Instance not found",
		}, nil
	}

	// Check if instance is transitioning to DRAINING
	needsRecalculation := instance.Status.Status != proto.StatusReport_DRAINING &&
		req.Status == proto.StatusReport_DRAINING

	if needsRecalculation {
		s.logger.Info("Instance transitioning to DRAINING state", zap.String("instance", req.InstanceId))
	}

	// Update status and heartbeat
	instance.Status = req
	instance.LastHeartbeat = time.Now()

	// If instance is draining and we're the leader, recalculate distribution
	if needsRecalculation && s.isLeader {
		go s.recalculateShardDistribution()
	}

	return &proto.StatusResponse{
		Acknowledged: true,
		Message:      "Status updated",
	}, nil
}

// WatchShardAssignments implements the gRPC streaming WatchShardAssignments method
func (s *ShardDistributorServer) WatchShardAssignments(
	req *proto.InstanceInfo,
	stream proto.ShardDistributor_WatchShardAssignmentsServer,
) error {
	instanceID := req.InstanceId
	s.logger.Info("Instance started watching for shard assignments", zap.String("instance", req.InstanceId))

	// Store the stream for this instance
	s.mu.Lock()
	if _, exists := s.instanceStreams[instanceID]; !exists {
		s.instanceStreams[instanceID] = make([]proto.ShardDistributor_WatchShardAssignmentsServer, 0)
	}
	s.instanceStreams[instanceID] = append(s.instanceStreams[instanceID], stream)

	// Send initial assignments
	currentAssignments := make([]*proto.ShardAssignment, 0)
	for shardID, assignedInstance := range s.shardAssignments {
		if assignedInstance == instanceID {
			currentAssignments = append(currentAssignments, &proto.ShardAssignment{
				ShardId: shardID,
				Action:  proto.ShardAssignment_ASSIGN,
			})
		}
	}
	s.mu.Unlock()

	// Send initial assignments
	for _, assignment := range currentAssignments {
		if err := stream.Send(assignment); err != nil {
			s.logger.Error("Error sending initial assignment", zap.String("instance", req.InstanceId), zap.Error(err))
			return err
		}
	}

	// Keep the stream open until the client disconnects
	<-stream.Context().Done()

	// Remove the stream when the client disconnects
	s.mu.Lock()
	defer s.mu.Unlock()

	streams := s.instanceStreams[instanceID]
	for i, str := range streams {
		if str == stream {
			// Remove this stream from the slice
			s.instanceStreams[instanceID] = append(streams[:i], streams[i+1:]...)
			break
		}
	}

	s.logger.Info("Instance stopped watching for shard assignments", zap.String("instance", req.InstanceId))
	return nil
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

	// Check if the distribution changed before proceeding
	if distributionsEqual(s.shardAssignments, newDistribution) {
		// No changes in distribution, skip the rest
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

	s.logger.Info("Distribution changed", zap.Int("shard_affected", changes))

	// Identify changes
	assignmentsToSend := make(map[string][]*proto.ShardAssignment)
	pendingRevokes := make(map[string][]string) // instanceID -> []shardID

	// First, process new assignments
	for shardID, newInstanceID := range newDistribution {
		oldInstanceID, exists := s.shardAssignments[shardID]

		if exists && oldInstanceID != "" && oldInstanceID != newInstanceID {
			// This is a transfer - prepare new instance
			if newInstanceID != "" {
				if _, ok := assignmentsToSend[newInstanceID]; !ok {
					assignmentsToSend[newInstanceID] = make([]*proto.ShardAssignment, 0)
				}

				// First prepare
				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ShardAssignment{
					ShardId:          shardID,
					Action:           proto.ShardAssignment_PREPARE,
					SourceInstanceId: oldInstanceID,
				})

				// Then assign
				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ShardAssignment{
					ShardId:          shardID,
					Action:           proto.ShardAssignment_ASSIGN,
					SourceInstanceId: oldInstanceID,
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
					assignmentsToSend[newInstanceID] = make([]*proto.ShardAssignment, 0)
				}

				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ShardAssignment{
					ShardId: shardID,
					Action:  proto.ShardAssignment_ASSIGN,
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
	go func(pendingRevokes map[string][]string) {
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
					err := stream.Send(&proto.ShardAssignment{
						ShardId: shardID,
						Action:  proto.ShardAssignment_REVOKE,
					})
					if err != nil {
						s.logger.Warn("Error sending revoke", zap.String("instance", instanceID), zap.Error(err))
					}
				}
			}
		}
	}(pendingRevokes)

	// Update assignments in etcd
	batch := s.etcdClient.Txn(context.Background())
	var ops []clientv3.Op
	for shardID, instanceID := range newDistribution {
		key := fmt.Sprintf("/shards/%s", shardID)
		ops = append(ops, clientv3.OpPut(key, instanceID))
	}

	// Execute batch operation if there are assignments to update
	if len(ops) > 0 {
		_, err := batch.Then(ops...).Commit()
		if err != nil {
			s.logger.Warn("Error updating shard assignments in etcd", zap.Error(err))
		}
	}
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
				s.logger.Info("Recalculating distribution", zap.Int("unhealth_instances", unhealthy))
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
