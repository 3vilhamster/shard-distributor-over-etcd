package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/sharding"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
)

// ShardDistributorServer implements the ShardDistributor gRPC service
type ShardDistributorServer struct {
	proto.UnimplementedShardDistributorServer
	mu               sync.RWMutex
	etcdClient       *clientv3.Client
	instanceStreams  map[string][]proto.ShardDistributor_WatchShardAssignmentsServer
	instances        map[string]*InstanceData
	shardAssignments map[string]string // shardID -> instanceID
	hashStrategy     *sharding.ConsistentHashStrategy
	isLeader         bool
	election         *concurrency.Election
	leaderChan       chan bool
}

// InstanceData contains information about a service instance
type InstanceData struct {
	Info          *proto.InstanceInfo
	Status        *proto.StatusReport
	LastHeartbeat time.Time
}

// NewShardDistributorServer creates a new shard distributor server
func NewShardDistributorServer(etcdClient *clientv3.Client) (*ShardDistributorServer, error) {
	// Create a session for leader election
	session, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(5))
	if err != nil {
		return nil, err
	}

	// Create election
	election := concurrency.NewElection(session, "/shard-distributor/leader")

	server := &ShardDistributorServer{
		etcdClient:       etcdClient,
		instanceStreams:  make(map[string][]proto.ShardDistributor_WatchShardAssignmentsServer),
		instances:        make(map[string]*InstanceData),
		shardAssignments: make(map[string]string),
		hashStrategy:     sharding.NewConsistentHashStrategy(100), // 100 virtual nodes
		election:         election,
		leaderChan:       make(chan bool, 1),
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
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Instance registered: %s at %s", req.InstanceId, req.Endpoint)

	// Store instance information
	s.instances[req.InstanceId] = &InstanceData{
		Info:          req,
		Status:        &proto.StatusReport{InstanceId: req.InstanceId, Status: proto.StatusReport_ACTIVE},
		LastHeartbeat: time.Now(),
	}

	// Register in etcd
	key := fmt.Sprintf("/services/%s", req.InstanceId)
	_, err := s.etcdClient.Put(ctx, key, req.Endpoint)
	if err != nil {
		return &proto.RegisterResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to register in etcd: %v", err),
		}, nil
	}

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		s.recalculateShardDistribution()
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

	log.Printf("Instance deregistered: %s", req.InstanceId)

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
	for _, stream := range s.instanceStreams[req.InstanceId] {
		// Notifying that we're closing the stream
		stream.Send(&proto.ShardAssignment{
			ShardId: "shutdown",
			Action:  proto.ShardAssignment_REVOKE,
		})
	}
	delete(s.instanceStreams, req.InstanceId)

	// If we're the leader, recalculate shard distribution
	if s.isLeader {
		s.recalculateShardDistribution()
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

	// Update status and heartbeat
	instance.Status = req
	instance.LastHeartbeat = time.Now()

	// If instance is draining and we're the leader, recalculate distribution
	if needsRecalculation && s.isLeader {
		s.recalculateShardDistribution()
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
	log.Printf("Instance %s started watching for shard assignments", instanceID)

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
			log.Printf("Error sending initial assignment to %s: %v", instanceID, err)
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

	log.Printf("Instance %s stopped watching for shard assignments", instanceID)
	return nil
}

// campaignForLeadership tries to become the leader for shard distribution
func (s *ShardDistributorServer) campaignForLeadership() {
	for {
		// Try to become leader
		err := s.election.Campaign(context.Background(), "candidate")
		if err != nil {
			log.Printf("Failed to campaign for leadership: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Successfully became leader
		s.mu.Lock()
		s.isLeader = true
		s.mu.Unlock()

		log.Println("Became leader for shard distribution")

		// Recalculate distribution
		s.recalculateShardDistribution()

		// Watch for leadership changes
		ch := s.election.Observe(context.Background())

		// Block until leadership changes
		<-ch

		log.Println("Leadership changed, no longer leader")

		s.mu.Lock()
		s.isLeader = false
		s.mu.Unlock()
	}
}

// recalculateShardDistribution recalculates shard distribution after changes
func (s *ShardDistributorServer) recalculateShardDistribution() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip if not leader
	if !s.isLeader {
		return
	}

	log.Println("Recalculating shard distribution")

	// Convert instances to the format needed by the consistent hash strategy
	activeInstances := make(map[string]sharding.InstanceInfo)
	for id, instance := range s.instances {
		// Skip draining instances
		if instance.Status.Status == proto.StatusReport_DRAINING {
			continue
		}

		activeInstances[id] = sharding.InstanceInfo{
			ID:         id,
			Status:     "active",
			LoadFactor: instance.Status.CpuUsage,
			ShardCount: int(instance.Status.ActiveShardCount),
		}
	}

	// Calculate new distribution
	newDistribution := s.hashStrategy.CalculateDistribution(s.shardAssignments, activeInstances)

	// Identify changes
	assignmentsToSend := make(map[string][]*proto.ShardAssignment)

	// First, find shards to revoke
	for shardID, oldInstanceID := range s.shardAssignments {
		newInstanceID, exists := newDistribution[shardID]
		if !exists || newInstanceID != oldInstanceID {
			// Shard is no longer assigned to the old instance
			if _, ok := assignmentsToSend[oldInstanceID]; !ok {
				assignmentsToSend[oldInstanceID] = make([]*proto.ShardAssignment, 0)
			}

			assignmentsToSend[oldInstanceID] = append(assignmentsToSend[oldInstanceID], &proto.ShardAssignment{
				ShardId: shardID,
				Action:  proto.ShardAssignment_REVOKE,
			})
		}
	}

	// Then, find shards to assign
	for shardID, newInstanceID := range newDistribution {
		oldInstanceID, exists := s.shardAssignments[shardID]
		if !exists || oldInstanceID != newInstanceID {
			// Shard is newly assigned to the new instance
			if _, ok := assignmentsToSend[newInstanceID]; !ok {
				assignmentsToSend[newInstanceID] = make([]*proto.ShardAssignment, 0)
			}

			// First send prepare if it's a transfer
			if exists {
				assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ShardAssignment{
					ShardId:          shardID,
					Action:           proto.ShardAssignment_PREPARE,
					SourceInstanceId: oldInstanceID,
				})
			}

			// Then send assign
			assignmentsToSend[newInstanceID] = append(assignmentsToSend[newInstanceID], &proto.ShardAssignment{
				ShardId: shardID,
				Action:  proto.ShardAssignment_ASSIGN,
			})
		}
	}

	// Update shard assignments map
	s.shardAssignments = newDistribution

	// Send notifications through gRPC streams
	for instanceID, assignments := range assignmentsToSend {
		streams, exists := s.instanceStreams[instanceID]
		if !exists || len(streams) == 0 {
			// Store assignments for when instance connects
			continue
		}

		// Send to all streams for this instance
		for _, stream := range streams {
			for _, assignment := range assignments {
				err := stream.Send(assignment)
				if err != nil {
					log.Printf("Error sending assignment to %s: %v", instanceID, err)
				}
			}
		}
	}

	// Update assignments in etcd
	for shardID, instanceID := range newDistribution {
		key := fmt.Sprintf("/shards/%s", shardID)
		_, err := s.etcdClient.Put(context.Background(), key, instanceID)
		if err != nil {
			log.Printf("Error updating shard assignment in etcd: %v", err)
		}
	}

	log.Println("Shard distribution recalculated and notifications sent")
}

// checkInstanceHealth periodically checks for unhealthy instances
func (s *ShardDistributorServer) checkInstanceHealth() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()

		// Check for instances that haven't sent a heartbeat recently
		for id, instance := range s.instances {
			if now.Sub(instance.LastHeartbeat) > time.Second*15 {
				log.Printf("Instance %s appears to be unhealthy, removing", id)

				// Remove from our instances map
				delete(s.instances, id)

				// Remove from etcd
				key := fmt.Sprintf("/services/%s", id)
				_, err := s.etcdClient.Delete(context.Background(), key)
				if err != nil {
					log.Printf("Error removing unhealthy instance from etcd: %v", err)
				}

				// Close streams
				delete(s.instanceStreams, id)

				// Recalculate if leader
				if s.isLeader {
					go s.recalculateShardDistribution()
				}
			}
		}

		s.mu.Unlock()
	}
}

// LoadShardDefinitions loads shard definitions from etcd or initializes them
func (s *ShardDistributorServer) LoadShardDefinitions(ctx context.Context, numShards int) error {
	// Check if shards are already defined
	resp, err := s.etcdClient.Get(ctx, "/shards/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("error checking for existing shards: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If shards already exist, load them
	if len(resp.Kvs) > 0 {
		for _, kv := range resp.Kvs {
			shardID := string(kv.Key)[len("/shards/"):]
			instanceID := string(kv.Value)
			s.shardAssignments[shardID] = instanceID
		}
		return nil
	}

	// Initialize shards
	for i := 0; i < numShards; i++ {
		shardID := fmt.Sprintf("shard-%d", i)
		// Don't assign to any instance yet
		s.shardAssignments[shardID] = ""

		// Store in etcd
		key := fmt.Sprintf("/shards/%s", shardID)
		_, err := s.etcdClient.Put(ctx, key, "")
		if err != nil {
			return fmt.Errorf("error initializing shard %s: %v", shardID, err)
		}
	}

	return nil
}
