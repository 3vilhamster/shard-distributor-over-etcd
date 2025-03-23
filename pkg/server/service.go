package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcHealth "google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	config2 "github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/distribution"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/health"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/leader"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/reconcile"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/shard"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// Service implements the shard distributor service
type Service struct {
	proto.UnimplementedShardDistributorServer

	mu         sync.RWMutex
	config     config2.Config
	logger     *zap.Logger
	clock      clockwork.Clock
	grpcServer *grpc.Server

	// Components
	leaderElection   *leader.Election
	instanceRegistry *registry.Registry
	shardManager     *shard.Manager
	healthChecker    *health.Checker
	stateStore       store.Store
	reconciler       *reconcile.Reconciler
	strategyRegistry *distribution.StrategyRegistry

	isRunning bool
	stopCh    chan struct{}
	listener  net.Listener
}

// ServiceParams defines the dependencies for creating a new service
type ServiceParams struct {
	fx.In

	Lifecycle        fx.Lifecycle
	Config           config2.Config
	Logger           *zap.Logger
	Clock            clockwork.Clock `optional:"true"`
	EtcdClient       *clientv3.Client
	LeaderElection   *leader.Election
	InstanceRegistry *registry.Registry
	ShardManager     *shard.Manager
	HealthChecker    *health.Checker
	StateStore       store.Store
	Reconciler       *reconcile.Reconciler
	Strategy         distribution.Strategy `optional:"true"`
}

// NewService creates a new shard distributor service using dependency injection
func NewService(params ServiceParams) (*Service, error) {
	// Use defaults for optional dependencies
	clock := params.Clock
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	// Create strategy registry
	strategyRegistry := distribution.NewStrategyRegistry()

	// Register default strategy if provided
	if params.Strategy != nil {
		strategyRegistry.RegisterStrategy(params.Strategy)
		strategyRegistry.SetDefaultStrategy(params.Strategy.Name())
	}

	svc := &Service{
		config:           params.Config,
		logger:           params.Logger,
		clock:            clock,
		grpcServer:       grpc.NewServer(),
		leaderElection:   params.LeaderElection,
		instanceRegistry: params.InstanceRegistry,
		shardManager:     params.ShardManager,
		healthChecker:    params.HealthChecker,
		stateStore:       params.StateStore,
		reconciler:       params.Reconciler,
		strategyRegistry: strategyRegistry,
		stopCh:           make(chan struct{}),
	}

	// Wire up callbacks
	svc.connectComponents()

	// Register lifecycle hooks
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return svc.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			svc.Stop()
			return nil
		},
	})

	// Register leader election in lifecycle
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(subCtx context.Context) error {
			return svc.leaderElection.Start(subCtx)
		},
		OnStop: func(subCtx context.Context) error {
			svc.leaderElection.Stop()
			return nil
		},
	})

	// Register health checker in lifecycle
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(subCtx context.Context) error {
			return svc.healthChecker.Start(subCtx)
		},
		OnStop: func(subCtx context.Context) error {
			svc.healthChecker.Stop()
			return nil
		},
	})

	return svc, nil
}

// connectComponents connects the components with appropriate callbacks
func (s *Service) connectComponents() {
	// Set callbacks for instance registry
	s.instanceRegistry.SetCallbacks(
		// On instance registered
		func(instanceID string) {
			s.logger.Info("Instance registered callback", zap.String("instance", instanceID))
			s.recalculateDistribution()
		},
		// On instance deregistered
		func(instanceID string) {
			s.logger.Info("Instance deregistered callback", zap.String("instance", instanceID))
			s.recalculateDistribution()
		},
		// On instance draining
		func(instanceID string) {
			s.logger.Info("Instance draining callback", zap.String("instance", instanceID))
			s.recalculateDistribution()
		},
	)

	// Set callback for leadership changes
	s.leaderElection.SetCallback(s.onLeadershipChange)

	// Set shard manager stream provider
	s.shardManager.SetStreamProvider(func(instanceID string) []proto.ShardDistributor_ShardDistributorStreamServer {
		return s.instanceRegistry.GetInstanceStreams(instanceID)
	})

	// Set health checker callback
	s.healthChecker.SetCallback(func(instanceID string) {
		s.logger.Info("Instance removed by health checker", zap.String("instance", instanceID))
		s.recalculateDistribution()
	})
}

// Start starts the shard distributor service
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting service")

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("locked service")

	if s.isRunning {
		return fmt.Errorf("service already running")
	}

	s.logger.Info("initializing global version")

	// Initialize the store's global version
	if err := s.stateStore.InitializeGlobalVersion(ctx); err != nil {
		return fmt.Errorf("failed to initialize global version: %w", err)
	}

	s.logger.Info("loading shard definitions")

	// Initialize shards
	if err := s.shardManager.LoadShardDefinitions(ctx, s.config.ShardCount); err != nil {
		return fmt.Errorf("failed to initialize shards: %w", err)
	}

	s.logger.Info("loading shard groups")

	// Load any existing shard groups
	if err := s.shardManager.LoadShardGroups(ctx); err != nil {
		s.logger.Warn("Failed to load shard groups", zap.Error(err))
		// Continue anyway as this is not fatal
	}

	s.logger.Info("starting listener for grpc")

	// Start listening
	lis, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	healthgrpc.RegisterHealthServer(s.grpcServer, grpcHealth.NewServer())
	proto.RegisterShardDistributorServer(s.grpcServer, s)

	// Start the gRPC server
	go func() {
		s.logger.Info("Server listening", zap.String("addr", s.config.ListenAddr))
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("Failed to serve", zap.Error(err))
		}
	}()

	s.isRunning = true

	// Log startup
	s.logger.Info("Shard distributor service started",
		zap.Int("shard_count", s.config.ShardCount),
		zap.String("listen_addr", s.config.ListenAddr),
		zap.Strings("etcd_endpoints", s.config.EtcdEndpoints))

	return nil
}

// recalculateDistribution triggers redistribution if leader
func (s *Service) recalculateDistribution() {
	s.mu.RLock()
	isLeader := s.leaderElection.IsLeader()
	s.mu.RUnlock()

	if isLeader {
		s.shardManager.RecalculateDistribution()
	}
}

// Stop stops the shard distributor service
func (s *Service) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return
	}

	// Close listener and stop server
	if s.listener != nil {
		s.listener.Close()
	}
	s.grpcServer.GracefulStop()

	close(s.stopCh)
	s.isRunning = false
	s.logger.Info("Service stopped")
}

// onLeadershipChange is called when leadership status changes
func (s *Service) onLeadershipChange(isLeader bool) {
	s.logger.Info("Leadership status changed", zap.Bool("is_leader", isLeader))

	if isLeader {
		s.shardManager.SetLeaderStatus(true)

		// Start reconciler when becoming leader
		err := s.reconciler.Start(context.Background())
		if err != nil {
			s.logger.Fatal("Failed to start reconciler", zap.Error(err))
			return
		}

		s.shardManager.RecalculateDistribution()
	} else {
		s.shardManager.SetLeaderStatus(false)
		s.reconciler.Stop() // Stop reconciler when losing leadership
	}
}

// RegisterShardGroup creates a new shard group
func (s *Service) RegisterShardGroup(
	ctx context.Context,
	groupID string,
	shardCount int,
	description string,
	metadata map[string]string,
) error {
	return s.shardManager.RegisterShardGroup(ctx, groupID, shardCount, description, metadata)
}

// GetShardGroup gets a shard group by ID
func (s *Service) GetShardGroup(groupID string) (*shard.Group, bool) {
	return s.shardManager.GetShardGroup(groupID)
}

// GetAllShardGroups gets all shard groups
func (s *Service) GetAllShardGroups() map[string]*shard.Group {
	return s.shardManager.GetAllShardGroups()
}

// GetShardAssignments gets all shard assignments
func (s *Service) GetShardAssignments() map[string]string {
	return s.shardManager.GetShardAssignments()
}

// GetInstanceCount gets the number of registered instances
func (s *Service) GetInstanceCount() int {
	return s.instanceRegistry.GetInstanceCount()
}

// GetActiveInstanceCount gets the number of active instances
func (s *Service) GetActiveInstanceCount() int {
	return s.instanceRegistry.GetActiveInstanceCount()
}

// GetTransferLatencyStats gets statistics about shard transfers
func (s *Service) GetTransferLatencyStats() (min, max, avg time.Duration, count int) {
	return s.shardManager.GetTransferLatencyStats()
}

// GetHealth returns the health status of the service
func (s *Service) GetHealth() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"is_running":       s.isRunning,
		"is_leader":        s.leaderElection.IsLeader(),
		"instance_count":   s.instanceRegistry.GetInstanceCount(),
		"active_instances": s.instanceRegistry.GetActiveInstanceCount(),
		"shard_groups":     len(s.shardManager.GetAllShardGroups()),
		"health_checker":   s.healthChecker.GetHealthMetrics(),
	}
}

// RegisterStrategy registers a distribution strategy
func (s *Service) RegisterStrategy(strategy distribution.Strategy) {
	s.strategyRegistry.RegisterStrategy(strategy)
}

// SetDefaultStrategy sets the default distribution strategy
func (s *Service) SetDefaultStrategy(strategyName string) {
	s.strategyRegistry.SetDefaultStrategy(strategyName)
}

// GetAvailableStrategies gets the names of all available strategies
func (s *Service) GetAvailableStrategies() []string {
	return s.strategyRegistry.GetAvailableStrategies()
}

// ShardDistributorStream implements the gRPC service method
func (s *Service) ShardDistributorStream(
	stream proto.ShardDistributor_ShardDistributorStreamServer,
) error {
	// Handle stream messages from clients
	return s.handleClientStream(stream)
}

// handleClientStream processes messages from a client stream
func (s *Service) handleClientStream(
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
				s.instanceRegistry.HandleInstanceDisconnect(instanceID, stream)
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
			resp, err := s.instanceRegistry.RegisterInstance(
				stream.Context(),
				clientMsg.InstanceInfo,
				stream,
				peerAddr,
			)
			if err != nil {
				s.logger.Error("Failed to handle registration",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			if resp.Success {
				registered = true
			}

			// Send the response
			err = stream.Send(resp)
			if err != nil {
				s.logger.Warn("Failed to send registration response",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

		case proto.ClientMessage_DEREGISTER:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before deregistering")
			}

			resp, err := s.instanceRegistry.DeregisterInstance(stream.Context(), instanceID)
			if err != nil {
				s.logger.Error("Failed to handle deregistration",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			if resp.Success {
				registered = false

				// Send the response
				err = stream.Send(resp)
				if err != nil {
					s.logger.Warn("Failed to send deregistration response",
						zap.String("instance", instanceID),
						zap.Error(err))
				}

				// Disconnect after successful deregistration
				return nil
			}

		case proto.ClientMessage_WATCH:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before watching")
			}

			// Start watching for shard assignments
			s.logger.Info("Instance watching for shard assignments",
				zap.String("instance", instanceID))

			// Get current assignments for this instance
			assignments := s.shardManager.GetShardAssignments()

			// Send initial assignments
			for shardID, assignedInstanceID := range assignments {
				if assignedInstanceID == instanceID {
					version := s.shardManager.GetVersion(shardID)

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
			}

		case proto.ClientMessage_HEARTBEAT:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before sending heartbeats")
			}

			err := s.instanceRegistry.HandleHeartbeat(instanceID)
			if err != nil {
				s.logger.Warn("Failed to handle heartbeat",
					zap.String("instance", instanceID),
					zap.Error(err))
			}

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

			// Handle acknowledgment of shard assignment
			if clientMsg.ShardId != "" {
				s.logger.Debug("Received assignment ACK",
					zap.String("instance", instanceID),
					zap.String("shard", clientMsg.ShardId))

				// If this was a transfer, record completion
				s.shardManager.RecordTransferCompletion(clientMsg.ShardId, instanceID)
			}

		case proto.ClientMessage_STATUS_REPORT:
			if !registered {
				return status.Error(codes.FailedPrecondition, "instance must register before reporting status")
			}

			resp, err := s.instanceRegistry.UpdateInstanceStatus(stream.Context(), clientMsg.Status)
			if err != nil {
				s.logger.Error("Failed to handle status report",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

			// Send the response
			err = stream.Send(resp)
			if err != nil {
				s.logger.Warn("Failed to send status response",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}
		}
	}
}

// GetVersion gets the version for a shard (helper for handleClientStream)
func (s *Service) GetVersion(shardID string) int64 {
	return s.shardManager.GetVersion(shardID)
}
