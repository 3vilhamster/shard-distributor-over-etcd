package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

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

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
	config2 "github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/distribution"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/leader"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/reconcile"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

// Service implements the shard distributor service
type Service struct {
	proto.UnimplementedShardDistributorServiceServer

	mu         sync.RWMutex
	config     config2.Config
	logger     *zap.Logger
	clock      clockwork.Clock
	grpcServer *grpc.Server

	// Components
	leaderElection   *leader.Election
	instanceRegistry *registry.Registry
	stateStore       store.Store
	reconcilers      map[string]*reconcile.Reconciler
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
	Clock            clockwork.Clock
	EtcdClient       *clientv3.Client
	LeaderElection   *leader.Election
	InstanceRegistry *registry.Registry
	StateStore       store.Store
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
		stateStore:       params.StateStore,
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

	return svc, nil
}

// connectComponents connects the components with appropriate callbacks
func (s *Service) connectComponents() {
	// Set callbacks for instance registry
	//s.instanceRegistry.SetCallbacks(
	//	// On instance registered
	//	func(instanceID string) {
	//		s.logger.Info("Instance registered callback", zap.String("instance", instanceID))
	//		s.reconciler.ForceReconciliation()
	//	},
	//	// On instance deregistered
	//	func(instanceID string) {
	//		s.logger.Info("Instance deregistered callback", zap.String("instance", instanceID))
	//		s.reconciler.ForceReconciliation()
	//	},
	//	// On instance draining
	//	func(instanceID string) {
	//		s.logger.Info("Instance draining callback", zap.String("instance", instanceID))
	//		s.reconciler.ForceReconciliation()
	//	},
	//)

	// Set callback for leadership changes
	s.leaderElection.SetCallback(s.onLeadershipChange)
}

// Start starts the shard distributor service
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("starting service")

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return fmt.Errorf("service already running")
	}

	s.logger.Info("starting listener for grpc")

	// Start listening
	lis, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	healthgrpc.RegisterHealthServer(s.grpcServer, grpcHealth.NewServer())
	proto.RegisterShardDistributorServiceServer(s.grpcServer, s)

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

	//if isLeader {
	//	// Start reconciler when becoming leader
	//	err := s.reconciler.Start(context.Background())
	//	if err != nil {
	//		s.logger.Fatal("Failed to start reconciler", zap.Error(err))
	//		return
	//	}
	//} else {
	//	s.reconciler.Stop() // Stop reconciler when losing leadership
	//}
}

// GetInstanceCount gets the number of registered instances
func (s *Service) GetInstanceCount() int {
	return s.instanceRegistry.GetInstanceCount()
}

// GetActiveInstanceCount gets the number of active instances
func (s *Service) GetActiveInstanceCount() int {
	return s.instanceRegistry.GetActiveInstanceCount()
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
	stream grpc.BidiStreamingServer[proto.ShardDistributorStreamRequest, proto.ShardDistributorStreamResponse],
) error {
	// Handle stream messages from clients
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
		case proto.ShardDistributorStreamRequest_MESSAGE_TYPE_STOPPING:

		case proto.ShardDistributorStreamRequest_MESSAGE_TYPE_HEARTBEAT:
			err := s.instanceRegistry.UpdateInstanceStatus(context.Background(), clientMsg.Status)
			if err != nil {
				return fmt.Errorf("update instance status: %w", err)
			}

			// Send heartbeat acknowledgment
			err = stream.Send(&proto.ShardDistributorStreamResponse{
				Type: proto.ShardDistributorStreamResponse_MESSAGE_TYPE_HEARTBEAT_ACK,
			})
			if err != nil {
				s.logger.Warn("Failed to send heartbeat acknowledgment",
					zap.String("instance", instanceID),
					zap.Error(err))
				return err
			}

		case proto.ShardDistributorStreamRequest_MESSAGE_TYPE_ACK:
			// Handle acknowledgment of shard assignment
			if clientMsg.ShardId != "" {
				s.logger.Debug("Received assignment ACK",
					zap.String("instance", instanceID),
					zap.String("shard", clientMsg.ShardId))
			}
		}
	}
}
