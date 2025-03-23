package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto/sharddistributor/v1"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/connection"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// Service is the main client service that coordinates all components
type Service struct {
	config          config.ServiceConfig
	logger          *zap.Logger
	connectionMgr   *connection.Manager
	shardProcessors map[string]*shard.Processor
	stateManager    *shard.StateManager
	handlerReg      *shard.HandlerRegistry
	shutdownOnce    sync.Once
	readyCh         chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc

	namespaces      []string
	hanlderRegistry *shard.HandlerRegistry
}

// NewService creates a new client service
func NewService(cfg config.ServiceConfig, logger *zap.Logger, hanlderRegistry *shard.HandlerRegistry) (*Service, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	if cfg.ServerAddr == "" {
		return nil, fmt.Errorf("server address is required")
	}

	if cfg.InstanceID == "" {
		return nil, fmt.Errorf("instance ID is required")
	}

	// Set default config values if not provided
	if cfg.ReconnectBackoff == 0 {
		cfg.ReconnectBackoff = config.DefaultServiceConfig.ReconnectBackoff
	}
	if cfg.MaxReconnectBackoff == 0 {
		cfg.MaxReconnectBackoff = config.DefaultServiceConfig.MaxReconnectBackoff
	}
	if cfg.ReconnectJitter == 0 {
		cfg.ReconnectJitter = config.DefaultServiceConfig.ReconnectJitter
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = config.DefaultServiceConfig.HeartbeatInterval
	}
	if cfg.HealthReportInterval == 0 {
		cfg.HealthReportInterval = config.DefaultServiceConfig.HealthReportInterval
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create instance info
	instanceInfo := &proto.InstanceInfo{
		InstanceId: cfg.InstanceID,
	}

	// Create connection manager
	connMgr := connection.NewManager(
		cfg.ServerAddr,
		cfg.InstanceID,
		instanceInfo,
	)

	// Create state manager
	stateMgr := shard.NewStateManager()

	// Create handler registry
	handlerReg := shard.NewHandlerRegistry()

	service := &Service{
		config:          cfg,
		logger:          logger,
		connectionMgr:   connMgr,
		stateManager:    stateMgr,
		handlerReg:      handlerReg,
		readyCh:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
		hanlderRegistry: hanlderRegistry,
		shardProcessors: make(map[string]*shard.Processor),
	}

	for _, ns := range cfg.Namespaces {
		service.shardProcessors[ns] = shard.NewProcessor(
			ns,
			connMgr,
			stateMgr,
			logger,
			cfg.ShardProcessorConfig,
		)

		if err := service.RegisterShardHandler(ns); err != nil {
			service.logger.Fatal("Failed to register shard handler",
				zap.String("ns", ns),
				zap.Error(err))
		}

	}

	return service, nil
}

// RegisterShardHandler registers a handler for a specific shard type
func (s *Service) RegisterShardHandler(namespace string) error {
	s.logger.Info("Registering shard handler", zap.String("namespace", namespace))

	// Create a handler instance
	factory := s.handlerReg.GetFactory(namespace)
	if factory == nil {
		return fmt.Errorf("failed to create handler for shard type: %s", namespace)
	}

	handler := factory(s.logger)

	// Register with the processor
	s.shardProcessors[namespace].RegisterHandler(namespace, handler)

	return nil
}

// Start starts the client service
func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("Starting client service",
		zap.String("instanceID", s.config.InstanceID),
		zap.String("serverAddr", s.config.ServerAddr))

	// Connect to the server
	if err := s.connectionMgr.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	return nil
}

// Stop stops the client service
func (s *Service) Stop(ctx context.Context) error {
	var err error

	s.shutdownOnce.Do(func() {
		s.logger.Info("Stopping client service")

		// Cancel our context
		s.cancel()

		// Wait a bit for the status to propagate
		select {
		case <-ctx.Done():
			s.logger.Warn("Context canceled while waiting for status update")
		case <-time.After(1 * time.Second):
			// Continue shutdown
		}

		for _, processor := range s.shardProcessors {
			// Shutdown processor
			if shutdownErr := processor.Shutdown(ctx); shutdownErr != nil {
				s.logger.Error("Failed to shutdown processor", zap.Error(shutdownErr))
				err = shutdownErr
			}
		}

		// Shutdown connection manager
		if shutdownErr := s.connectionMgr.Shutdown(ctx); shutdownErr != nil {
			s.logger.Error("Failed to shutdown connection manager", zap.Error(shutdownErr))
			err = shutdownErr
		}

		s.logger.Info("Client service stopped")
	})

	return err
}
