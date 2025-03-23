package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/connection"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/health"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// ServiceConfig defines configuration for the client service
type ServiceConfig struct {
	// ServerAddr is the address of the shard distributor server
	ServerAddr string

	// InstanceID is the unique identifier for this instance
	InstanceID string

	// Capacity is the maximum capacity of this instance
	Capacity int32

	// Metadata contains additional information about this instance
	Metadata map[string]string

	// ReconnectBackoff is the base time to wait before reconnecting
	ReconnectBackoff time.Duration

	// MaxReconnectBackoff is the maximum time to wait before reconnecting
	MaxReconnectBackoff time.Duration

	// ReconnectJitter is the jitter to add to reconnect backoff
	ReconnectJitter float64

	// HeartbeatInterval is the interval between heartbeats
	HeartbeatInterval time.Duration

	// HealthReportInterval is the interval between health reports
	HealthReportInterval time.Duration

	// ShardProcessorConfig is the configuration for the shard processor
	ShardProcessorConfig shard.ProcessorConfig
}

// DefaultServiceConfig provides default configuration values
var DefaultServiceConfig = ServiceConfig{
	ReconnectBackoff:     time.Second,
	MaxReconnectBackoff:  30 * time.Second,
	ReconnectJitter:      0.2,
	HeartbeatInterval:    5 * time.Second,
	HealthReportInterval: 10 * time.Second,
	ShardProcessorConfig: shard.DefaultProcessorConfig,
}

// Service is the main client service that coordinates all components
type Service struct {
	config         ServiceConfig
	logger         *zap.Logger
	connectionMgr  *connection.Manager
	shardProcessor *shard.Processor
	stateManager   *shard.StateManager
	healthReporter *health.Reporter
	handlerReg     *shard.HandlerRegistry
	shutdownOnce   sync.Once
	readyCh        chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewService creates a new client service
func NewService(config ServiceConfig, logger *zap.Logger) (*Service, error) {
	if logger == nil {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	if config.ServerAddr == "" {
		return nil, fmt.Errorf("server address is required")
	}

	if config.InstanceID == "" {
		return nil, fmt.Errorf("instance ID is required")
	}

	// Set default config values if not provided
	if config.ReconnectBackoff == 0 {
		config.ReconnectBackoff = DefaultServiceConfig.ReconnectBackoff
	}
	if config.MaxReconnectBackoff == 0 {
		config.MaxReconnectBackoff = DefaultServiceConfig.MaxReconnectBackoff
	}
	if config.ReconnectJitter == 0 {
		config.ReconnectJitter = DefaultServiceConfig.ReconnectJitter
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = DefaultServiceConfig.HeartbeatInterval
	}
	if config.HealthReportInterval == 0 {
		config.HealthReportInterval = DefaultServiceConfig.HealthReportInterval
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create instance info
	instanceInfo := &proto.InstanceInfo{
		InstanceId: config.InstanceID,
		Capacity:   config.Capacity,
		Metadata:   config.Metadata,
	}

	// Create connection manager
	connMgr := connection.NewManager(
		config.ServerAddr,
		config.InstanceID,
		instanceInfo,
		connection.WithLogger(logger.Named("connection")),
		connection.WithHeartbeatInterval(config.HeartbeatInterval),
		connection.WithReconnectBackoff(
			config.ReconnectBackoff,
			config.MaxReconnectBackoff,
			config.ReconnectJitter,
		),
	)

	// Create state manager
	stateMgr := shard.NewStateManager()

	// Create handler registry
	handlerReg := shard.NewHandlerRegistry()

	service := &Service{
		config:        config,
		logger:        logger,
		connectionMgr: connMgr,
		stateManager:  stateMgr,
		handlerReg:    handlerReg,
		readyCh:       make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Create shard processor
	shardProcessor := shard.NewProcessor(
		connMgr,
		stateMgr,
		"default", // Default workload type
		logger.Named("processor"),
		config.ShardProcessorConfig,
	)
	service.shardProcessor = shardProcessor

	// Create health reporter
	healthReporter := health.NewReporter(
		connMgr,
		shardProcessor,
		logger.Named("health"),
		health.ReporterConfig{
			ReportInterval: config.HealthReportInterval,
			InitialStatus:  proto.StatusReport_ACTIVE,
		},
	)
	service.healthReporter = healthReporter

	return service, nil
}

// RegisterShardHandler registers a handler for a specific shard type
func (s *Service) RegisterShardHandler(shardType string, factory shard.HandlerFactory) error {
	s.logger.Info("Registering shard handler", zap.String("shardType", shardType))

	// Register in the handler registry
	s.handlerReg.Register(shardType, factory)

	// Create a handler instance
	handler, ok := s.handlerReg.Create(shardType)
	if !ok {
		return fmt.Errorf("failed to create handler for shard type: %s", shardType)
	}

	// Register with the processor
	s.shardProcessor.RegisterHandler(shardType, handler)

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

	// Register with the server
	if err := s.connectionMgr.Register(ctx); err != nil {
		return fmt.Errorf("failed to register with server: %w", err)
	}

	// Start watching for shard assignments
	if err := s.connectionMgr.StartWatching(ctx); err != nil {
		return fmt.Errorf("failed to start watching for shard assignments: %w", err)
	}

	// Start health reporter
	if err := s.healthReporter.Start(); err != nil {
		return fmt.Errorf("failed to start health reporter: %w", err)
	}

	// Signal readiness
	close(s.readyCh)

	return nil
}

// Stop stops the client service
func (s *Service) Stop(ctx context.Context) error {
	var err error

	s.shutdownOnce.Do(func() {
		s.logger.Info("Stopping client service")

		// Cancel our context
		s.cancel()

		// Set status to DRAINING
		s.healthReporter.SetStatus(proto.StatusReport_DRAINING)

		// Wait a bit for the status to propagate
		select {
		case <-ctx.Done():
			s.logger.Warn("Context canceled while waiting for status update")
		case <-time.After(1 * time.Second):
			// Continue shutdown
		}

		// Stop health reporter
		if stopErr := s.healthReporter.Stop(); stopErr != nil {
			s.logger.Error("Failed to stop health reporter", zap.Error(stopErr))
			err = stopErr
		}

		// Shutdown processor
		if shutdownErr := s.shardProcessor.Shutdown(ctx); shutdownErr != nil {
			s.logger.Error("Failed to shutdown processor", zap.Error(shutdownErr))
			err = shutdownErr
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

// WaitForReady waits for the service to be ready
func (s *Service) WaitForReady(ctx context.Context) error {
	select {
	case <-s.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetShardProcessor returns the shard processor
func (s *Service) GetShardProcessor() *shard.Processor {
	return s.shardProcessor
}

// GetStateManager returns the state manager
func (s *Service) GetStateManager() *shard.StateManager {
	return s.stateManager
}

// GetHealthReporter returns the health reporter
func (s *Service) GetHealthReporter() *health.Reporter {
	return s.healthReporter
}

// GetConnectionManager returns the connection manager
func (s *Service) GetConnectionManager() *connection.Manager {
	return s.connectionMgr
}

// RegisterStateChangeListener registers a listener for shard state changes
func (s *Service) RegisterStateChangeListener(listener shard.StateChangeListener) {
	s.stateManager.AddStateChangeListener(listener)
}

// SetStatus sets the status of the instance
func (s *Service) SetStatus(status proto.StatusReport_Status) {
	s.healthReporter.SetStatus(status)
}

// GetStatus returns the current status of the instance
func (s *Service) GetStatus() proto.StatusReport_Status {
	return s.healthReporter.GetStatus()
}

// ShardCount returns the count of shards by status
func (s *Service) ShardCount() (active int, standby int, total int, err error) {
	states, err := s.stateManager.GetAllShardStates()
	if err != nil {
		return 0, 0, 0, err
	}

	for _, state := range states {
		if state.Status == shard.ShardStatusActive {
			active++
		} else if state.Status == shard.ShardStatusPrepared {
			standby++
		}
	}

	total = len(states)
	return active, standby, total, nil
}
