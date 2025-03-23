package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// Command line flags
var (
	serverAddr        = flag.String("server", "localhost:50051", "Server address")
	instanceID        = flag.String("id", "", "Instance ID (defaults to hostname)")
	capacity          = flag.Int("capacity", 100, "Instance capacity")
	registerTimeout   = flag.Duration("register-timeout", 30*time.Second, "Registration timeout")
	logLevel          = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	shardTypes        = flag.String("shard-types", "default", "Comma-separated list of shard types to handle")
	metadataFlag      = flag.String("metadata", "", "Comma-separated list of key=value metadata")
	heartbeatInterval = flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	reportInterval    = flag.Duration("report", 10*time.Second, "Health report interval")
)

// ExampleShardHandler is a simple implementation of the shard.Handler interface
type ExampleShardHandler struct {
	shard.BaseHandler
	logger *zap.Logger
}

// NewExampleShardHandler creates a new example shard handler
func NewExampleShardHandler(shardType string, logger *zap.Logger) *ExampleShardHandler {
	return &ExampleShardHandler{
		BaseHandler: shard.BaseHandler{ShardType: shardType},
		logger:      logger,
	}
}

// Prepare prepares the shard
func (h *ExampleShardHandler) Prepare(ctx context.Context, shardID string, version int64) error {
	h.logger.Info("Preparing shard",
		zap.String("shardID", shardID),
		zap.Int64("version", version))

	// Simulate some preparation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Activate activates the shard
func (h *ExampleShardHandler) Activate(ctx context.Context, shardID string, version int64) error {
	h.logger.Info("Activating shard",
		zap.String("shardID", shardID),
		zap.Int64("version", version))

	// Simulate some activation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Deactivate deactivates the shard
func (h *ExampleShardHandler) Deactivate(ctx context.Context, shardID string, version int64) error {
	h.logger.Info("Deactivating shard",
		zap.String("shardID", shardID),
		zap.Int64("version", version))

	// Simulate some deactivation work
	select {
	case <-time.After(500 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetStats returns statistics about the shard
func (h *ExampleShardHandler) GetStats(shardID string) (map[string]interface{}, error) {
	return map[string]interface{}{
		"example_stat": 42,
	}, nil
}

// parseMetadata parses the metadata flag
func parseMetadata(metadataStr string) map[string]string {
	metadata := make(map[string]string)
	if metadataStr == "" {
		return metadata
	}

	pairs := strings.Split(metadataStr, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			metadata[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	return metadata
}

func main() {
	flag.Parse()

	// Configure logging
	logConfig := zap.NewProductionConfig()

	// Set log level
	switch strings.ToLower(*logLevel) {
	case "debug":
		logConfig.Level.SetLevel(zapcore.DebugLevel)
	case "info":
		logConfig.Level.SetLevel(zapcore.InfoLevel)
	case "warn":
		logConfig.Level.SetLevel(zapcore.WarnLevel)
	case "error":
		logConfig.Level.SetLevel(zapcore.ErrorLevel)
	default:
		logConfig.Level.SetLevel(zapcore.InfoLevel)
	}

	logger, err := logConfig.Build()
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting shard distributor client")

	// Use hostname as instance ID if not provided
	id := *instanceID
	if id == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.Fatal("Failed to get hostname", zap.Error(err))
		}
		id = hostname
		logger.Info("Using hostname as instance ID", zap.String("instanceID", id))
	}

	// Parse metadata
	metadata := parseMetadata(*metadataFlag)

	// Add workload type to metadata
	metadata["shard_types"] = *shardTypes

	// Create client service
	svc, err := client.NewService(client.ServiceConfig{
		ServerAddr:           *serverAddr,
		InstanceID:           id,
		Capacity:             int32(*capacity),
		Metadata:             metadata,
		HeartbeatInterval:    *heartbeatInterval,
		HealthReportInterval: *reportInterval,
		ShardProcessorConfig: shard.ProcessorConfig{
			MaxConcurrentTransfers:   5,
			ShardActivationTimeout:   30 * time.Second,
			ShardDeactivationTimeout: 30 * time.Second,
		},
	}, logger)
	if err != nil {
		logger.Fatal("Failed to create client service", zap.Error(err))
	}

	// Register handlers for shard types
	for _, shardType := range strings.Split(*shardTypes, ",") {
		shardType = strings.TrimSpace(shardType)
		if shardType == "" {
			continue
		}

		typeCopy := shardType // Create a copy for the closure
		handlerFactory := func() shard.Handler {
			return NewExampleShardHandler(typeCopy, logger.Named("handler").With(zap.String("shardType", typeCopy)))
		}

		if err := svc.RegisterShardHandler(shardType, handlerFactory); err != nil {
			logger.Fatal("Failed to register shard handler",
				zap.String("shardType", shardType),
				zap.Error(err))
		}
	}

	// Create context with timeout for startup
	ctx, cancel := context.WithTimeout(context.Background(), *registerTimeout)
	defer cancel()

	// Start the service
	if err := svc.Start(ctx); err != nil {
		logger.Fatal("Failed to start client service", zap.Error(err))
	}

	// Wait for the service to be ready
	if err := svc.WaitForReady(ctx); err != nil {
		logger.Fatal("Service failed to become ready", zap.Error(err))
	}

	logger.Info("Client service started and ready")

	// Register state change listener to log state changes
	svc.RegisterStateChangeListener(func(change shard.ShardStateChange) {
		var oldStatus, newStatus string
		if change.OldState != nil {
			oldStatus = string(change.OldState.Status)
		} else {
			oldStatus = "none"
		}

		if change.NewState != nil {
			newStatus = string(change.NewState.Status)
		} else {
			newStatus = "none"
		}

		logger.Info("Shard state changed",
			zap.String("shardID", change.ShardID),
			zap.String("oldStatus", oldStatus),
			zap.String("newStatus", newStatus),
			zap.Time("timestamp", change.Timestamp))
	})

	// Show the shard count
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				active, standby, total, err := svc.ShardCount()
				if err != nil {
					logger.Error("Failed to get shard count", zap.Error(err))
					continue
				}

				logger.Info("Shard count",
					zap.Int("active", active),
					zap.Int("standby", standby),
					zap.Int("total", total))
			case <-ctx.Done():
				return
			}
		}
	}()

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigCh
	logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop the service
	if err := svc.Stop(shutdownCtx); err != nil {
		logger.Error("Error during shutdown", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("Client shutdown complete")
}
