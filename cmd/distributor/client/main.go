package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client/shard"
)

// Command line flags
var (
	serverAddr        = flag.String("server", "localhost:50051", "Server address")
	instanceID        = flag.String("id", "", "Instance ID (defaults to hostname)")
	logLevel          = flag.String("log-level", "debug", "Log level (debug, info, warn, error)")
	namespaces        = flag.String("namespaces", "default", "Comma-separated list of shard types to handle")
	heartbeatInterval = flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	reportInterval    = flag.Duration("report", 10*time.Second, "Health report interval")
)

func main() {
	flag.Parse()

	// Use hostname as instance ID if not provided
	id := *instanceID
	if id == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal("Failed to get hostname", zap.Error(err))
		}
		id = hostname
	}

	cfg := config.ServiceConfig{
		ServerAddr:           *serverAddr,
		InstanceID:           id,
		HeartbeatInterval:    *heartbeatInterval,
		HealthReportInterval: *reportInterval,
		ShardProcessorConfig: shard.ProcessorConfig{
			MaxConcurrentTransfers:   5,
			ShardActivationTimeout:   30 * time.Second,
			ShardDeactivationTimeout: 30 * time.Second,
		},
		Namespaces: strings.Split(*namespaces, ","),
	}

	registry := shard.NewHandlerRegistry()
	for _, namespace := range strings.Split(*namespaces, ",") {
		registry.Register(namespace, func(logger *zap.Logger) shard.Handler {
			return NewExampleShardHandler(namespace, logger)
		})
	}

	fmt.Println("Starting with config:", cfg)
	// Start the fx application
	app := fx.New(
		// Provide logger
		fx.Provide(func() (*zap.Logger, error) {
			var logConfig zap.Config
			switch *logLevel {
			case "debug":
				logConfig = zap.NewDevelopmentConfig()
			case "info", "warn", "error":
				logConfig = zap.NewProductionConfig()
				switch *logLevel {
				case "info":
					logConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
				case "warn":
					logConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
				case "error":
					logConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
				}
			default:
				logConfig = zap.NewProductionConfig()
			}
			return logConfig.Build()
		}),

		// Provide configuration
		fx.Provide(func() config.ServiceConfig {
			return cfg
		}),

		// Provide clock
		fx.Provide(func() clockwork.Clock {
			return clockwork.NewRealClock()
		}),

		fx.Provide(func() *shard.HandlerRegistry {
			return registry
		}),

		fx.Provide(client.NewService),
	)

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger, err := zap.NewProduction()
		if err == nil {
			logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		}
		cancel()
	}()

	// Start the application
	if err := app.Start(ctx); err != nil {
		os.Exit(1)
	}

	// Wait for shutdown signal
	<-app.Done()
}
