package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jonboulle/clockwork"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/config"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/distribution"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/health"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/leader"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/reconcile"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/registry"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server/store"
)

func main() {
	// Parse command line flags
	etcdAddr := flag.String("etcd", "localhost:2379", "etcd server address")
	serverAddr := flag.String("server", "localhost:50051", "Server address")
	numShards := flag.Int("shards", 10, "Number of shards")
	reconcileInterval := flag.Duration("reconcile-interval", 30*time.Second, "Reconciliation interval")
	healthCheckInterval := flag.Duration("health-interval", 5*time.Second, "Health check interval")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	leaderElectionPath := flag.String("leader-path", "/shard-distributor/leader", "Leader election path")
	distributionStrategy := flag.String("strategy", "farmHash", "Distribution strategy (consistentHash, farmHash)")
	virtualNodes := flag.Int("virtual-nodes", 10, "Virtual nodes for consistent hashing")
	flag.Parse()

	// Create app configuration
	serverConfig := config.Config{
		EtcdEndpoints:       []string{*etcdAddr},
		ListenAddr:          *serverAddr,
		LeaderElectionPath:  *leaderElectionPath,
		ShardCount:          *numShards,
		ReconcileInterval:   *reconcileInterval,
		HealthCheckInterval: *healthCheckInterval,
	}

	fmt.Println("Starting with config:", serverConfig)

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
		fx.Provide(func() config.Config {
			return serverConfig
		}),

		// Provide clock
		fx.Provide(func() clockwork.Clock {
			return clockwork.NewRealClock()
		}),

		// Provide etcd client
		fx.Provide(func(lc fx.Lifecycle, logger *zap.Logger) (*clientv3.Client, error) {
			etcdClient, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{*etcdAddr},
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				logger.Fatal("Failed to connect to etcd", zap.Error(err))
				return nil, err
			}

			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					logger.Info("Closing etcd client")
					return etcdClient.Close()
				},
			})

			return etcdClient, nil
		}),

		fx.Provide(func() LeaderElectionPathOut {
			return LeaderElectionPathOut{
				LeaderPath: *leaderElectionPath,
			}
		}),

		// Provide virtual nodes count with name annotation
		fx.Provide(
			fx.Annotate(
				func() int { return *virtualNodes },
				fx.ResultTags(`name:"consistentHashVirtualNodes"`),
			),
		),

		// Provide distribution strategy
		fx.Provide(func(logger *zap.Logger) distribution.Strategy {
			switch *distributionStrategy {
			case "farmHash":
				return distribution.NewFarmHashStrategy()
			default: // consistentHash
				return distribution.NewConsistentHashStrategy(*virtualNodes)
			}
		}),

		// Provide core components
		fx.Provide(store.NewEtcdStore),
		fx.Provide(registry.NewRegistry),
		fx.Provide(leader.NewElection),
		fx.Provide(health.NewChecker),
		fx.Provide(reconcile.NewReconciler),

		// Provide the service
		fx.Provide(server.NewService),

		// Invoke functions to start the application
		fx.Invoke(func(logger *zap.Logger, service *server.Service, etcdStore store.Store) {
			logger.Info("Shard distributor server initialized",
				zap.String("etcd", *etcdAddr),
				zap.Any("store", etcdStore),
				zap.String("server", *serverAddr),
				zap.Int("shards", *numShards),
				zap.Duration("reconcile_interval", *reconcileInterval),
				zap.Duration("health_interval", *healthCheckInterval),
				zap.String("strategy", *distributionStrategy))
		}),
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

// LeaderElectionPathOut is an output struct for Fx
type LeaderElectionPathOut struct {
	fx.Out

	LeaderPath string `name:"leaderElectionPath"`
}
