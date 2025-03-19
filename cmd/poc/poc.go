package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server"
)

var (
	mode        = flag.String("mode", "", "Mode: 'server', 'client' or 'healthcheck'")
	etcdAddr    = flag.String("etcd", "localhost:2379", "etcd server address")
	serverAddr  = flag.String("server", "localhost:50051", "Server address")
	instanceID  = flag.String("id", "", "Instance ID (required for client mode)")
	numShards   = flag.Int("shards", 10, "Number of shards (server mode only)")
	numClients  = flag.Int("clients", 3, "Number of client instances to start (client mode only)")
	runDuration = flag.Duration("duration", 60*time.Second, "Duration to run the test")
	failover    = flag.Bool("failover", false, "Simulate instance failure (client mode only)")
)

func main() {
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to initialize zap logger: %v", err)
	}

	if *mode == "" {
		logger.Fatal("Please specify mode: -mode=server or -mode=client")
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("Received shutdown signal")
		cancel()
	}()

	switch *mode {
	case "server":
		runServer(ctx, logger)
	case "client":
		runClient(ctx, logger)
	case "healthcheck":
		conn, err := net.DialTimeout("tcp", *serverAddr, time.Second*2)
		if err != nil {
			fmt.Println("Health check failed:", err)
			os.Exit(1)
		}
		err = conn.Close()
		if err != nil {
			return
		}
		fmt.Println("Health check passed")
		os.Exit(0)
	}
}

func runServer(ctx context.Context, logger *zap.Logger) {
	logger.Info("Starting shard distributor server...")

	// Create etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatal("Failed to connect to etcd", zap.Error(err))
	}
	defer func(etcdClient *clientv3.Client) {
		closeErr := etcdClient.Close()
		if closeErr != nil {
			logger.Error("Failed to close etcd", zap.Error(closeErr))
		}
	}(etcdClient)

	// Create the shard distributor server
	s, err := server.NewShardDistributorServer(logger, etcdClient)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	// Initialize shards
	if err := s.LoadShardDefinitions(ctx, *numShards); err != nil {
		logger.Fatal("Failed to initialize shards", zap.Error(err))
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	proto.RegisterShardDistributorServer(grpcServer, s)

	// Start listening
	lis, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Server listening", zap.String("port", *serverAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	grpcServer.GracefulStop()
	logger.Info("Server shut down")
}

func runClient(ctx context.Context, logger *zap.Logger) {
	var instances []*client.ServiceInstance
	baseID := *instanceID

	// Start multiple instances if requested
	for i := 0; i < *numClients; i++ {
		id := baseID
		if *numClients > 1 {
			id = fmt.Sprintf("%s-%d", baseID, i)
		}

		endpoint := fmt.Sprintf("localhost:%d", 60000+i)
		logger.Info("Starting service instance", zap.String("instance", id), zap.String("endpoint", endpoint))

		// Create the service instance
		instance, err := client.NewServiceInstance(id, endpoint, *serverAddr, logger)
		if err != nil {
			logger.Fatal("Failed to create service", zap.String("instance", id), zap.Error(err))
		}

		// Start the instance
		if err := instance.Start(ctx); err != nil {
			logger.Fatal("Failed to start service instance", zap.String("instance", id), zap.Error(err))
		}

		instances = append(instances, instance)
	}

	// Simulate instance failure if requested
	if *failover && len(instances) > 1 {
		go func() {
			// Wait a bit before killing the first instance
			time.Sleep(20 * time.Second)
			logger.Info("Simulating failure of instance %s", zap.String("instance", instances[0].InstanceID()))
			err := instances[0].Shutdown(context.Background())
			if err != nil {
				logger.Warn("Failed to shut down instance %s", zap.String("instance", instances[0].InstanceID()))
				return
			}
			instances = instances[1:]
		}()
	}

	// Create a timer for the test duration
	timer := time.NewTimer(*runDuration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// External shutdown signal
	case <-timer.C:
		logger.Info("Test duration of completed", zap.Duration("duration", *runDuration))
	}

	// Shutdown all instances gracefully
	for _, instance := range instances {
		err := instance.Shutdown(context.Background())
		if err != nil {
			logger.Warn("Failed to shut down instance %s", zap.String("instance", instances[0].InstanceID()))
			return
		}
	}
}
