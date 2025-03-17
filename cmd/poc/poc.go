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
	"google.golang.org/grpc"

	"github.com/3vilhamster/shard-distributor-over-etcd/gen/proto"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/client"
	"github.com/3vilhamster/shard-distributor-over-etcd/pkg/server"
)

var (
	mode        = flag.String("mode", "", "Mode: 'server' or 'client'")
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

	if *mode == "" {
		fmt.Println("Please specify mode: -mode=server or -mode=client")
		os.Exit(1)
	}

	// Create etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal")
		cancel()
	}()

	if *mode == "server" {
		runServer(ctx, etcdClient)
	} else if *mode == "client" {
		runClient(ctx)
	}
}

func runServer(ctx context.Context, etcdClient *clientv3.Client) {
	log.Println("Starting shard distributor server...")

	// Create the shard distributor server
	s, err := server.NewShardDistributorServer(etcdClient)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Initialize shards
	if err := s.LoadShardDefinitions(ctx, *numShards); err != nil {
		log.Fatalf("Failed to initialize shards: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	proto.RegisterShardDistributorServer(grpcServer, s)

	// Start listening
	lis, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server listening on %s", *serverAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	grpcServer.GracefulStop()
	log.Println("Server shut down")
}

func runClient(ctx context.Context) {
	var instances []*client.ServiceInstance
	baseID := *instanceID

	// Start multiple instances if requested
	for i := 0; i < *numClients; i++ {
		id := baseID
		if *numClients > 1 {
			id = fmt.Sprintf("%s-%d", baseID, i)
		}

		endpoint := fmt.Sprintf("localhost:%d", 60000+i)
		log.Printf("Starting service instance %s at %s", id, endpoint)

		// Create the service instance
		instance, err := client.NewServiceInstance(id, endpoint, *serverAddr)
		if err != nil {
			log.Fatalf("Failed to create service instance %s: %v", id, err)
		}

		// Start the instance
		if err := instance.Start(ctx); err != nil {
			log.Fatalf("Failed to start service instance %s: %v", id, err)
		}

		instances = append(instances, instance)
	}

	// Simulate instance failure if requested
	if *failover && len(instances) > 1 {
		go func() {
			// Wait a bit before killing the first instance
			time.Sleep(20 * time.Second)
			log.Printf("Simulating failure of instance %s", instances[0].InstanceID())
			instances[0].Shutdown(context.Background())
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
		log.Printf("Test duration of %s completed", *runDuration)
	}

	// Shutdown all instances gracefully
	for _, instance := range instances {
		instance.Shutdown(context.Background())
	}
}
