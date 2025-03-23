package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// Configuration via command-line flags
var (
	etcdEndpoints    = flag.String("etcd", "localhost:2379", "etcd endpoints")
	numType1Shards   = flag.Int("type1", 10, "number of type1 shards to generate")
	numType2Shards   = flag.Int("type2", 10, "number of type2 shards to generate")
	batchSize        = flag.Int("batch", 5, "batch size for shard generation")
	intervalMillis   = flag.Int("interval", 1000, "interval between batches in milliseconds")
	shardPrefix      = flag.String("prefix", "/shards/", "etcd key prefix for shards")
	cleanupOnStartup = flag.Bool("cleanup", true, "cleanup existing shards on startup")
)

func main() {
	flag.Parse()

	// Read configuration from environment variables if set
	if os.Getenv("ETCD_ENDPOINTS") != "" {
		*etcdEndpoints = os.Getenv("ETCD_ENDPOINTS")
	}

	if os.Getenv("NUM_TYPE1_SHARDS") != "" {
		val, err := strconv.Atoi(os.Getenv("NUM_TYPE1_SHARDS"))
		if err == nil {
			*numType1Shards = val
		}
	}

	if os.Getenv("NUM_TYPE2_SHARDS") != "" {
		val, err := strconv.Atoi(os.Getenv("NUM_TYPE2_SHARDS"))
		if err == nil {
			*numType2Shards = val
		}
	}

	if os.Getenv("BATCH_SIZE") != "" {
		val, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
		if err == nil {
			*batchSize = val
		}
	}

	if os.Getenv("INTERVAL_MS") != "" {
		val, err := strconv.Atoi(os.Getenv("INTERVAL_MS"))
		if err == nil {
			*intervalMillis = val
		}
	}

	if os.Getenv("SHARD_PREFIX") != "" {
		*shardPrefix = os.Getenv("SHARD_PREFIX")
	}

	// Connect to etcd
	endpoints := strings.Split(*etcdEndpoints, ",")
	log.Printf("Connecting to etcd: %v", endpoints)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	log.Println("Connected to etcd")

	// Clean up existing shards if requested
	if *cleanupOnStartup {
		log.Println("Cleaning up existing shards...")
		_, err = cli.Delete(context.Background(), *shardPrefix, clientv3.WithPrefix())
		if err != nil {
			log.Printf("Warning: Failed to clean up existing shards: %v", err)
		}
	}

	// Generate shards
	log.Printf("Generating %d type1 shards and %d type2 shards", *numType1Shards, *numType2Shards)

	// Generate type1 shards
	for i := 0; i < *numType1Shards; i++ {
		shardID := fmt.Sprintf("type1:%06d", i+1)
		key := *shardPrefix + shardID
		value := fmt.Sprintf(`{"id":"%s","type":"type1","created_at":%d,"version":1}`,
			shardID, time.Now().UnixNano())

		_, err = cli.Put(context.Background(), key, value)
		if err != nil {
			log.Printf("Error creating shard %s: %v", shardID, err)
		} else {
			log.Printf("Created shard %s", shardID)
		}

		// Apply batch delay
		if (i+1)%*batchSize == 0 && i < *numType1Shards-1 {
			time.Sleep(time.Duration(*intervalMillis) * time.Millisecond)
		}
	}

	// Generate type2 shards
	for i := 0; i < *numType2Shards; i++ {
		shardID := fmt.Sprintf("type2:%06d", i+1)
		key := *shardPrefix + shardID
		value := fmt.Sprintf(`{"id":"%s","type":"type2","created_at":%d,"version":1}`,
			shardID, time.Now().UnixNano())

		_, err = cli.Put(context.Background(), key, value)
		if err != nil {
			log.Printf("Error creating shard %s: %v", shardID, err)
		} else {
			log.Printf("Created shard %s", shardID)
		}

		// Apply batch delay
		if (i+1)%*batchSize == 0 && i < *numType2Shards-1 {
			time.Sleep(time.Duration(*intervalMillis) * time.Millisecond)
		}
	}

	log.Println("All shards created successfully")
}
