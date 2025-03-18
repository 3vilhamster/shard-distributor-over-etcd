#!/bin/bash
# This script helps debug and run the Shard Distributor PoC

# Set up logging for easier debugging
export ETCDCTL_API=3

echo "=== Checking etcd installation ==="
if ! command -v etcdctl &> /dev/null; then
    echo "etcd client not found, you may need to install it or add to PATH"
    echo "On macOS: brew install etcd"
    echo "On Ubuntu: apt-get install etcd-client"
fi

echo "=== Checking if etcd is running ==="
if ! etcdctl endpoint health --endpoints=localhost:2379 &> /dev/null; then
    echo "etcd doesn't appear to be running. Starting etcd container..."
    docker run -d --name etcd-server \
      -p 2379:2379 \
      -p 2380:2380 \
      --env ALLOW_NONE_AUTHENTICATION=yes \
      --env ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
      bitnami/etcd:latest

    echo "Waiting for etcd to start..."
    sleep 5

    if ! etcdctl endpoint health --endpoints=localhost:2379 &> /dev/null; then
        echo "Failed to start etcd. Check if Docker is running and try again."
        exit 1
    fi
    echo "etcd started successfully!"
else
    echo "etcd is already running"
fi

echo "=== Cleaning up existing etcd keys ==="
etcdctl del --prefix /shard-distributor
etcdctl del --prefix /services
etcdctl del --prefix /shards

echo "=== Building the application ==="
go build -o shard-distributor cmd/poc/poc.go

echo "=== Running the server ==="
./shard-distributor -mode=server -shards=10 &
SERVER_PID=$!

echo "Waiting for server to initialize..."
sleep 2

echo "=== Starting client instances ==="
./shard-distributor -mode=client -id=instance -clients=3 -duration=30s -failover=true &
CLIENT_PID=$!

echo "=== Test running. Use these commands to inspect state: ==="
echo "etcdctl get --prefix /services    # List registered services"
echo "etcdctl get --prefix /shards      # Show shard assignments"
echo "etcdctl get --prefix /shard-distributor # Show leader info"

echo "=== Logs will appear below. Press Ctrl+C to stop the test ==="
wait $CLIENT_PID

echo "=== Test complete. Cleaning up ==="
kill $SERVER_PID
echo "You can stop the etcd container with: docker stop etcd-server"
echo "You can remove the etcd container with: docker rm etcd-server"
