#!/bin/bash
# This script helps debug and run the Shard Distributor PoC

trap 'kill
 ${SERVER_PID} ${CLIENT_PID}; exit 1' INT

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
go build -o .bin/server cmd/distributor/server/main.go
.bin/server &
SERVER_PID=$!

echo "Waiting for server to initialize..."
sleep 2

echo "=== Starting client instances ==="
go build -o .bin/client cmd/distributor/client/main.go
.bin/client &
CLIENT_PID=$!

echo "=== Test running. Use these commands to inspect state: ==="
echo "etcdctl get --prefix /services    # List registered services"
echo "etcdctl get --prefix /shards      # Show shard assignments"
echo "etcdctl get --prefix /shard-distributor # Show leader info"

echo "=== Logs will appear below. Press Ctrl+C to stop the test ==="
wait $CLIENT_PID

wait
