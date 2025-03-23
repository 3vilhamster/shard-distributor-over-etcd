#!/bin/bash
set -e

# This script helps troubleshoot Docker networking issues

echo "=== Docker Network Information ==="
docker network ls
echo ""

echo "=== Shard Distributor Network Info ==="
docker network inspect shard-distributor-net || echo "Network not found"
echo ""

echo "=== Container Status ==="
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

echo "=== Container IP Addresses ==="
docker ps -q | xargs -I {} docker inspect -f '{{.Name}} - {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' {} | sort
echo ""

echo "=== Testing connectivity from client to server ==="
CLIENT_CONTAINER=$(docker ps -q --filter "name=client-type1")

if [ -z "$CLIENT_CONTAINER" ]; then
  echo "No client container found. Is the service running?"
  exit 1
fi

echo "Executing ping to test network connectivity..."
docker exec $CLIENT_CONTAINER ping -c 2 server || echo "Ping failed - DNS resolution may not be working"
echo ""

echo "Checking if server port is listening..."
docker exec $CLIENT_CONTAINER nc -zv server 50051 || echo "Port check failed - server may not be listening properly"
echo ""

echo "=== Server Logs ==="
docker logs $(docker ps -q --filter "name=server") | tail -n 20
echo ""

echo "=== Client Logs ==="
docker logs $(docker ps -q --filter "name=client-type1") | tail -n 20
echo ""

echo "=== Troubleshooting Recommendations ==="
echo "1. Ensure server is listening on 0.0.0.0:50051 not just :50051 or 127.0.0.1:50051"
echo "2. Check client connection string is using 'server:50051' not 'localhost:50051'"
echo "3. Verify all containers are on the same Docker network"
echo "4. Check for firewall rules that might block traffic"
echo "5. Ensure gRPC service is running correctly in the server container"
