#!/bin/bash

# Simulation script for testing graceful shard transfer
echo "Starting shard distribution simulation..."
echo "Initial state: 3 running client instances"

# Wait for initial setup to stabilize
sleep 30
echo "Initial setup complete - all clients should have shards assigned"

# Start an additional client to trigger redistribution
echo "Starting additional client (client4)"
docker run -d --name client4 --hostname client4 --network shard-network shard-distributor_client1 -mode=client -id=instance-4 -server=distributor-server:50051 -duration=300s
sleep 20
echo "Client4 added - should have received some shards from existing clients"

# Gracefully shut down one instance to simulate graceful transfer
echo "Initiating graceful shutdown of client1"
docker stop client1
sleep 15
echo "Client1 stopped - shards should have been transferred to other clients"

# Start replacement instance
echo "Starting replacement client (client5)"
docker run -d --name client5 --hostname client5 --network shard-network shard-distributor_client1 -mode=client -id=instance-5 -server=distributor-server:50051 -duration=300s
sleep 20
echo "Client5 started - should have received some shards"

# Simulate ungraceful failure
echo "Simulating ungraceful failure of client2"
docker kill client2
echo "Client2 killed - shards should be reassigned within 10 seconds"
sleep 15

# Collect logs from remaining clients to verify handover times
echo "Collecting handover metrics from clients..."
for client in client3 client4 client5; do
  echo "===== $client metrics ====="
  docker logs $client | grep "transfer" || echo "No transfer data found"
  docker logs $client | grep "handover" || echo "No handover data found"
  docker logs $client | grep "latency" || echo "No latency data found"
done

# Final verification
echo "Verifying all shards are assigned..."
for server in $(docker ps -q -f name=distributor-server); do
  echo "Server $server logs:"
  docker logs $server | grep "Distribution changed" | tail -3
done

echo "Simulation complete!"
