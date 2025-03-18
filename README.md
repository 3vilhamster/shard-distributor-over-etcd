# shard-distributor-over-etcd
Experiment over shard distributor on external etcd

# Shard Distributor PoC Runbook

This runbook provides step-by-step instructions to run and verify the Shard Distributor PoC.

## Prerequisites

- Go 1.18 or later
- Docker and Docker Compose
- [buf](https://github.com/bufbuild/buf) installed for Proto generation

## Running the PoC

### Option 1: Using Docker Compose
Run with Docker Compose:
```
docker-compose up
```

### Option 2: Running Locally

1. Install and run etcd:
```
# Using Docker
docker run -d --name etcd-server -p 2379:2379 -p 2380:2380 \
  --env ALLOW_NONE_AUTHENTICATION=yes \
  --env ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
  bitnami/etcd:latest
```

2. Build the application:
```
go build -o shard-distributor cmd/poc/poc.go
```

3. Run the server:
```
./shard-distributor -mode=server -shards=20
```

4. In another terminal, run clients:
```
./shard-distributor -mode=client -id=instance -clients=3 -duration=60s -failover=true
```

## Monitoring and Verification

1. Check etcd state:
```
# List registered services
etcdctl get --prefix /services

# Show shard assignments
etcdctl get --prefix /shards

# Check leader information
etcdctl get --prefix /shard-distributor
```

2. Observe logs for:
- Leader election ("Became leader for shard distribution")
- Shard assignments ("Sending initial assignment of shard...")
- Failover ("Instance appears to be unhealthy...")
- Redistribution timing (look for timestamps between REVOKE and ASSIGN actions)

3. Verify key aspects:
- Fast activation during preparation (look for "Fast activation of shard" messages)
- Graceful redistribution during draining (when an instance reports DRAINING status)
- Ungraceful failover when an instance is killed

## Testing Scenarios

1. **Graceful Shutdown**:
```
# In a new terminal, call the client with a short duration
./shard-distributor -mode=client -id=graceful-test -duration=10s
```
Watch logs for DRAINING status and shard reassignment process.

2. **Ungraceful Failure**:
```
# Find the process ID of a running client
ps aux | grep "shard-distributor -mode=client"

# Kill it abruptly
kill -9 <PID>
```
Watch logs for detection of the failed instance and shard reassignment.

3. **Scale Up**:
```
# Add new instances while system is running
./shard-distributor -mode=client -id=scale-up-test -duration=30s
```
Observe how shards are redistributed to include the new instance.

## Common Issues and Fixes

1. **etcd Connection Issues**:
    - Ensure etcd is running: `docker ps | grep etcd`
    - Check connectivity: `etcdctl endpoint health`

2. **gRPC Stream Errors**:
    - Increase log verbosity to debug stream issues
    - Check for network connectivity between clients and server

3. **Leader Election Problems**:
    - Check etcd session TTL settings
    - Verify leader election path in etcd: `etcdctl get --prefix /shard-distributor/leader`

4. **Shard Assignment Delays**:
    - Monitor timestamps in logs to identify bottlenecks
    - Check for mutex contention in the server

## Cleanup

```
# Stop and remove Docker containers
docker-compose down

# Or if running individually:
docker stop etcd-server
docker rm etcd-server
```

## Next Steps

After verifying the basic functionality, consider:

1. Implementing multiple service groups
2. Adding custom metrics collection
3. Implementing a more sophisticated load-aware distribution strategy
4. Adding performance benchmarks to measure actual distribution delays
