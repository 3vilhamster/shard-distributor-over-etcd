#!/bin/bash
set -e

# Build command arguments
ARGS=""

# Set server address if provided
if [ ! -z "$SERVER_ADDR" ]; then
  ARGS="$ARGS --server=$SERVER_ADDR"
fi

# Set instance ID if provided
if [ ! -z "$INSTANCE_ID" ]; then
  ARGS="$ARGS --id=$INSTANCE_ID"
fi

# Set capacity if provided
if [ ! -z "$CAPACITY" ]; then
  ARGS="$ARGS --capacity=$CAPACITY"
fi

# Set shard types if provided
if [ ! -z "$SHARD_TYPES" ]; then
  ARGS="$ARGS --shard-types=$SHARD_TYPES"
fi

# Set log level if provided
if [ ! -z "$LOG_LEVEL" ]; then
  ARGS="$ARGS --log-level=$LOG_LEVEL"
fi

# Set heartbeat interval if provided
if [ ! -z "$HEARTBEAT_INTERVAL" ]; then
  ARGS="$ARGS --heartbeat=${HEARTBEAT_INTERVAL}s"
fi

# Set report interval if provided
if [ ! -z "$REPORT_INTERVAL" ]; then
  ARGS="$ARGS --report=${REPORT_INTERVAL}s"
fi

# Set metadata if provided
if [ ! -z "$METADATA" ]; then
  ARGS="$ARGS --metadata=$METADATA"
fi

# Check if we need to perform graceful shutdown after a delay
if [ ! -z "$SHUTDOWN_AFTER" ] && [ "$GRACEFUL_SHUTDOWN" = "true" ]; then
  echo "Client will gracefully shut down after $SHUTDOWN_AFTER seconds"

  # Start client in background
  /app/client $ARGS &
  CLIENT_PID=$!

  # Set up signal forwarding
  trap "kill -TERM $CLIENT_PID" TERM INT

  # Wait for specified time
  sleep "$SHUTDOWN_AFTER"

  echo "Initiating graceful shutdown..."
  kill -TERM $CLIENT_PID

  # Wait for client to exit
  wait $CLIENT_PID
else
  # Start client normally
  exec /app/client $ARGS
fi
