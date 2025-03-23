#!/bin/bash
set -e

# Use environment variables if set, otherwise use defaults
SERVER_ADDR=${SERVER_ADDR:-"server:50051"}
NAMESPACES=${NAMESPACES:-"default"}
LOG_LEVEL=${LOG_LEVEL:-"info"}
HEARTBEAT_INTERVAL=${HEARTBEAT_INTERVAL:-5}
REPORT_INTERVAL=${REPORT_INTERVAL:-10}
SHUTDOWN_AFTER=${SHUTDOWN_AFTER:-0}
GRACEFUL_SHUTDOWN=${GRACEFUL_SHUTDOWN:-false}

# Build the command with all parameters
CMD="/app/client"
CMD+=" --server=${SERVER_ADDR}"
CMD+=" --namespaces=${NAMESPACES}"
CMD+=" --log-level=${LOG_LEVEL}"
CMD+=" --heartbeat=${HEARTBEAT_INTERVAL}s"
CMD+=" --report=${REPORT_INTERVAL}s"

# Log the command
echo "Starting client with command: $CMD"

# Execute the client with all arguments
if [ "$GRACEFUL_SHUTDOWN" = "true" ] && [ "$SHUTDOWN_AFTER" -gt 0 ]; then
  # Start the client in the background
  $CMD &
  PID=$!

  # Wait for specified time
  echo "Client started, will gracefully shutdown after ${SHUTDOWN_AFTER} seconds"
  sleep ${SHUTDOWN_AFTER}

  # Send SIGTERM for graceful shutdown
  echo "Initiating graceful shutdown..."
  kill -TERM $PID

  # Wait for client to terminate
  wait $PID
  exit_code=$?
  echo "Client exited with code ${exit_code}"
  exit $exit_code
else
  # Start the client in the foreground
  exec $CMD
fi
