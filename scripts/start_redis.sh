#!/usr/bin/env bash
# Start Redis Server locally
echo "Starting Redis server on port 6379..."
exec redis-server --daemonize no
