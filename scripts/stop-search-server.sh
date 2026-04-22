#!/bin/bash
# Stop Search Server
# Usage: ./scripts/stop-search-server.sh [port]
#   port: Port number of the search server (default: 8080)

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Default port
PORT=${1:-8080}

echo "==================================="
echo "Stopping Search Server"
echo "==================================="
echo "Port: $PORT"
echo ""

# Find process using the port
PID=$(lsof -ti :$PORT 2>/dev/null || true)

if [ -z "$PID" ]; then
    echo "No process found running on port $PORT"
    echo "Search server may already be stopped"
    exit 0
fi

echo "Found process $PID on port $PORT"
echo "Stopping search server..."

# Try graceful shutdown first (SIGTERM)
kill $PID 2>/dev/null || true

# Wait a bit for graceful shutdown
sleep 2

# Check if process is still running
if kill -0 $PID 2>/dev/null; then
    echo "Process still running, forcing shutdown..."
    kill -9 $PID 2>/dev/null || true
    sleep 1
fi

# Verify it's stopped
if lsof -ti :$PORT >/dev/null 2>&1; then
    echo "ERROR: Failed to stop process on port $PORT"
    exit 1
else
    echo "✓ Search server stopped successfully"
fi

