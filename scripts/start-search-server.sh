#!/bin/bash
# Start Search Server
# Usage: ./scripts/start-search-server.sh [port] [kvsCoordinator]
#   port: Port number for the search server (default: 8080)
#   kvsCoordinator: KVS coordinator address (default: localhost:8000)

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Default values
PORT=${1:-8080}
KVS_COORDINATOR=${2:-localhost:8000}

echo "==================================="
echo "Starting Search Server"
echo "==================================="
echo "Port: $PORT"
echo "KVS Coordinator: $KVS_COORDINATOR"
echo ""

# Check if KVS coordinator is running
echo "Checking KVS coordinator..."
if ! curl -s http://$KVS_COORDINATOR/ > /dev/null 2>&1; then
    echo "WARNING: KVS Coordinator not responding on $KVS_COORDINATOR"
    echo "The search server may not work correctly if KVS is not running."
    echo "Make sure services are running. Check with: ./scripts/check-services.sh"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✓ KVS Coordinator is running"
fi
echo ""

# Create bin directory if missing
mkdir -p bin

# JAR paths for compilation
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

# Compile all Java source files (with jsoup and snowball in classpath)
echo "Compiling source files (with jsoup and snowball)..."
javac -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi
echo "✓ Compilation successful"
echo ""

# Check if port is already in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "ERROR: Port $PORT is already in use"
    echo "Please choose a different port or stop the process using port $PORT"
    exit 1
fi

# Start the search server
echo "Starting Search Server on port $PORT..."
echo "Access the search interface at: http://localhost:$PORT"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.jobs.SearchServer $PORT $KVS_COORDINATOR

