#!/bin/bash
# Run VerifyPageRank to verify PageRank computation results
# Usage: ./scripts/run-verify-pagerank.sh [kvs_coordinator]
#   kvs_coordinator: KVS coordinator address (default: localhost:8000)
#   Example: ./scripts/run-verify-pagerank.sh localhost:8000

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Default values
KVS_COORDINATOR=${1:-localhost:8000}

echo "==================================="
echo "Verifying PageRank Results"
echo "==================================="
echo "KVS Coordinator: $KVS_COORDINATOR"
echo ""

# Check if KVS coordinator is running
echo "Checking KVS coordinator..."
if ! curl -s http://$KVS_COORDINATOR/ > /dev/null 2>&1; then
    echo "ERROR: KVS Coordinator not responding on $KVS_COORDINATOR"
    echo "Make sure services are running. Check with: ./scripts/check-services.sh"
    exit 1
else
    echo "✓ KVS Coordinator is running"
fi
echo ""

# Create bin directory if missing
mkdir -p bin

# JAR paths for compilation (matching start-services.sh)
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

# Run VerifyPageRank
echo "Running VerifyPageRank..."
echo ""

java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.jobs.VerifyPageRank "$KVS_COORDINATOR"

