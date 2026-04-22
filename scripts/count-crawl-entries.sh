#!/bin/bash
# Count entries in pt-crawl table with both 'page' and 'url' columns present and non-null
# Usage: ./scripts/count-crawl-entries.sh [kvs_coordinator]
#   kvs_coordinator: KVS coordinator address (default: localhost:8000)
#   Example: ./scripts/count-crawl-entries.sh localhost:8000

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Default values
KVS_COORDINATOR=${1:-localhost:8000}

echo "==================================="
echo "Counting pt-crawl Entries"
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

# Run CountCrawlEntries
echo "Counting entries in pt-crawl..."
echo ""

java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.jobs.CountCrawlEntries "$KVS_COORDINATOR"

