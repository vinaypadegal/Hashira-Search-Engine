#!/bin/bash
# Run Indexer after crawler has completed
# Usage: ./scripts/run-indexer.sh
#
# Note: Debug logging is controlled via src/log.properties
# Set cis5550.jobs=DEBUG to enable detailed debug logs

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "Building and Running Indexer"
echo "==================================="

# Check services are running
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "ERROR: Flame Coordinator not responding on port 9000"
    echo "Make sure services are running. Check with: ./scripts/check-services.sh"
    exit 1
fi

if ! curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "ERROR: KVS Coordinator not responding on port 8000"
    echo "Make sure services are running. Check with: ./scripts/check-services.sh"
    exit 1
fi

# Create bin and logs directories if missing
mkdir -p bin
mkdir -p logs

# Compile all Java source files
echo "Compiling all source files..."
javac --release 21 -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi
echo "✓ Compilation successful"
echo ""

# Build JAR with all classes (required for Flame workers)
echo "Building indexer.jar..."
cd bin
jar cf ../indexer.jar cis5550/ 2>/dev/null || {
    cd "$PROJECT_ROOT"
    echo "ERROR: Failed to create JAR file"
    exit 1
}
cd "$PROJECT_ROOT"

if [ ! -f "indexer.jar" ]; then
    echo "ERROR: JAR file was not created"
    exit 1
fi

echo "✓ JAR created: indexer.jar ($(ls -lh indexer.jar | awk '{print $5}'))"
echo ""

# Submit Indexer job to Flame
echo "Submitting IndexerOptimized job to Flame Coordinator..."
echo "Logs will be written to: logs/indexer.log"
echo ""

result=$(java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.IndexerOptimized 2>&1 | tee logs/indexer.log)
echo "$result"
echo ""

if echo "$result" | grep -q "OK\|successfully"; then
    echo "✓ Indexer job submitted successfully!"
    echo ""
    echo "The indexer is now running. To check progress:"
    echo "  - View logs: tail -f logs/indexer.log"
    echo "  - View services: tmux attach -t services"
    echo "  - Check index table: curl http://localhost:8000/view/pt-index"
    echo "  - Check a specific word: curl http://localhost:8000/get/pt-index/{wordHash}/acc"
else
    echo "⚠ Warning: Job submission may have encountered issues"
    echo "Check the output above or logs/indexer.log for details"
    echo ""
    echo "To view service logs: tmux attach -t services"
fi