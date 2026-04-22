#!/bin/bash
# Run IndexerJsoup after crawler has completed
# Usage: ./scripts/run-indexer-jsoup.sh
#
# Note: Debug logging is controlled via src/log.properties
# Set cis5550.jobs=DEBUG to enable detailed debug logs

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

echo "==================================="
echo "Building and Running IndexerJsoup"
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

# Compile all Java source files with jsoup + snowball available
echo "Compiling all source files (with jsoup + snowball)..."
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi
echo "✓ Compilation successful"
echo ""

# Build JAR with all classes (required for Flame workers)
echo "Building indexer-jsoup.jar..."
cd bin
jar cf ../indexer-jsoup.jar cis5550/ 2>/dev/null || {
    cd "$PROJECT_ROOT"
    echo "ERROR: Failed to create JAR file"
    exit 1
}
cd "$PROJECT_ROOT"

if [ ! -f "indexer-jsoup.jar" ]; then
    echo "ERROR: JAR file was not created"
    exit 1
fi

echo "✓ JAR created: indexer-jsoup.jar ($(ls -lh indexer-jsoup.jar | awk '{print $5}'))"
echo ""

# Submit IndexerJsoup job to Flame
echo "Submitting IndexerJsoup job to Flame Coordinator..."
echo "Logs will be written to: logs/indexer-jsoup.log"
echo ""

# result=$(java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.flame.FlameSubmit localhost:9000 indexer-jsoup.jar cis5550.jobs.IndexerJsoup 2>&1 | tee logs/indexer-jsoup.log)
result=$(java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.flame.FlameSubmit 44.200.108.27:9000 indexer-jsoup.jar cis5550.jobs.IndexerJsoup 2>&1 | tee logs/indexer-jsoup.log)
echo "$result"
echo ""

if echo "$result" | grep -q "OK\|successfully"; then
    echo "✓ IndexerJsoup job submitted successfully!"
    echo ""
    echo "The indexer is now running. To check progress:"
    echo "  - View logs: tail -f logs/indexer-jsoup.log"
    echo "  - View services: tmux attach -t services"
    echo "  - Check index table: curl http://localhost:8000/view/pt-index"
    echo "  - Check a specific word: curl http://localhost:8000/get/pt-index/{wordHash}/acc"
else
    echo "⚠ Warning: Job submission may have encountered issues"
    echo "Check the output above or logs/indexer-jsoup.log for details"
    echo ""
    echo "To view service logs: tmux attach -t services"
fi

