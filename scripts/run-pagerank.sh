#!/bin/bash
# Run PageRank computation
# Usage: ./scripts/run-pagerank.sh [threshold] [percentage]
#   threshold: Convergence threshold (default: 0.01)
#   percentage: Convergence percentage 0-100 (default: 100)
#
# Example:
#   ./scripts/run-pagerank.sh          # Uses defaults: 0.01, 100%
#   ./scripts/run-pagerank.sh 0.001   # Custom threshold, default percentage
#   ./scripts/run-pagerank.sh 0.001 95 # Custom threshold and percentage

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments (defaults if not provided)
THRESHOLD=${1:-0.01}
PERCENTAGE=${2:-100}

echo "==================================="
echo "Building and Running PageRank"
echo "==================================="
echo "Convergence threshold: $THRESHOLD"
echo "Convergence percentage: $PERCENTAGE%"
echo ""

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

# JAR paths (matching start-services.sh)
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

# Check JSoup JAR exists
if [ ! -f "$JSOUP_JAR" ]; then
    echo "ERROR: JSoup JAR not found at $JSOUP_JAR"
    echo "Please ensure JSoup is available in the libs directory"
    exit 1
fi

# Compile all Java source files with JSoup and Snowball in classpath
echo "Compiling all source files (with JSoup and Snowball)..."
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi
echo "✓ Compilation successful"
echo ""

# Build JAR with all classes including JSoup (required for Flame workers)
echo "Building pagerank.jar (including JSoup)..."
cd bin
# Create JAR with cis5550 classes
jar cf ../pagerank.jar cis5550/ 2>/dev/null || {
    cd "$PROJECT_ROOT"
    echo "ERROR: Failed to create JAR file"
    exit 1
}
cd "$PROJECT_ROOT"

# Add JSoup library to JAR (extract and merge)
echo "Adding JSoup library to JAR..."
TEMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'jsoup-temp')
cd "$TEMP_DIR"
jar xf "$JSOUP_JAR" 2>/dev/null
if [ -d "org" ]; then
    cd "$PROJECT_ROOT"
    jar uf pagerank.jar -C "$TEMP_DIR" org/ 2>/dev/null || {
        echo "WARNING: Could not add JSoup to JAR"
    }
    rm -rf "$TEMP_DIR"
else
    echo "WARNING: Could not extract JSoup from JAR"
    rm -rf "$TEMP_DIR"
fi
cd "$PROJECT_ROOT"

if [ ! -f "pagerank.jar" ]; then
    echo "ERROR: JAR file was not created"
    exit 1
fi

echo "✓ JAR created: pagerank.jar ($(ls -lh pagerank.jar | awk '{print $5}'))"
echo ""

# Submit PageRank job to Flame
echo "Submitting PageRank job to Flame Coordinator..."
echo "  Threshold: $THRESHOLD"
echo "  Percentage: $PERCENTAGE%"
echo "Logs will be written to: logs/pagerank.log"
echo ""

result=$(java -cp "$JSOUP_JAR:$SNOWBALL_JAR:bin" cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRankOptimized "$THRESHOLD" "$PERCENTAGE" 2>&1 | tee logs/pagerank.log)

echo "$result"
echo ""

if echo "$result" | grep -q "OK\|successfully"; then
    echo "✓ PageRank job submitted successfully!"
    echo ""
    echo "The PageRank computation is now running. To check progress:"
    echo "  - View logs: tail -f logs/pagerank.log"
    echo "  - View services: tmux attach -t services"
    echo "  - Check PageRank table: curl http://localhost:8000/view/pt-pageranks"
    echo "  - Check a specific URL's rank: curl http://localhost:8000/get/pt-pageranks/{urlHash}/rank"
    echo ""
    echo "Note: PageRank computation may take several minutes depending on the size of your crawl."
else
    echo "⚠ Warning: Job submission may have encountered issues"
    echo "Check the output above for details"
    echo ""
    echo "To view service logs: tmux attach -t services"
fi