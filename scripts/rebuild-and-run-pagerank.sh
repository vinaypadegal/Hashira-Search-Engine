#!/bin/bash
# Rebuild and Run PageRank
# Usage: ./rebuild-and-run-pagerank.sh [threshold] [percentage]

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

THRESHOLD=${1:-0.01}
PERCENT=${2:-""}

echo "==================================="
echo "Rebuilding and Running PageRank"
echo "==================================="

# Clean old JAR
rm -f pagerank.jar

# Recompile everything
echo "Compiling all source files..."
javac --release 21 -d bin src/cis5550/**/*.java 2>&1 | head -20
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi

# Build JAR with all classes
echo "Building pagerank.jar..."
cd bin
jar cf ../pagerank.jar cis5550/
cd "$PROJECT_ROOT"

echo "JAR size: $(ls -lh pagerank.jar | awk '{print $5}')"
echo ""

# Check services
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "ERROR: Flame Coordinator not responding on port 9000"
    echo "Make sure services are running: ./scripts/check-services.sh"
    exit 1
fi

# Run PageRank
echo "Submitting PageRank job (threshold: $THRESHOLD${PERCENT:+", percentage: $PERCENT"} )..."
if [ -n "$PERCENT" ]; then
    java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank "$THRESHOLD" "$PERCENT"
else
    java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank "$THRESHOLD"
fi

