#!/bin/bash
# Rebuild and Run Indexer
# Usage: ./rebuild-and-run-indexer.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "Rebuilding and Running Indexer"
echo "==================================="

# Clean old JAR
rm -f indexer.jar

# Recompile everything
echo "Compiling all source files..."
javac --release 21 -d bin src/cis5550/**/*.java 2>&1 | head -20
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Compilation failed!"
    exit 1
fi

# Build JAR with all classes
echo "Building indexer.jar..."
cd bin
jar cf ../indexer.jar cis5550/
cd "$PROJECT_ROOT"

echo "JAR size: $(ls -lh indexer.jar | awk '{print $5}')"
echo ""

# Check services
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "ERROR: Flame Coordinator not responding on port 9000"
    echo "Make sure services are running: ./scripts/check-services.sh"
    exit 1
fi

# Run Indexer
echo "Submitting Indexer job..."
java -cp "lib/*:bin" cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer

