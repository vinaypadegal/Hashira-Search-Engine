#!/bin/bash
# Submit Indexer Job to Running Flame Services
# Usage: ./submit-indexer.sh [inputTable] [outputTable]
#
# Default input table for your crawler is "pt-crawl"
# Default output table is "pt-index"
#
# Prerequisites: KVS and Flame services must be running
# Use ./start-services.sh to start them first

set -e

# Get project root (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments
inputTable=${1:-"pt-crawl"}
outputTable=${2:-"pt-index"}

echo "==================================="
echo "Submitting Indexer Job"
echo "==================================="
echo "Input Table: $inputTable"
echo "Output Table: $outputTable"
echo ""

# Check that services are running
if ! curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "Error: Flame Coordinator is not responding on port 9000"
    echo "Please start services first using: ./start-services.sh"
    exit 1
fi

if ! curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "Error: KVS Coordinator is not responding on port 8000"
    echo "Please start services first using: ./start-services.sh"
    exit 1
fi

# Create bin directory if missing
mkdir -p bin

# Compile if necessary
if [ ! -d "bin/cis5550" ] || [ "src/cis5550/jobs/Indexer2.java" -nt "bin/cis5550/jobs/Indexer2.class" ]; then
    echo "Compiling source files to bin/..."
    javac --release 21 -d bin src/cis5550/**/*.java
fi

# Create JAR
echo "Creating JAR file for Indexer..."
mkdir -p jars
jarFile="$PROJECT_ROOT/jars/indexer.jar"

# Temporary manifest
manifestFile=$(mktemp)
echo "Manifest-Version: 1.0" > "$manifestFile"
echo "Main-Class: cis5550.jobs.Indexer2" >> "$manifestFile"

# Build JAR (include full cis5550 directory)
cd bin
echo "Including all classes in JAR for proper execution..."
jar cfm "$jarFile" "$manifestFile" cis5550/ 2>/dev/null || {
    echo "Error: Failed to create JAR file"
    cd "$PROJECT_ROOT"
    rm -f "$manifestFile"
    exit 1
}
cd "$PROJECT_ROOT"
rm -f "$manifestFile"

if [ ! -f "$jarFile" ]; then
    echo "Error: Failed to create JAR file"
    exit 1
fi

echo "JAR file created: $jarFile"
echo ""

# Submit the Indexer job
echo "Submitting indexer job to Flame Coordinator..."

result=$(java -cp bin cis5550.flame.FlameSubmit \
    localhost:9000 \
    "$jarFile" \
    cis5550.jobs.Indexer2 \
    "$inputTable" \
    "$outputTable" 2>&1)

echo ""
echo "Job submission result:"
echo "$result"
echo ""

if echo "$result" | grep -q "OK"; then
    echo "✓ Indexer job submitted successfully!"
    echo ""
    echo "The indexer is now running. You can:"
    echo "  - View services: tmux attach -t services"
    echo "  - Check index data: curl http://localhost:8000/view/$outputTable"
else
    echo "⚠ Warning: Job submission may have encountered issues"
    echo "Check the output above for details"
fi
