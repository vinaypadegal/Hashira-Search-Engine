#!/bin/bash

# Script to run KVS Benchmark
# Usage: ./scripts/run-kvs-benchmark.sh [test_data_directory] [flame_coordinator]

set -e

# Default values
TEST_DATA_DIR="${1:-./test-data}"
FLAME_COORD="${2:-localhost:9000}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== KVS Benchmark Runner ===${NC}"
echo ""

# Check if test data directory exists
if [ ! -d "$TEST_DATA_DIR" ]; then
    echo -e "${YELLOW}Test data directory '$TEST_DATA_DIR' not found.${NC}"
    echo "Generating test data..."
    
    # Compile the test data generator if needed
    if [ ! -f "out/cis5550/test/GenerateTestData.class" ]; then
        echo "Compiling GenerateTestData..."
        javac -cp "lib/*:out:src" -d out src/cis5550/test/GenerateTestData.java 2>/dev/null || \
        javac -cp "lib/*" -d out src/cis5550/test/GenerateTestData.java
    fi
    
    # Generate test data (100 files, 1KB to 100KB each)
    echo "Generating 100 test files (1KB-100KB each)..."
    java -cp "lib/*:out:src" \
         cis5550.test.GenerateTestData "$TEST_DATA_DIR" 100 1 100
    
    if [ ! -d "$TEST_DATA_DIR" ]; then
        echo "Error: Failed to generate test data directory"
        exit 1
    fi
fi

# Count files in test directory
FILE_COUNT=$(find "$TEST_DATA_DIR" -type f | wc -l)
if [ "$FILE_COUNT" -eq 0 ]; then
    echo "Error: No files found in test data directory '$TEST_DATA_DIR'"
    exit 1
fi

echo "Found $FILE_COUNT files in test data directory"
echo ""

# Compile benchmark and FlameSubmit if needed
if [ ! -f "out/cis5550/test/KvsBenchmark.class" ] || [ ! -f "out/cis5550/flame/FlameSubmit.class" ]; then
    echo "Compiling KvsBenchmark and dependencies..."
    javac -cp "lib/*:out:src" -d out \
          src/cis5550/test/KvsBenchmark.java \
          src/cis5550/flame/FlameSubmit.java 2>/dev/null || \
    javac -cp "lib/*" -d out \
          src/cis5550/test/KvsBenchmark.java \
          src/cis5550/flame/FlameSubmit.java
fi

# Create JAR if needed
if [ ! -f "kvsBenchmark.jar" ] || [ "src/cis5550/test/KvsBenchmark.java" -nt "kvsBenchmark.jar" ]; then
    echo "Creating kvsBenchmark.jar..."
    jar cvf kvsBenchmark.jar -C out . 2>/dev/null || jar cvf kvsBenchmark.jar -C out cis5550/
fi

# Check if services are running
echo "Checking if KVS and Flame services are running..."
if ! curl -s "http://${FLAME_COORD}/" > /dev/null 2>&1; then
    echo -e "${YELLOW}Warning: Flame coordinator at $FLAME_COORD is not responding${NC}"
    echo "Make sure services are started with: ./scripts/start-services.sh"
    echo ""
fi

# Run the benchmark
echo -e "${GREEN}Running KVS Benchmark...${NC}"
echo "Test data: $TEST_DATA_DIR"
echo "Flame coordinator: $FLAME_COORD"
echo ""
echo "Available modes:"
echo "  'direct' - Direct KVS calls (coordinator -> KVS worker) - default"
echo "  'flame'  - Flame operations (coordinator -> Flame worker -> KVS worker)"
echo "            This mode tests the optimized worker batching!"
echo ""

# Check if mode argument provided
MODE="${3:-direct}"

echo "Running in '$MODE' mode..."
echo ""

java -cp "lib/*:out:src" \
     cis5550.flame.FlameSubmit "$FLAME_COORD" kvsBenchmark.jar \
     cis5550.test.KvsBenchmark "$TEST_DATA_DIR" "$MODE"

echo ""
echo -e "${GREEN}Benchmark completed!${NC}"
echo ""
echo "Tip: Run with 'flame' mode to test the optimized worker operations:"
echo "  ./scripts/run-kvs-benchmark.sh $TEST_DATA_DIR $FLAME_COORD flame"

