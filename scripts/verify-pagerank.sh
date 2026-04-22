#!/bin/bash
# Script to verify PageRank results using VerifyPageRank.java

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

COORDINATOR="${1:-localhost:8000}"

echo "========================================"
echo "PageRank Verification Script"
echo "========================================"
echo ""

# Check if bin directory exists
if [ ! -d "bin" ]; then
    echo "Compiling verification tool..."
    JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
    SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"
    
    if [ ! -f "$JSOUP_JAR" ] || [ ! -f "$SNOWBALL_JAR" ]; then
        echo "ERROR: JAR files not found. Please run from project root."
        exit 1
    fi
    
    javac -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/tools/VerifyPageRank.java 2>&1 | head -20
    if [ $? -ne 0 ]; then
        echo "ERROR: Compilation failed"
        exit 1
    fi
    echo "✓ Compilation successful"
    echo ""
fi

echo "Running PageRank verification..."
echo "KVS Coordinator: $COORDINATOR"
echo ""

JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

java -cp "$JSOUP_JAR:$SNOWBALL_JAR:bin" cis5550.tools.VerifyPageRank "$COORDINATOR"

