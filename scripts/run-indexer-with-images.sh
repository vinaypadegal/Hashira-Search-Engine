#!/bin/bash
# Build and run IndexerWithImages (indexes pt-crawl -> pt-index and pt-image-crawl -> pt-image-index)
# Usage: ./scripts/run-indexer-with-images.sh [flame_coordinator]
#   flame_coordinator: host:port of Flame coordinator (default: localhost:9000)

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

FLAME_COORDINATOR="${1:-localhost:9000}"

echo "=========================================="
echo " IndexerWithImages"
echo "=========================================="
echo "Flame Coordinator: $FLAME_COORDINATOR"
echo

# JAR paths for compilation
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

if [ ! -f "$JSOUP_JAR" ] || [ ! -f "$SNOWBALL_JAR" ]; then
  echo "ERROR: Required JARs not found (jsoup, snowball)."
  echo "Expected at:"
  echo "  $JSOUP_JAR"
  echo "  $SNOWBALL_JAR"
  exit 1
fi

echo "Compiling sources (with jsoup and snowball)..."
mkdir -p bin
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java 2>&1 | head -30
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "ERROR: Compilation failed."
  exit 1
fi
echo "✓ Compilation successful"
echo

echo "Building indexer-with-images.jar..."
mkdir -p jars
JAR_PATH="$PROJECT_ROOT/jars/indexer-with-images.jar"
rm -f "$JAR_PATH"
jar cf "$JAR_PATH" -C bin cis5550 >/dev/null 2>&1
if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: Failed to create JAR at $JAR_PATH"
  exit 1
fi
echo "✓ JAR created: $JAR_PATH"
echo

echo "Submitting job to Flame..."
java -cp "bin:$JSOUP_JAR:$SNOWBALL_JAR" cis5550.flame.FlameSubmit "$FLAME_COORDINATOR" "$JAR_PATH" cis5550.jobs.IndexerWithImages
echo
echo "Done. Tables populated:"
echo "  - pt-index (from pt-crawl)"
echo "  - pt-image-index (from pt-image-crawl)"

