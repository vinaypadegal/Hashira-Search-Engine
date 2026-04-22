#!/bin/bash
# Run PageRank Reverse (Inlink-based) computation on AWS
# Usage: ./scripts/run-pagerank-reverse-aws.sh [threshold] [percentage] [terraform_dir]
#   threshold: Convergence threshold (default: 0.01)
#   percentage: Convergence percentage 0-100 (default: 100)
#   terraform_dir: Terraform directory to use (default: terraform)
#
# Example:
#   ./scripts/run-pagerank-reverse-aws.sh                    # Uses defaults: 0.01, 100%, terraform
#   ./scripts/run-pagerank-reverse-aws.sh 0.001              # Custom threshold, default percentage
#   ./scripts/run-pagerank-reverse-aws.sh 0.001 95          # Custom threshold and percentage
#   ./scripts/run-pagerank-reverse-aws.sh 0.001 95 terraform-indexer  # All custom

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments (defaults if not provided)
THRESHOLD=${1:-0.01}
PERCENTAGE=${2:-100}
TERRAFORM_DIR=${3:-terraform}

echo "==================================="
echo "Building and Running PageRank Reverse (Inlink-based)"
echo "==================================="
echo "Convergence threshold: $THRESHOLD"
echo "Convergence percentage: $PERCENTAGE%"
echo "Using PageRankReverse class"
echo ""

# --- Get Flame Coordinator endpoint from Terraform ---

echo "Fetching Flame coordinator address from Terraform directory: $TERRAFORM_DIR..."

if [ -d "$TERRAFORM_DIR" ]; then
  FLAME_HOST=$(cd "$TERRAFORM_DIR" && terraform output -raw flame_coordinator_public_dns 2>/dev/null || true)
  if [ -z "$FLAME_HOST" ]; then
    FLAME_HOST=$(cd "$TERRAFORM_DIR" && terraform output -raw flame_coordinator_public_ip 2>/dev/null || true)
  fi
else
  FLAME_HOST=""
fi

if [ -z "$FLAME_HOST" ]; then
  echo "Error: Could not get flame_coordinator_public_dns or public_ip from Terraform directory: $TERRAFORM_DIR"
  echo "Make sure 'terraform apply' has been run in $TERRAFORM_DIR and outputs are available."
  exit 1
fi

FLAME_ENDPOINT="${FLAME_HOST}:9000"
echo "Using Flame coordinator at: $FLAME_ENDPOINT"
echo ""

# --- Build pagerank-reverse.jar locally ---

# JAR paths for compilation (matching start-services.sh)
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

echo "Compiling sources (with jsoup and snowball)..."
mkdir -p bin
# Force clean compile - remove old class files to ensure fresh build
find bin -name "*.class" -delete 2>/dev/null || true
javac --release 21 -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin src/cis5550/**/*.java

echo "Building pagerank-reverse.jar..."
mkdir -p jars
JAR_FILE="$PROJECT_ROOT/jars/pagerank-reverse.jar"
# Ensure absolute path
JAR_FILE="$(cd "$(dirname "$JAR_FILE")" && pwd)/$(basename "$JAR_FILE")"
# Remove old JAR to ensure fresh build
rm -f "$JAR_FILE"
MANIFEST_FILE=$(mktemp)
printf 'Manifest-Version: 1.0\nMain-Class: cis5550.jobs.PageRankReverse\n' > "$MANIFEST_FILE"

jar cfm "$JAR_FILE" "$MANIFEST_FILE" -C bin cis5550 >/dev/null 2>&1
rm -f "$MANIFEST_FILE"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: Failed to create pagerank-reverse.jar"
  exit 1
fi

JAR_SIZE=$(stat -f%z "$JAR_FILE" 2>/dev/null || stat -c%s "$JAR_FILE" 2>/dev/null || echo "unknown")
echo "JAR file created: $JAR_FILE (size: $JAR_SIZE bytes)"
echo ""

# --- Submit PageRank Reverse job ---

echo "Submitting PageRank Reverse job to $FLAME_ENDPOINT ..."
echo "  Threshold: $THRESHOLD"
echo "  Percentage: $PERCENTAGE%"
echo ""

result=$(java -cp bin cis5550.flame.FlameSubmit "$FLAME_ENDPOINT" "$JAR_FILE" cis5550.jobs.PageRankReverse "$THRESHOLD" "$PERCENTAGE" 2>&1)
echo "$result"
echo ""

if echo "$result" | grep -q "OK"; then
    echo "✓ PageRank Reverse job submitted successfully!"
else
    echo "⚠ Warning: Job submission may have encountered issues"
    echo "Check the output above for details"
fi

