#!/bin/bash
# Run deploy-to-aws and then submit the crawler repeatedly with a delay.
# Usage: ./scripts/deploy-and-crawl-loop.sh <worker_storage_dir> [s3_bucket]
# Example: ./scripts/deploy-and-crawl-loop.sh /data/workerOnlyoneJob my-bucket

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <worker_storage_dir> [s3_bucket]"
  exit 1
fi

WORKER_STORAGE_DIR="$1"

while true; do
  echo "=== $(date) | Deploying to AWS ==="
  ./scripts/deploy-to-aws.sh "$WORKER_STORAGE_DIR"

  echo "=== $(date) | Submitting crawler job ==="
  ./scripts/submit-crawler-aws.sh seeds.txt 120 120 || true

  echo "=== $(date) | Sleeping 25 minutes ==="
  sleep 3000
done
