#!/bin/bash
# Download /data/<folder> from all AWS worker instances
# Usage:
#   ./scripts/download-worker-data.sh <data_folder_name> [ssh_key_path]
# Example:
#   ./scripts/download-worker-data.sh worker3
#   ./scripts/download-worker-data.sh worker3 ~/Downloads/amanMBP.pem

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

DATA_FOLDER_NAME="$1"
if [ -z "$DATA_FOLDER_NAME" ]; then
  echo "Error: data folder name is required (the subfolder under /data, e.g. worker3)"
  echo "Usage: $0 <data_folder_name> [ssh_key_path]"
  exit 1
fi

# Get SSH key path from argument or use default (Downloads)
SSH_KEY="${2:-${HOME}/Downloads/amanMBP.pem}"
if [ ! -f "$SSH_KEY" ]; then
  echo "Error: SSH key not found at $SSH_KEY"
  echo "Usage: $0 <data_folder_name> [ssh_key_path]"
  echo "Example: $0 worker3 ~/Downloads/amanMBP.pem"
  exit 1
fi

chmod 600 "$SSH_KEY" 2>/dev/null || true

# Local output directory
OUTPUT_ROOT="$PROJECT_ROOT/worker-data"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SESSION_DIR="$OUTPUT_ROOT/data_${DATA_FOLDER_NAME}_$TIMESTAMP"
mkdir -p "$SESSION_DIR"

echo "==================================="
echo "Downloading /data/$DATA_FOLDER_NAME from all workers"
echo "==================================="
echo "Data folder: /data/$DATA_FOLDER_NAME"
echo "SSH key   : $SSH_KEY"
echo "Output dir: $SESSION_DIR"
echo

# Go to terraform directory to read outputs
cd "$PROJECT_ROOT/terraform"

if [ ! -f "terraform.tfstate" ]; then
  echo "Error: terraform.tfstate not found in terraform/. Run terraform apply first."
  exit 1
fi

echo "Getting worker public IPs from Terraform outputs..."

KVS_WORKER_IPS=""
if command -v jq >/dev/null 2>&1; then
  KVS_WORKER_IPS="$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")"
else
  echo "Note: jq not found, using text parsing for worker IPs..."
  KVS_WORKER_IPS="$(terraform output kvs_worker_public_ips 2>/dev/null | grep -oE '[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+' || echo "")"
fi

if [ -z "$KVS_WORKER_IPS" ]; then
  echo "Error: could not retrieve worker IPs from Terraform outputs (kvs_worker_public_ips)."
  exit 1
fi

cd "$PROJECT_ROOT"

download_from_worker() {
  local idx="$1"
  local ip="$2"
  local worker_name="worker-$idx"
  local worker_dir="$SESSION_DIR/$worker_name"

  mkdir -p "$worker_dir"

  if [ -z "$ip" ] || [ "$ip" = "null" ]; then
    echo "⚠ Skipping $worker_name: empty IP"
    return
  fi

  echo "📥 $worker_name ($ip): zipping /data/$DATA_FOLDER_NAME on remote host..."

  # Remote archive path
  local remote_archive="/tmp/${DATA_FOLDER_NAME}-${worker_name}.tar.gz"

  # Create tar.gz on remote host
  if ! ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=15 \
    ec2-user@"$ip" "bash -c 'set -e; if [ ! -d \"/data/$DATA_FOLDER_NAME\" ]; then echo \"Remote directory /data/$DATA_FOLDER_NAME does not exist\" >&2; exit 2; fi; tar -C /data -czf \"$remote_archive\" \"$DATA_FOLDER_NAME\"'"; then
    ssh_status=$?
    if [ $ssh_status -eq 2 ]; then
      echo "  ⚠ Remote directory /data/$DATA_FOLDER_NAME does not exist on $worker_name ($ip)"
    else
      echo "  ⚠ Failed to SSH into $worker_name ($ip) or create archive (exit code $ssh_status)"
    fi
    return
  fi

  # Copy archive back
  echo "  ⬇ Downloading archive from $worker_name..."
  if ! scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 \
    ec2-user@"$ip":"$remote_archive" "$worker_dir/" >/dev/null 2>&1; then
    echo "  ⚠ Failed to download archive from $worker_name"
    return
  fi

  # Optional: list contents locally
  local local_archive="$worker_dir/$(basename "$remote_archive")"
  if [ -f "$local_archive" ]; then
    echo "  ✓ Saved: $local_archive"
  else
    echo "  ⚠ Archive not found locally for $worker_name"
  fi
}

# Iterate over worker IPs (handle both space- and newline-separated lists), in parallel
idx=0
pids=()
for ip in $KVS_WORKER_IPS; do
  [ -z "$ip" ] && continue
  download_from_worker "$idx" "$ip" &
  pids+=($!)
  idx=$((idx + 1))
done

# Wait for all background downloads to finish
for pid in "${pids[@]}"; do
  wait "$pid" || true
done

echo
echo "==================================="
echo "Download complete"
echo "==================================="
echo "Archives stored under: $SESSION_DIR"
echo
echo "Example to inspect one archive:"
echo "  tar -tzf \"$SESSION_DIR/worker-0/${DATA_FOLDER_NAME}-worker-0.tar.gz\" | head"


