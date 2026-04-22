#!/bin/bash
# Restart KVS and Flame workers on all KVS worker instances with a new storage directory
# Usage: ./scripts/restart-kvs-workers.sh <new_storage_dir>
# Example: ./scripts/restart-kvs-workers.sh /data/newStorageDir

set -euo pipefail

# Configuration variables
REMOTE_PROJECT_DIR="/home/ec2-user/Fa25-CIS5550-Project-Hashira-kvs_optimization"

if [ $# -lt 1 ]; then
  echo "Error: Storage directory argument required"
  echo "Usage: $0 <storage_dir>"
  echo "Example: $0 /data/newStorageDir"
  exit 1
fi

NEW_STORAGE_DIR="$1"

# Project root
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Get SSH key path
SSH_KEY="${HOME}/Downloads/amanMBP.pem"
if [ ! -f "$SSH_KEY" ]; then
  echo "Error: SSH key not found at $SSH_KEY"
  exit 1
fi

# Get worker public IPs from Terraform
echo "Fetching KVS worker IPs from Terraform..."

if [ ! -d "terraform" ]; then
  echo "Error: terraform directory not found"
  exit 1
fi

cd terraform

# Get both public and private IPs
if command -v jq &> /dev/null; then
  WORKER_PUBLIC_IPS=$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")
  KVS_COORD_PRIVATE_IP=$(terraform output -json kvs_coordinator_private_ip 2>/dev/null | jq -r '.' 2>/dev/null || echo "")
  FLAME_COORD_PRIVATE_IP=$(terraform output -json flame_coordinator_private_ip 2>/dev/null | jq -r '.' 2>/dev/null || echo "")
else
  # Fallback: parse terraform output without jq
  WORKER_PUBLIC_IPS=$(terraform output kvs_worker_public_ips 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "")
  KVS_COORD_PRIVATE_IP=$(terraform output kvs_coordinator_private_ip 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "")
  FLAME_COORD_PRIVATE_IP=$(terraform output flame_coordinator_private_ip 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "")
fi

cd ..

if [ -z "$WORKER_PUBLIC_IPS" ]; then
  echo "Error: Could not get worker IPs from Terraform"
  exit 1
fi

if [ -z "$KVS_COORD_PRIVATE_IP" ] || [ -z "$FLAME_COORD_PRIVATE_IP" ]; then
  echo "Error: Could not get coordinator IPs from Terraform"
  exit 1
fi

TOTAL_WORKERS=$(echo "$WORKER_PUBLIC_IPS" | grep -v '^$' | wc -l | tr -d ' ')
echo "Found $TOTAL_WORKERS worker(s) in Terraform"
echo "New storage directory: $NEW_STORAGE_DIR"
echo "KVS Coordinator: $KVS_COORD_PRIVATE_IP:8000"
echo "Flame Coordinator: $FLAME_COORD_PRIVATE_IP:9000"
echo ""

# Create temporary file with worker IPs
TEMP_PUBLIC=$(mktemp)
echo "$WORKER_PUBLIC_IPS" > "$TEMP_PUBLIC"

# Process each worker
WORKER_NUM=0
SUCCESSFUL_RESTARTS=0
FAILED_RESTARTS=0

exec 3< "$TEMP_PUBLIC"

while IFS= read -r WORKER_PUBLIC_IP <&3; do
  [ -z "$WORKER_PUBLIC_IP" ] && continue

  WORKER_NUM=$((WORKER_NUM + 1))
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Worker $WORKER_NUM/$TOTAL_WORKERS (Public: $WORKER_PUBLIC_IP):"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Determine ports (KVS workers always use 8001, Flame workers use 9001-9021)
  # Port assignment matches terraform count.index (0-based)
  KVS_PORT=8001
  WORKER_INDEX=$((WORKER_NUM - 1))
  FLAME_PORT=$((9001 + WORKER_INDEX))
  if [ $FLAME_PORT -gt 9021 ]; then
    FLAME_PORT=$((9001 + (WORKER_INDEX % 21)))
  fi

  # Kill existing processes
  echo "  Killing existing KVS and Flame worker processes..."
  ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "sudo pkill -f 'cis5550.kvs.Worker' || true; sudo pkill -f 'cis5550.flame.Worker' || true; sleep 2" 2>/dev/null || true

  # Wait a moment for processes to fully terminate
  sleep 2

  # Create storage directory on EBS volume (requires sudo)
  echo "  Creating storage directory: $NEW_STORAGE_DIR"
  DIR_CREATED=$(ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "sudo mkdir -p '$NEW_STORAGE_DIR' && sudo chown ec2-user:ec2-user '$NEW_STORAGE_DIR' && echo 'ok' || echo 'failed'" 2>/dev/null || echo "failed")

  if [ "$DIR_CREATED" = "ok" ]; then
    echo "  ✓ Storage directory created successfully"
  else
    echo "  ⚠ Warning: Could not create directory (may already exist or EBS not mounted)"
    echo "  Continuing anyway - KVS worker will attempt to create it"
  fi

  # Check if processes are still running after kill
  REMAINING_PROCESSES=$(ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "pgrep -f 'cis5550.kvs.Worker|cis5550.flame.Worker' | wc -l" 2>/dev/null || echo "0")

  if [ "$REMAINING_PROCESSES" -gt 0 ]; then
    echo "  ⚠ Warning: $REMAINING_PROCESSES process(es) still running after kill attempt"
    echo "  Attempting force kill (SIGKILL)..."
    ssh -i "$SSH_KEY" \
      -o StrictHostKeyChecking=no \
      -o ConnectTimeout=10 \
      -o BatchMode=yes \
      -o LogLevel=ERROR \
      ec2-user@"$WORKER_PUBLIC_IP" \
      "sudo pkill -9 -f 'cis5550.kvs.Worker' || true; sudo pkill -9 -f 'cis5550.flame.Worker' || true; sleep 1" 2>/dev/null || true
    sleep 1
    
    # Verify again after force kill
    REMAINING_AFTER_FORCE=$(ssh -i "$SSH_KEY" \
      -o StrictHostKeyChecking=no \
      -o ConnectTimeout=10 \
      -o BatchMode=yes \
      -o LogLevel=ERROR \
      ec2-user@"$WORKER_PUBLIC_IP" \
      "pgrep -f 'cis5550.kvs.Worker|cis5550.flame.Worker' | wc -l" 2>/dev/null || echo "0")
    
    if [ "$REMAINING_AFTER_FORCE" -gt 0 ]; then
      echo "  ✗ Error: $REMAINING_AFTER_FORCE process(es) still running after force kill"
    else
      echo "  ✓ All processes terminated after force kill"
    fi
  else
    echo "  ✓ All processes terminated successfully"
  fi

  # Start KVS Worker
  echo "  Starting KVS Worker on port $KVS_PORT (as root)..."
  ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "cd '$REMOTE_PROJECT_DIR' && \
     sudo nohup java -cp bin cis5550.kvs.Worker $KVS_PORT '$NEW_STORAGE_DIR' $KVS_COORD_PRIVATE_IP:8000 >/tmp/kvs-worker.log 2>&1 &" 2>/dev/null || {
    echo "  ✗ Failed to start KVS Worker"
    FAILED_RESTARTS=$((FAILED_RESTARTS + 1))
    continue
  }

  # Wait a bit for KVS worker to start
  sleep 3

  # Start Flame Worker
  echo "  Starting Flame Worker on port $FLAME_PORT (as root)..."
  FLAME_WORKER_ID="flame-worker-${WORKER_NUM}"
  ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "cd '$REMOTE_PROJECT_DIR' && \
     export FLAME_WORKER_ID='$FLAME_WORKER_ID' && \
     sudo nohup java -cp bin cis5550.flame.Worker $FLAME_PORT $FLAME_COORD_PRIVATE_IP:9000 >/tmp/flame-worker.log 2>&1 &" 2>/dev/null || {
    echo "  ✗ Failed to start Flame Worker"
    FAILED_RESTARTS=$((FAILED_RESTARTS + 1))
    continue
  }

  # Verify processes are running
  sleep 2
  PROCESSES=$(ssh -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    -o ConnectTimeout=10 \
    -o BatchMode=yes \
    -o LogLevel=ERROR \
    ec2-user@"$WORKER_PUBLIC_IP" \
    "pgrep -f 'cis5550.kvs.Worker|cis5550.flame.Worker' | wc -l" 2>/dev/null || echo "0")

  if [ "$PROCESSES" -ge 2 ]; then
    echo "  ✓ Both workers started successfully (found $PROCESSES processes)"
    SUCCESSFUL_RESTARTS=$((SUCCESSFUL_RESTARTS + 1))
  else
    echo "  ⚠ Warning: Expected 2 processes, found $PROCESSES"
    FAILED_RESTARTS=$((FAILED_RESTARTS + 1))
  fi

  echo ""
done

# Close file descriptor and clean up
exec 3<&-
rm -f "$TEMP_PUBLIC"

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Total workers:              $TOTAL_WORKERS"
echo "Successfully restarted:    $SUCCESSFUL_RESTARTS"
echo "Failed restarts:           $FAILED_RESTARTS"
echo "New storage directory:     $NEW_STORAGE_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $FAILED_RESTARTS -eq 0 ]; then
  echo "✓ All workers restarted successfully!"
  exit 0
else
  echo "⚠ Some workers failed to restart. Check logs on failed workers."
  exit 1
fi

