#!/bin/bash
# Counts files in pt-crawl directory that contain the word "page"
# Usage: ./scripts/check-kvs-workers.sh [dataDir]
# Example: ./scripts/check-kvs-workers.sh workerOnlyoneJob

set -uo pipefail  # Removed -e so script continues even if individual checks fail

# Project root (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Remote data directory name under /data (argument or default)
REMOTE_DATA_DIR="${1:-workerOnlyoneJob}"
REMOTE_PT_CRAWL_DIR="/data/${REMOTE_DATA_DIR}/pt-crawl"
echo "Remote pt-crawl directory: $REMOTE_PT_CRAWL_DIR"
echo "Counting files with 'page' in content from $REMOTE_PT_CRAWL_DIR"

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

# Get both public and private IPs - we need public for SSH, private for matching with coordinator
if command -v jq &> /dev/null; then
  WORKER_PUBLIC_IPS=$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")
  WORKER_PRIVATE_IPS=$(terraform output -json kvs_worker_private_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")
else
  # Fallback: parse terraform output without jq
  WORKER_PUBLIC_IPS=$(terraform output kvs_worker_public_ips 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "")
  WORKER_PRIVATE_IPS=$(terraform output kvs_worker_private_ips 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' || echo "")
fi

cd ..

if [ -z "$WORKER_PUBLIC_IPS" ] || [ -z "$WORKER_PRIVATE_IPS" ]; then
  echo "Error: Could not get worker IPs from Terraform"
  exit 1
fi

TOTAL_TERRAFORM_WORKERS=$(echo "$WORKER_PUBLIC_IPS" | grep -v '^$' | wc -l | tr -d ' ')
echo "Found $TOTAL_TERRAFORM_WORKERS worker(s) in Terraform"
echo

# Get workers registered with coordinator for comparison
cd terraform
KVS_COORD_PUBLIC=$(terraform output -raw kvs_coordinator_public_ip 2>/dev/null || terraform output -raw kvs_coordinator_public_dns 2>/dev/null || echo "")
cd ..

REGISTERED_WORKERS=""
REGISTERED_COUNT=0
if [ -n "$KVS_COORD_PUBLIC" ]; then
  REGISTERED_WORKERS=$(curl -s --max-time 5 "http://${KVS_COORD_PUBLIC}:8000/workers" 2>/dev/null || echo "")
  REGISTERED_COUNT=$(echo "$REGISTERED_WORKERS" | head -n 1 | tr -d '\n\r ' 2>/dev/null || echo "0")
  echo "Workers registered with coordinator: $REGISTERED_COUNT"
  echo
fi

# Process each worker from Terraform
# TOTAL_FILES=0
# TOTAL_GREP_MATCHES=0
TOTAL_FILES_WITH_PAGE=0
# SUCCESSFUL_CHECKS=0
# FAILED_CHECKS=0
# SUCCESSFUL_GREP_CHECKS=0
# FAILED_GREP_CHECKS=0
SUCCESSFUL_PAGE_CHECKS=0
FAILED_PAGE_CHECKS=0

# Create temporary files with worker IPs to avoid subshell issues
TEMP_PUBLIC=$(mktemp)
TEMP_PRIVATE=$(mktemp)
echo "$WORKER_PUBLIC_IPS" > "$TEMP_PUBLIC"
echo "$WORKER_PRIVATE_IPS" > "$TEMP_PRIVATE"

# Create temporary directory for storing results
RESULTS_DIR=$(mktemp -d)
trap "rm -rf '$RESULTS_DIR' '$TEMP_PUBLIC' '$TEMP_PRIVATE'" EXIT

# Read worker IPs into arrays for parallel processing
WORKER_PUBLIC_ARR=()
WORKER_PRIVATE_ARR=()
exec 3< "$TEMP_PUBLIC"
exec 4< "$TEMP_PRIVATE"
while IFS= read -r WORKER_PUBLIC_IP <&3 && IFS= read -r WORKER_PRIVATE_IP <&4; do
  [ -z "$WORKER_PUBLIC_IP" ] || [ -z "$WORKER_PRIVATE_IP" ] && continue
  WORKER_PUBLIC_ARR+=("$WORKER_PUBLIC_IP")
  WORKER_PRIVATE_ARR+=("$WORKER_PRIVATE_IP")
done
exec 3<&-
exec 4<&-

# Function to check a single worker (runs in background)
check_worker() {
  local worker_num=$1
  local worker_public_ip=$2
  local worker_private_ip=$3
  local result_file="$RESULTS_DIR/worker_${worker_num}.txt"
  
  {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Worker $worker_num/$TOTAL_TERRAFORM_WORKERS (Public: $worker_public_ip, Private: $worker_private_ip):"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Fetch worker ID from storage directory (/data/<dir>/id)
    # WORKER_ID=$(ssh -i "$SSH_KEY" \
    #   -o StrictHostKeyChecking=no \
    #   -o ConnectTimeout=5 \
    #   -o BatchMode=yes \
    #   -o LogLevel=ERROR \
    #   ec2-user@"$worker_public_ip" \
    #   "cat /data/${REMOTE_DATA_DIR}/id 2>/dev/null" 2>/dev/null || echo "")
    # WORKER_ID=$(echo "${WORKER_ID:-UNKNOWN}" | tr -d '\n\r')
    # [ -z "$WORKER_ID" ] && WORKER_ID="UNKNOWN"
    # echo "Worker ID: $WORKER_ID"

    # Check if registered with coordinator using PRIVATE IP (coordinator uses private IPs)
    if [ -n "${REGISTERED_WORKERS:-}" ]; then
      if echo "$REGISTERED_WORKERS" | grep -q "$worker_private_ip" 2>/dev/null; then
        echo "✓ Registered with coordinator: YES"
      else
        echo "✗ Registered with coordinator: NO"
      fi
    fi
    
    # SSH and count files in the pt-crawl directory using PUBLIC IP (for external access)
    # FILE_COUNT=$(ssh -i "$SSH_KEY" \
    #   -o StrictHostKeyChecking=no \
    #   -o ConnectTimeout=5 \
    #   -o BatchMode=yes \
    #   -o LogLevel=ERROR \
    #   ec2-user@"$worker_public_ip" \
    #   "find '$REMOTE_PT_CRAWL_DIR' -type f -size +50k 2>/dev/null | wc -l" 2>/dev/null || echo "ERROR")
    # 
    # FILE_COUNT=$(echo "${FILE_COUNT:-ERROR}" | tr -d '\n\r ')
    # if [[ "$FILE_COUNT" =~ ^[0-9]+$ ]]; then
    #   echo "✓ Files: $FILE_COUNT"
    #   echo "STATS_FILE_COUNT:$FILE_COUNT" >> "$result_file"
    # else
    #   echo "✗ Files: ERROR (SSH failed or directory not accessible)"
    #   echo "STATS_FILE_COUNT:ERROR" >> "$result_file"
    # fi
    
    # Count occurrences of [PAGE_STORED] in ~/logs/crawler.log
    # GREP_COUNT=$(ssh -i "$SSH_KEY" \
    #   -o StrictHostKeyChecking=no \
    #   -o ConnectTimeout=5 \
    #   -o BatchMode=yes \
    #   -o LogLevel=ERROR \
    #   ec2-user@"$worker_public_ip" \
    #   "grep -c '\[PAGE_STORED\]' ~/logs/crawler.log 2>/dev/null || echo '0'" 2>/dev/null || echo "ERROR")
    
    # GREP_COUNT=$(echo "${GREP_COUNT:-ERROR}" | tr -d '\n\r ')
    # if [[ "$GREP_COUNT" =~ ^[0-9]+$ ]]; then
    #   echo "✓ [PAGE_STORED] occurrences: $GREP_COUNT"
    #   echo "STATS_GREP_COUNT:$GREP_COUNT" >> "$result_file"
    # else
    #   echo "✗ [PAGE_STORED] occurrences: ERROR (SSH failed or log file not accessible)"
    #   echo "STATS_GREP_COUNT:ERROR" >> "$result_file"
    # fi
    
    # Count files in pt-crawl directory that contain the word "page"
    FILES_WITH_PAGE=$(ssh -i "$SSH_KEY" \
      -o StrictHostKeyChecking=no \
      -o ConnectTimeout=5 \
      -o BatchMode=yes \
      -o LogLevel=ERROR \
      ec2-user@"$worker_public_ip" \
      "find '$REMOTE_PT_CRAWL_DIR' -type f -exec grep -l 'page' {} \; 2>/dev/null | wc -l" 2>/dev/null || echo "ERROR")
    
    FILES_WITH_PAGE=$(echo "${FILES_WITH_PAGE:-ERROR}" | tr -d '\n\r ')
    if [[ "$FILES_WITH_PAGE" =~ ^[0-9]+$ ]]; then
      echo "✓ Files with 'page' in content: $FILES_WITH_PAGE"
      echo "STATS_FILES_WITH_PAGE:$FILES_WITH_PAGE" >> "$result_file"
    else
      echo "✗ Files with 'page' in content: ERROR (SSH failed or directory not accessible)"
      echo "STATS_FILES_WITH_PAGE:ERROR" >> "$result_file"
    fi
    echo ""
  } > "$result_file"
}

# Launch all worker checks in parallel
echo "Launching parallel checks for $TOTAL_TERRAFORM_WORKERS worker(s)..."
for i in "${!WORKER_PUBLIC_ARR[@]}"; do
  worker_num=$((i + 1))
  check_worker "$worker_num" "${WORKER_PUBLIC_ARR[$i]}" "${WORKER_PRIVATE_ARR[$i]}" &
done

# Wait for all background jobs to complete
wait

# Print results sequentially in order
for i in "${!WORKER_PUBLIC_ARR[@]}"; do
  worker_num=$((i + 1))
  result_file="$RESULTS_DIR/worker_${worker_num}.txt"
  
  if [ -f "$result_file" ]; then
    # Print the output (but exclude the statistics lines)
    grep -v "^STATS_" "$result_file" | cat
    
    # Extract and accumulate statistics from the stats lines
    # FILE_COUNT=$(grep "^STATS_FILE_COUNT:" "$result_file" | cut -d: -f2- | tr -d '[:space:]')
    # GREP_COUNT=$(grep "^STATS_GREP_COUNT:" "$result_file" | cut -d: -f2- | tr -d '[:space:]')
    FILES_WITH_PAGE=$(grep "^STATS_FILES_WITH_PAGE:" "$result_file" | cut -d: -f2- | tr -d '[:space:]')
    
    # if [[ "$FILE_COUNT" =~ ^[0-9]+$ ]]; then
    #   TOTAL_FILES=$((TOTAL_FILES + FILE_COUNT))
    #   SUCCESSFUL_CHECKS=$((SUCCESSFUL_CHECKS + 1))
    # else
    #   FAILED_CHECKS=$((FAILED_CHECKS + 1))
    # fi
    
    # if [[ "$GREP_COUNT" =~ ^[0-9]+$ ]]; then
    #   TOTAL_GREP_MATCHES=$((TOTAL_GREP_MATCHES + GREP_COUNT))
    #   SUCCESSFUL_GREP_CHECKS=$((SUCCESSFUL_GREP_CHECKS + 1))
    # else
    #   FAILED_GREP_CHECKS=$((FAILED_GREP_CHECKS + 1))
    # fi
    
    if [[ "$FILES_WITH_PAGE" =~ ^[0-9]+$ ]]; then
      TOTAL_FILES_WITH_PAGE=$((TOTAL_FILES_WITH_PAGE + FILES_WITH_PAGE))
      SUCCESSFUL_PAGE_CHECKS=$((SUCCESSFUL_PAGE_CHECKS + 1))
    else
      FAILED_PAGE_CHECKS=$((FAILED_PAGE_CHECKS + 1))
    fi
  fi
done

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Total workers in Terraform:  $TOTAL_TERRAFORM_WORKERS"
echo "Workers registered:           $REGISTERED_COUNT"
# echo "Successfully checked:        $SUCCESSFUL_CHECKS"
# echo "Failed checks:               $FAILED_CHECKS"
# echo "Total files across all workers: $TOTAL_FILES"
# echo ""
# echo "Log file:                    ~/logs/crawler.log"
# echo "Successful log checks:      $SUCCESSFUL_GREP_CHECKS"
# echo "Failed log checks:          $FAILED_GREP_CHECKS"
# echo "Total [PAGE_STORED] occurrences: $TOTAL_GREP_MATCHES"
# echo ""
echo "Files with 'page' in content:"
echo "Successful checks:          $SUCCESSFUL_PAGE_CHECKS"
echo "Failed checks:              $FAILED_PAGE_CHECKS"
echo "Total files with 'page':    $TOTAL_FILES_WITH_PAGE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

