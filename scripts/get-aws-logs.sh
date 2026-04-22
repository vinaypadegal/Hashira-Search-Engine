#!/bin/bash
# Get logs from all AWS instances
# Usage: ./get-aws-logs.sh [ssh_key_path]
# Example: ./get-aws-logs.sh ~/Downloads/amanMBP.pem

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Get SSH key path from argument or use default
SSH_KEY="${1:-${HOME}/Downloads/amanMBP.pem}"
if [ ! -f "$SSH_KEY" ]; then
    echo "Error: SSH key not found at $SSH_KEY"
    echo "Usage: $0 [ssh_key_path]"
    echo "Example: $0 ~/Downloads/amanMBP.pem"
    exit 1
fi

# Set permissions on SSH key
chmod 600 "$SSH_KEY" 2>/dev/null || true

# Create aws-logs directory
AWS_LOGS_DIR="$PROJECT_ROOT/aws-logs"
mkdir -p "$AWS_LOGS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SESSION_DIR="$AWS_LOGS_DIR/logs_$TIMESTAMP"
mkdir -p "$SESSION_DIR"

echo "==================================="
echo "Fetching AWS Instance Logs"
echo "==================================="
echo "SSH Key: $SSH_KEY"
echo "Output Directory: $SESSION_DIR"
echo ""

# Change to terraform directory to get outputs
cd terraform

# Check if terraform is initialized
if [ ! -f "terraform.tfstate" ]; then
    echo "Error: terraform.tfstate not found. Make sure terraform has been applied."
    exit 1
fi

# Get instance IPs from terraform outputs
echo "Getting instance IPs from Terraform outputs..."
KVS_COORD_IP=$(terraform output -raw kvs_coordinator_public_ip 2>/dev/null || echo "")
FLAME_COORD_IP=$(terraform output -raw flame_coordinator_public_ip 2>/dev/null || echo "")

# Get worker IPs - try with jq first, fallback to manual parsing
if command -v jq &> /dev/null; then
    KVS_WORKER_IPS=$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r '.[]' 2>/dev/null || echo "")
else
    echo "Note: jq not found, using alternative method to get worker IPs..."
    # Get all IPs, one per line
    KVS_WORKER_IPS=$(terraform output kvs_worker_public_ips 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | tr '\n' ' ' | sed 's/ $/\n/' || echo "")
fi

if [ -z "$KVS_COORD_IP" ] && [ -z "$FLAME_COORD_IP" ] && [ -z "$KVS_WORKER_IPS" ]; then
    echo "Warning: Could not get IPs from terraform outputs. Trying alternative method..."
    # Fallback: try to parse terraform.tfstate directly (requires jq)
    if command -v jq &> /dev/null; then
        KVS_COORD_IP=$(jq -r '.resources[] | select(.type == "aws_instance" and .name == "kvs_coordinator") | .instances[0].attributes.public_ip' terraform.tfstate 2>/dev/null | grep -v null | head -1 || echo "")
        FLAME_COORD_IP=$(jq -r '.resources[] | select(.type == "aws_instance" and .name == "flame_coordinator") | .instances[0].attributes.public_ip' terraform.tfstate 2>/dev/null | grep -v null | head -1 || echo "")
    else
        echo "Error: Cannot parse terraform.tfstate without jq. Please install jq or ensure terraform outputs are available."
        exit 1
    fi
fi

cd "$PROJECT_ROOT"

# Function to fetch logs from an instance
fetch_logs() {
    local instance_name=$1
    local ip=$2
    local instance_dir="$SESSION_DIR/$instance_name"
    mkdir -p "$instance_dir"
    
    if [ -z "$ip" ] || [ "$ip" == "null" ]; then
        echo "⚠ Skipping $instance_name: No IP address"
        return
    fi
    
    echo "📥 Fetching logs from $instance_name ($ip)..."
    
    # SSH and get logs
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        ec2-user@"$ip" "bash -s" <<'ENDSSH' > "$instance_dir/logs.txt" 2>&1 || echo "Failed to connect to $instance_name" > "$instance_dir/error.txt"
    echo "=== System Info ==="
    hostname
    date
    echo ""
    
    echo "=== Java Processes ==="
    ps aux | grep java | grep -v grep || echo "No Java processes found"
    echo ""
    
    echo "=== KVS Coordinator Log ==="
    # Check new location first (timestamped), then fallback to old location
    KVS_COORD_LOG=$(ls -t /home/ec2-user/logs/kvs-coordinator-*.log 2>/dev/null | head -1)
    if [ -n "$KVS_COORD_LOG" ] && [ -f "$KVS_COORD_LOG" ]; then
        tail -1000 "$KVS_COORD_LOG"
    elif [ -f /tmp/kvs-coordinator.log ]; then
        tail -1000 /tmp/kvs-coordinator.log
    else
        echo "Log file not found: /home/ec2-user/logs/kvs-coordinator-*.log or /tmp/kvs-coordinator.log"
    fi
    echo ""
    
    echo "=== KVS Worker Log ==="
    # Check new location first (timestamped), then fallback to old location
    KVS_WORKER_LOG=$(ls -t /home/ec2-user/logs/kvs-worker-*.log 2>/dev/null | head -1)
    if [ -n "$KVS_WORKER_LOG" ] && [ -f "$KVS_WORKER_LOG" ]; then
        tail -1000 "$KVS_WORKER_LOG"
    elif [ -f /tmp/kvs-worker.log ]; then
        tail -1000 /tmp/kvs-worker.log
    else
        echo "Log file not found: /home/ec2-user/logs/kvs-worker-*.log or /tmp/kvs-worker.log"
    fi
    echo ""
    
    echo "=== Flame Coordinator Log ==="
    # Check new location first (timestamped), then fallback to old location
    FLAME_COORD_LOG=$(ls -t /home/ec2-user/logs/flame-coordinator-*.log 2>/dev/null | head -1)
    if [ -n "$FLAME_COORD_LOG" ] && [ -f "$FLAME_COORD_LOG" ]; then
        tail -1000 "$FLAME_COORD_LOG"
    elif [ -f /tmp/flame-coordinator.log ]; then
        tail -1000 /tmp/flame-coordinator.log
    else
        echo "Log file not found: /home/ec2-user/logs/flame-coordinator-*.log or /tmp/flame-coordinator.log"
    fi
    echo ""
    
    echo "=== Flame Worker Log(s) ==="
    # Check new location first (timestamped), then fallback to old location
    # Since workers can have multiple flame workers on different ports, show all
    FLAME_WORKER_LOGS=$(ls -t /home/ec2-user/logs/flame-worker-*.log 2>/dev/null | head -1)
    if [ -n "$FLAME_WORKER_LOGS" ]; then
        for log in $FLAME_WORKER_LOGS; do
            echo "--- $(basename $log) ---"
            tail -1000 "$log"
            echo ""
        done
    elif [ -f /tmp/flame-worker.log ]; then
        tail -1000 /tmp/flame-worker.log
    else
        echo "Log file not found: /home/ec2-user/logs/flame-worker-*.log or /tmp/flame-worker.log"
    fi
    echo ""
    
    # echo "=== Crawler Log ==="
    # PROJECT_DIR="/home/ec2-user/Fa25-CIS5550-Project-Hashira-main"
    # if [ -f "$PROJECT_DIR/logs/crawler.log" ]; then
    #     tail -1000 "$PROJECT_DIR/logs/crawler.log"
    # elif [ -f "logs/crawler.log" ]; then
    #     tail -1000 logs/crawler.log
    # else
    #     echo "Crawler log not found"
    # fi
    # echo ""
    
    echo "=== Recent System Logs ==="
    sudo journalctl -n 50 --no-pager 2>/dev/null || echo "Cannot access system logs"
    echo ""
    
    echo "=== Disk Usage ==="
    df -h
    echo ""
    
    echo "=== Memory Usage ==="
    free -h || vmstat
ENDSSH

    # Also try to copy log files directly if they exist
    echo "  Copying log files..."
    
    # Copy timestamped logs from new location (/home/ec2-user/logs/)
    # Try to copy the most recent log files matching our patterns
    for log_pattern in "kvs-coordinator-*.log" "kvs-worker-*.log" "flame-coordinator-*.log" "flame-worker-*.log"; do
        # Get the most recent matching log file
        log_file=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            ec2-user@"$ip" "ls -t /home/ec2-user/logs/$log_pattern 2>/dev/null | head -1" 2>/dev/null || echo "")
        if [ -n "$log_file" ]; then
            log_name=$(basename "$log_file")
            scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
                "ec2-user@$ip:$log_file" "$instance_dir/$log_name" 2>/dev/null || true
        fi
    done
    
    # Also try old location as fallback (/tmp/)
    for log_file in "/tmp/kvs-coordinator.log" "/tmp/kvs-worker.log" "/tmp/flame-coordinator.log" "/tmp/flame-worker.log"; do
        log_name=$(basename "$log_file")
        scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "ec2-user@$ip:$log_file" "$instance_dir/$log_name" 2>/dev/null || true
    done
    
    # Try to copy crawler log
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "ec2-user@$ip:/home/ec2-user/Fa25-CIS5550-Project-Hashira-main/logs/crawler.log" \
        "$instance_dir/crawler.log" 2>/dev/null || \
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "ec2-user@$ip:logs/crawler.log" \
        "$instance_dir/crawler.log" 2>/dev/null || true
    
    if [ -f "$instance_dir/logs.txt" ] && [ -s "$instance_dir/logs.txt" ]; then
        echo "  ✓ Successfully fetched logs from $instance_name"
    else
        echo "  ⚠ No logs retrieved from $instance_name"
    fi
}

# Fetch logs from coordinators
if [ -n "$KVS_COORD_IP" ] && [ "$KVS_COORD_IP" != "null" ]; then
    fetch_logs "kvs-coordinator" "$KVS_COORD_IP"
fi

if [ -n "$FLAME_COORD_IP" ] && [ "$FLAME_COORD_IP" != "null" ]; then
    fetch_logs "flame-coordinator" "$FLAME_COORD_IP"
fi

# Fetch logs from workers
if [ -n "$KVS_WORKER_IPS" ]; then
    worker_index=0
    # Handle both space-separated and newline-separated IPs
    for worker_ip in $KVS_WORKER_IPS; do
        if [ -n "$worker_ip" ] && [ "$worker_ip" != "null" ]; then
            echo "Processing worker-$worker_index at $worker_ip..."
            fetch_logs "worker-$worker_index" "$worker_ip"
            worker_index=$((worker_index + 1))
        fi
    done
else
    # Try to get worker IPs from terraform state directly
    cd terraform
    worker_count=$(terraform output -raw worker_count 2>/dev/null || echo "0")
    cd "$PROJECT_ROOT"
    
    if [ "$worker_count" -gt 0 ]; then
        echo "Attempting to get worker IPs from terraform outputs..."
        cd terraform
        for i in $(seq 0 $((worker_count - 1))); do
            if command -v jq &> /dev/null; then
                worker_ip=$(terraform output -json kvs_worker_public_ips 2>/dev/null | jq -r ".[$i]" 2>/dev/null || echo "")
            else
                # Parse from terraform output without jq
                all_ips=$(terraform output kvs_worker_public_ips 2>/dev/null || echo "")
                worker_ip=$(echo "$all_ips" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sed -n "$((i+1))p" || echo "")
            fi
            if [ -n "$worker_ip" ] && [ "$worker_ip" != "null" ]; then
                cd "$PROJECT_ROOT"
                fetch_logs "worker-$i" "$worker_ip"
                cd terraform
            fi
        done
        cd "$PROJECT_ROOT"
    fi
fi

# Create summary
echo ""
echo "==================================="
echo "Log Fetch Summary"
echo "==================================="
echo "Logs saved to: $SESSION_DIR"
echo ""
echo "Directory structure:"
find "$SESSION_DIR" -type f -name "*.log" -o -name "logs.txt" | head -20
echo ""
echo "Total files: $(find "$SESSION_DIR" -type f | wc -l)"
echo ""
echo "To view logs:"
echo "  cat $SESSION_DIR/kvs-coordinator/logs.txt"
echo "  tail -f $SESSION_DIR/worker-0/crawler.log"
echo ""
echo "✓ Done!"

