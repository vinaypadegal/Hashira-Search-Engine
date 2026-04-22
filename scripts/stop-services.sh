#!/bin/bash
# Stop KVS and Flame Services
# Usage: ./stop-services.sh

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "Stopping KVS and Flame Services"
echo "==================================="
echo ""

# Kill tmux session
if tmux has-session -t services 2>/dev/null; then
    echo "Stopping tmux session 'services'..."
    tmux kill-session -t services
    echo "✓ Services stopped"
else
    echo "No services session found (may already be stopped)"
fi

echo ""
echo "All services have been stopped."

# Archive logs into a timestamped local run folder *inside* logs/
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_DIR="logs/localrun_${TIMESTAMP}"

echo "Archiving local run logs to ./$RUN_DIR ..."
mkdir -p "$RUN_DIR"

if [ -d logs ] && [ "$(ls -A logs 2>/dev/null)" ]; then
    # Move only *.log files from logs/ into logs/localrun_timestamp/, keep folders and other files
    moved_any=false
    shopt -s nullglob 2>/dev/null || true
    for entry in logs/*.log; do
        if [ -f "$entry" ]; then
            mv "$entry" "$RUN_DIR"/
            moved_any=true
        fi
    done
    shopt -u nullglob 2>/dev/null || true

    if [ "$moved_any" = true ]; then
        echo "  Moved *.log files from logs/ -> $RUN_DIR/"
    else
        echo "  No .log files to move from logs/ (only subdirectories or non-log files present)"
    fi
else
    echo "  No logs directory found or it is empty"
fi

echo "Logs archived. New logs will continue to be written to ./logs/ on next run."
