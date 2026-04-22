#!/bin/bash
# HW4 Worker Test Script
# Tests worker functionality (registration, heartbeat, persistence, etc.)

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW4 Worker Test"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

# Create worker directory
mkdir -p __worker

echo
echo "Starting tmux session..."
echo "Press Ctrl+B then D to detach from tmux"
echo "Use 'tmux kill-session -t hw4-worker' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw4-worker 2>/dev/null || true

# Create tmux session with 3 panes (test on bottom, coordinator and worker on top)
tmux new-session -d -s hw4-worker -n worker
tmux split-window -h -t hw4-worker
tmux split-window -v -t hw4-worker

# Start WorkerTest in bottom pane
tmux send-keys -t hw4-worker:worker.2 "cd '$PROJECT_ROOT' && java -cp bin cis5550.test.HW4WorkerTest all && echo && echo 'Test completed!' && bash" C-m

# Wait for test to show prompt
sleep 3

# Start Coordinator in top-left pane
tmux send-keys -t hw4-worker:worker.0 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000" C-m

# Wait a moment
sleep 1

# Start Worker in top-right pane
tmux send-keys -t hw4-worker:worker.1 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Worker 8001 __worker localhost:8000" C-m

# Attach to session
tmux attach -t hw4-worker
