#!/bin/bash
# HW5 Test Script - Full KVS Distributed System
# Uses tmux to run coordinator, worker, and tests in separate panes

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW5 Test - Full KVS System"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

echo
echo "HW5Test will create __worker directory with test data."
echo "Starting tmux session with Coordinator, Worker, and Test..."
echo
echo "Layout:"
echo "  - Top left: Coordinator (port 8000)"
echo "  - Top right: Worker (port 8001)"
echo "  - Bottom: Test runner"
echo
echo "Press Ctrl+B then D to detach from tmux session"
echo "Use 'tmux attach -t hw5-test' to reattach"
echo "Use 'tmux kill-session -t hw5-test' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw5-test 2>/dev/null || true

# Create new tmux session with 3 panes
tmux new-session -d -s hw5-test -n hw5

# Split window horizontally for coordinator and worker
tmux split-window -h -t hw5-test

# Split bottom for test
tmux split-window -v -t hw5-test

# Run test in bottom pane (pane 2) first
tmux send-keys -t hw5-test:hw5.2 "cd '$PROJECT_ROOT' && echo 'Running HW5Test...' && java -cp bin cis5550.test.HW5Test all && echo && echo 'Test completed! Press Ctrl+C in other panes to stop services.' && bash" C-m

# Wait for test to prompt for services
sleep 3

# Run Coordinator in top-left pane (pane 0)
tmux send-keys -t hw5-test:hw5.0 "cd '$PROJECT_ROOT' && echo 'Starting Coordinator on port 8000...' && java -cp bin cis5550.kvs.Coordinator 8000" C-m

# Wait a moment
sleep 1

# Run Worker in top-right pane (pane 1)
tmux send-keys -t hw5-test:hw5.1 "cd '$PROJECT_ROOT' && echo 'Starting Worker on port 8001...' && echo 'Worker will load test data from __worker directory' && java -cp bin cis5550.kvs.Worker 8001 __worker localhost:8000" C-m

# Attach to the session
tmux attach -t hw5-test
