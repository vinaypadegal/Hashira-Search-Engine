#!/bin/bash
# HW6 Flame Test Script
# Starts KVS Coordinator, KVS Worker, Flame Coordinator, and 2 Flame Workers

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW6 Flame System"
echo "==================================="
echo

kvsWorkers=1
flameWorkers=2

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

# Create worker directory
mkdir -p worker1

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with: sudo apt-get install tmux"
    exit 1
fi

echo
echo "Starting tmux session with 6 panes (5 services + 1 test)..."
echo "Press Ctrl+B then D to detach from tmux"
echo "Use 'tmux kill-session -t flame' to stop"
echo

tmux kill-session -t flame 2>/dev/null || true

tmux new-session -d -s flame -n "Flame"

tmux split-window -h -t flame
tmux split-window -v -t flame:0.0
tmux split-window -v -t flame:0.2
tmux select-pane -t flame:0.0
tmux split-window -v -t flame:0.0
tmux select-pane -t flame:0.4
tmux split-window -v -t flame:0.4

# Start test in bottom pane first (it will wait for Enter)
tmux send-keys -t flame:0.5 "cd '$PROJECT_ROOT' && echo 'Running HW6Test...' && java -cp bin cis5550.test.HW6Test all && echo && echo 'Test completed!' && bash" C-m

# Wait for test to show prompt
sleep 3

# Start all services
tmux send-keys -t flame:0.0 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000" C-m
sleep 1
tmux send-keys -t flame:0.1 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Worker 8001 worker1 localhost:8000" C-m
sleep 1
tmux send-keys -t flame:0.2 "cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.Coordinator 9000 localhost:8000" C-m
sleep 1
tmux send-keys -t flame:0.3 "cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.Worker 9001 localhost:9000" C-m
sleep 1
tmux send-keys -t flame:0.4 "cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.Worker 9002 localhost:9000" C-m

echo
echo "All components started in tmux session 'flame'"
echo "Layout:"
echo "  - Pane 0 (top-left): KVS Coordinator (port 8000)"
echo "  - Pane 1 (middle-left): KVS Worker (port 8001)"
echo "  - Pane 2 (top-right): Flame Coordinator (port 9000)"
echo "  - Pane 3 (middle-right): Flame Worker 1 (port 9001)"
echo "  - Pane 4 (bottom-middle): Flame Worker 2 (port 9002)"
echo "  - Pane 5 (bottom): HW6 Test"
echo
echo "When tmux opens, press Enter in the bottom test pane to start tests"
echo
echo "To detach: Press Ctrl+B then D"
echo "To reattach: tmux attach -t flame"
echo "To kill all: tmux kill-session -t flame"
echo

tmux attach -t flame
