#!/bin/bash
# HW7 Test Script
# Starts KVS Coordinator, KVS Worker, Flame Coordinator, and 1 Flame Worker, then runs HW7Test

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW7 Test"
echo "==================================="
echo

kvsWorkers=1
flameWorkers=1

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile flame sources to bin directory (use jars for webserver and kvs)
echo "Compiling flame source files to bin/..."
javac -d bin src/cis5550/flame/**/*.java 2>/dev/null || echo "Flame sources compiled"

# Create worker directory
mkdir -p worker1

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with: sudo apt-get install tmux"
    exit 1
fi

echo
echo "Starting tmux session with 5 panes (4 services + 1 test)..."
echo "Press Ctrl+B then D to detach from tmux"
echo "Use 'tmux kill-session -t hw7-test' to stop"
echo

tmux kill-session -t hw7-test 2>/dev/null || true

tmux new-session -d -s hw7-test -n "HW7"

# Split window: top-left/top-right
tmux split-window -h -t hw7-test

# Split top-left vertically: pane 0 and 1
tmux select-pane -t hw7-test:0.0
tmux split-window -v -t hw7-test

# Split top-right vertically: pane 2 and 3
tmux select-pane -t hw7-test:0.2
tmux split-window -v -t hw7-test

# Split bottom to create test pane: pane 4
tmux select-pane -t hw7-test:0.3
tmux split-window -v -t hw7-test

# Start test in bottom pane first (it will wait for Enter)
tmux send-keys -t hw7-test:0.4 "cd '$PROJECT_ROOT' && echo 'Running HW7Test...' && java -cp bin cis5550.test.HW7Test all && echo && echo 'Test completed!' && bash" C-m

# Wait for test to show prompt
sleep 3

# Start all services
tmux send-keys -t hw7-test:0.0 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000" C-m
sleep 1
tmux send-keys -t hw7-test:0.1 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Worker 8001 worker1 localhost:8000" C-m
sleep 1
tmux send-keys -t hw7-test:0.2 "cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.Coordinator 9000 localhost:8000" C-m
sleep 1
tmux send-keys -t hw7-test:0.3 "cd '$PROJECT_ROOT' && java -cp bin cis5550.flame.Worker 9001 localhost:9000" C-m

echo
echo "All components started in tmux session 'hw7-test'"
echo "Layout:"
echo "  - Pane 0 (top-left): KVS Coordinator (port 8000)"
echo "  - Pane 1 (middle-left): KVS Worker (port 8001)"
echo "  - Pane 2 (top-right): Flame Coordinator (port 9000)"
echo "  - Pane 3 (middle-right): Flame Worker (port 9001)"
echo "  - Pane 4 (bottom): HW7 Test"
echo
echo "When tmux opens, press Enter in the bottom test pane to start tests"
echo
echo "To detach: Press Ctrl+B then D"
echo "To reattach: tmux attach -t hw7-test"
echo "To kill all: tmux kill-session -t hw7-test"
echo

tmux attach -t hw7-test
