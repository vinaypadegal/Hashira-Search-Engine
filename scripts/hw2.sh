#!/bin/bash
# HW2 Test Script - Multi-host Routing
# Uses tmux to run test server and client in separate panes

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW2 Test - Multi-host Routing"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

# Create test directory if needed
mkdir -p __test123

echo
echo "Starting tmux session with HW2TestServer and HW2TestClient..."
echo "Press Ctrl+B then D to detach from tmux session"
echo "Use 'tmux attach -t hw2-test' to reattach"
echo "Use 'tmux kill-session -t hw2-test' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw2-test 2>/dev/null || true

# Create new tmux session
tmux new-session -d -s hw2-test -n hw2

# Split window horizontally
tmux split-window -h -t hw2-test

# Run HW2TestServer in left pane
tmux send-keys -t hw2-test:hw2.0 "cd '$PROJECT_ROOT' && echo 'Starting HW2TestServer...' && java -cp bin cis5550.test.HW2TestServer" C-m

# Wait for server to start
sleep 2

# Run HW2TestClient in right pane
tmux send-keys -t hw2-test:hw2.1 "cd '$PROJECT_ROOT' && echo 'Running HW2TestClient...' && java -cp bin cis5550.test.HW2TestClient all && echo && echo 'Tests completed! Press Ctrl+C in server pane to stop.' && bash" C-m

# Attach to the session
tmux attach -t hw2-test
