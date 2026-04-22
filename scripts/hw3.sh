#!/bin/bash
# HW3 Test Script - HTTPS/SNI and Sessions
# Uses tmux to run test server and client in separate panes

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW3 Test - HTTPS/SNI and Sessions"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

echo
echo "Starting tmux session with HW3TestServer and HW3TestClient..."
echo "Press Ctrl+B then D to detach from tmux session"
echo "Use 'tmux attach -t hw3-test' to reattach"
echo "Use 'tmux kill-session -t hw3-test' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw3-test 2>/dev/null || true

# Create new tmux session
tmux new-session -d -s hw3-test -n hw3

# Split window horizontally
tmux split-window -h -t hw3-test

# Run HW3TestServer in left pane
tmux send-keys -t hw3-test:hw3.0 "cd '$PROJECT_ROOT' && echo 'Starting HW3TestServer...' && java -cp bin cis5550.test.HW3TestServer" C-m

# Wait for server to start
sleep 2

# Run HW3TestClient in right pane
tmux send-keys -t hw3-test:hw3.1 "cd '$PROJECT_ROOT' && echo 'Running HW3TestClient...' && java -cp bin cis5550.test.HW3TestClient all && echo && echo 'Tests completed! Press Ctrl+C in server pane to stop.' && bash" C-m

# Attach to the session
tmux attach -t hw3-test
