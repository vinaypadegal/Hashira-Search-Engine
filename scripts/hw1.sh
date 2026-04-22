#!/bin/bash
# HW1 Test Script - HTTP Server Basics
# Uses tmux to run test server and client in separate panes

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW1 Test - HTTP Server Basics"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

# Create test directory if needed
mkdir -p test

echo
echo "Starting tmux session with HW1TestServer and HW1Test..."
echo "Press Ctrl+B then D to detach from tmux session"
echo "Use 'tmux attach -t hw1-test' to reattach"
echo "Use 'tmux kill-session -t hw1-test' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw1-test 2>/dev/null || true

# Create new tmux session
tmux new-session -d -s hw1-test -n hw1

# Split window horizontally
tmux split-window -h -t hw1-test

# Run HW1TestServer in left pane
tmux send-keys -t hw1-test:hw1.0 "cd '$PROJECT_ROOT' && echo 'Starting HW1TestServer on port 8000...' && java -cp bin cis5550.test.HW1TestServer 8000 test" C-m

# Wait for server to start
sleep 2

# Run HW1Test in right pane
tmux send-keys -t hw1-test:hw1.1 "cd '$PROJECT_ROOT' && echo 'Running HW1Test...' && echo 'Press Enter to start tests...' && read && java -cp bin cis5550.test.HW1Test all && echo && echo 'Tests completed! Press Ctrl+C in server pane to stop.' && bash" C-m

# Attach to the session
tmux attach -t hw1-test
