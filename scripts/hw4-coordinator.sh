#!/bin/bash
# HW4 Coordinator Test Script
# Tests coordinator functionality (worker registration, heartbeat, etc.)

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "HW4 Coordinator Test"
echo "==================================="
echo

# Create bin directory if it doesn't exist
mkdir -p bin

# Compile all source files to bin directory
echo "Compiling source files to bin/..."
javac -d bin src/cis5550/**/*.java

echo
echo "Starting tmux session..."
echo "Press Ctrl+B then D to detach from tmux"
echo "Use 'tmux kill-session -t hw4-coordinator' to stop"
echo

# Kill existing session if it exists
tmux kill-session -t hw4-coordinator 2>/dev/null || true

# Create tmux session with 2 panes (test on left, coordinator on right)
tmux new-session -d -s hw4-coordinator -n coordinator
tmux split-window -h -t hw4-coordinator

# Start CoordinatorTest in left pane
tmux send-keys -t hw4-coordinator:coordinator.0 "cd '$PROJECT_ROOT' && java -cp bin cis5550.test.HW4CoordinatorTest all && echo && echo 'Test completed!' && bash" C-m

# Wait for test to show prompt
sleep 3

# Start Coordinator in right pane
tmux send-keys -t hw4-coordinator:coordinator.1 "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000" C-m

# Attach to session
tmux attach -t hw4-coordinator
