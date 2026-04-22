#!/bin/bash
# Start KVS and Flame Services
# Usage: ./start-services.sh [kvsWorkers] [flameWorkers]
# Example: ./start-services.sh 2 3  (2 KVS workers, 3 Flame workers)

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Parse arguments with defaults
kvsWorkers=${1:-1}
flameWorkers=${2:-2}

echo "==================================="
echo "Starting KVS and Flame Services"
echo "==================================="
echo "KVS Workers: $kvsWorkers"
echo "Flame Workers: $flameWorkers"
echo

# Create bin, logs, and jars directories if they don't exist
mkdir -p bin
mkdir -p logs
mkdir -p jars

# JAR paths for compilation
JSOUP_JAR="$PROJECT_ROOT/libs/jsoup-1.21.2.jar"
SNOWBALL_JAR="$PROJECT_ROOT/libs/libstemmer_java-3.0.1.jar"

# Compile all source files to bin directory (with jsoup and snowball in classpath)
echo "Compiling source files to bin/ (with jsoup and snowball)..."
javac -cp "$JSOUP_JAR:$SNOWBALL_JAR" -d bin \
  src/cis5550/generic/*.java \
  src/cis5550/kvs/*.java \
  src/cis5550/flame/*.java \
  src/cis5550/webserver/*.java \
  src/cis5550/tools/*.java

# Package IndexerJsoup jar
# echo "Packaging IndexerJsoup.jar..."
# jar cf jars/IndexerJsoup.jar -C bin cis5550

# Create worker directories
for ((i=1; i<=kvsWorkers; i++)); do
    mkdir -p "worker$i"
done

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install it with: sudo apt-get install tmux"
    exit 1
fi

echo
echo "Starting tmux session with $((2 + kvsWorkers + flameWorkers)) windows..."
echo "Using tmux windows (not panes) for better organization"
echo

tmux kill-session -t services 2>/dev/null || true

# Create session with first window for KVS Coordinator
tmux new-session -d -s services -n "KVS-Coord"
tmux send-keys -t services:KVS-Coord "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Coordinator 8000 >> logs/kvs-coordinator.log 2>&1" C-m

# Create windows for KVS Workers
for ((i=1; i<=kvsWorkers; i++)); do
    port=$((8000 + i))
    tmux new-window -t services -n "KVS-W$i"
    tmux send-keys -t services:KVS-W$i "cd '$PROJECT_ROOT' && java -cp bin cis5550.kvs.Worker $port worker$i localhost:8000 >> logs/kvs-worker-$i.log 2>&1" C-m
    sleep 0.5
done

# Create window for Flame Coordinator (needs jsoup + snowball for IndexerJsoup)
tmux new-window -t services -n "Flame-Coord"
tmux send-keys -t services:Flame-Coord "cd '$PROJECT_ROOT' && java -cp 'bin:$JSOUP_JAR:$SNOWBALL_JAR' cis5550.flame.Coordinator 9000 localhost:8000 >> logs/flame-coordinator.log 2>&1" C-m
sleep 1

# Create windows for Flame Workers (needs jsoup + snowball for IndexerJsoup)
for ((i=1; i<=flameWorkers; i++)); do
    port=$((9000 + i))
    tmux new-window -t services -n "Flame-W$i"
    tmux send-keys -t services:Flame-W$i "cd '$PROJECT_ROOT' && java -cp 'bin:$JSOUP_JAR:$SNOWBALL_JAR' cis5550.flame.Worker $port localhost:9000 >> logs/flame-worker-$i.log 2>&1" C-m
    sleep 0.5
done

echo
echo "All components started in tmux session 'services'"
echo
echo "Services running:"
echo "  - KVS Coordinator (port 8000)"
for ((i=1; i<=kvsWorkers; i++)); do
    echo "  - KVS Worker $i (port $((8000 + i)))"
done
echo "  - Flame Coordinator (port 9000)"
for ((i=1; i<=flameWorkers; i++)); do
    echo "  - Flame Worker $i (port $((9000 + i)))"
done
echo
echo "Logs are being written to:"
echo "  - logs/kvs-coordinator.log"
for ((i=1; i<=kvsWorkers; i++)); do
    echo "  - logs/kvs-worker-$i.log"
done
echo "  - logs/flame-coordinator.log"
for ((i=1; i<=flameWorkers; i++)); do
    echo "  - logs/flame-worker-$i.log"
done
echo
echo "Tmux commands:"
echo "  - View: tmux attach -t services"
echo "  - Switch windows: Ctrl+B then window number (0-$((1 + kvsWorkers + flameWorkers)))"
echo "  - List windows: Ctrl+B then w"
echo "  - Next window: Ctrl+B then n"
echo "  - Previous window: Ctrl+B then p"
echo "  - Detach: Ctrl+B then D"
echo "  - Kill all: tmux kill-session -t services"
echo
