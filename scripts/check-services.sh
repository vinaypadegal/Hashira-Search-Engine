#!/bin/bash
# Check Status of KVS and Flame Services
# Usage: ./check-services.sh

set -e

# Get the project root directory (parent of scripts/)
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==================================="
echo "Service Status Check"
echo "==================================="
echo ""

# Check tmux session
if tmux has-session -t services 2>/dev/null; then
    echo "✓ Tmux session 'services' is running"
    echo ""
    echo "Windows:"
    tmux list-windows -t services -F "  - #{window_name}"
else
    echo "✗ Tmux session 'services' is not running"
    echo "  Start services with: ./start-services.sh"
    exit 1
fi

echo ""
echo "Port Status:"
if lsof -i :8000 > /dev/null 2>&1; then
    echo "  ✓ Port 8000 (KVS Coordinator) - LISTENING"
else
    echo "  ✗ Port 8000 (KVS Coordinator) - NOT LISTENING"
fi

if lsof -i :8001 > /dev/null 2>&1; then
    echo "  ✓ Port 8001 (KVS Worker) - LISTENING"
else
    echo "  ✗ Port 8001 (KVS Worker) - NOT LISTENING"
fi

if lsof -i :9000 > /dev/null 2>&1; then
    echo "  ✓ Port 9000 (Flame Coordinator) - LISTENING"
else
    echo "  ✗ Port 9000 (Flame Coordinator) - NOT LISTENING"
fi

if lsof -i :9001 > /dev/null 2>&1; then
    echo "  ✓ Port 9001 (Flame Worker 1) - LISTENING"
else
    echo "  ✗ Port 9001 (Flame Worker 1) - NOT LISTENING"
fi

if lsof -i :9002 > /dev/null 2>&1; then
    echo "  ✓ Port 9002 (Flame Worker 2) - LISTENING"
else
    echo "  ✗ Port 9002 (Flame Worker 2) - NOT LISTENING"
fi

echo ""
echo "HTTP Responses:"
if curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "  ✓ KVS Coordinator responding"
else
    echo "  ✗ KVS Coordinator not responding"
fi

if curl -s http://localhost:9000/ > /dev/null 2>&1; then
    echo "  ✓ Flame Coordinator responding"
    workers=$(curl -s "http://localhost:9000/workers" 2>/dev/null | head -1)
    if [ -n "$workers" ] && [ "$workers" != "0" ]; then
        echo "  ✓ $workers Flame workers registered"
    else
        echo "  ⚠ No Flame workers registered"
    fi
else
    echo "  ✗ Flame Coordinator not responding"
fi

echo ""
echo "To view services: tmux attach -t services"
echo "To stop services: ./stop-services.sh"

