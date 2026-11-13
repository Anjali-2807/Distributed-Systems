#!/bin/bash
# Stop all cluster nodes

cd "$(dirname "$0")/.."

for pidfile in node*.pid; do
    if [ -f "$pidfile" ]; then
        PID=$(cat "$pidfile")
        echo "Killing process $PID ($pidfile)..."
        kill -9 "$PID" 2>/dev/null || true
        rm -f "$pidfile"
    fi
done

pkill -f "python/server.py" 2>/dev/null || true
echo "All cluster nodes stopped."
