#!/bin/bash
# Start 3-node distributed cluster in background

cd "$(dirname "$0")/.."

echo "Starting Node 0 on 127.0.0.1:8001..."
python3 python/server.py --id 0 --port 8001 --peers "1:127.0.0.1:8002,2:127.0.0.1:8003" > node0.log 2>&1 &
echo $! > node0.pid

echo "Starting Node 1 on 127.0.0.1:8002..."
python3 python/server.py --id 1 --port 8002 --peers "0:127.0.0.1:8001,2:127.0.0.1:8003" > node1.log 2>&1 &
echo $! > node1.pid

echo "Starting Node 2 on 127.0.0.1:8003..."
python3 python/server.py --id 2 --port 8003 --peers "0:127.0.0.1:8001,1:127.0.0.1:8002" > node2.log 2>&1 &
echo $! > node2.pid

echo "Cluster started successfully. Check node logs or run 'python3 python/client.py status'."
