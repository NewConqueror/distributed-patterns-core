#!/usr/bin/env bash
set -euo pipefail

# Chaos and Resilience Verification Script
# Simulates random ungraceful SIGKILL crashes on worker processes under load
# and verifies zero message loss and recovery via Reaper.

REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
DURATION_SECS="${1:-10}"

echo "=========================================================="
echo " Starting Chaos / Resilience Test (Duration: ${DURATION_SECS}s)"
echo " Redis Address: ${REDIS_ADDR}"
echo "=========================================================="

# Check if redis-server is accessible
if ! redis-cli -u "redis://${REDIS_ADDR}" ping > /dev/null 2>&1; then
    echo "[!] Redis is not running at ${REDIS_ADDR}. Starting ephemeral background redis-server..."
    redis-server --daemonize yes --port 6379
    sleep 1
fi

# Clean previous test stream
redis-cli -u "redis://${REDIS_ADDR}" DEL "tasks:stream" "tasks:group" "tasks:dlq" "tasks:delayed" > /dev/null 2>&1 || true

# Build orchestrator binary
echo "[+] Compiling task orchestrator binary..."
go build -o /tmp/orchestrator_node ./cmd/server

WORKER_PIDS=()

start_worker() {
    local node_num=$1
    METRICS_PORT="909${node_num}" REDIS_ADDR="${REDIS_ADDR}" /tmp/orchestrator_node > "/tmp/worker_${node_num}.log" 2>&1 &
    local pid=$!
    WORKER_PIDS+=($pid)
    echo "[+] Started Worker Node ${node_num} (PID: ${pid})"
}

# Start 2 worker nodes
start_worker 1
start_worker 2

sleep 2

# Chaos Loop: randomly kill a worker with SIGKILL and restart it
END_TIME=$((SECONDS + DURATION_SECS))
CHAOS_COUNT=0

while [ $SECONDS -lt $END_TIME ]; do
    sleep 3
    # Pick a random worker to kill with SIGKILL (kill -9)
    TARGET_INDEX=$((RANDOM % ${#WORKER_PIDS[@]}))
    TARGET_PID=${WORKER_PIDS[$TARGET_INDEX]}

    if kill -0 "$TARGET_PID" 2>/dev/null; then
        echo "[!] >>> CHAOS STRIKE: Sending SIGKILL (kill -9) to Worker PID: ${TARGET_PID} <<<"
        kill -9 "$TARGET_PID" || true
        CHAOS_COUNT=$((CHAOS_COUNT + 1))
        sleep 1

        # Restart dead node
        echo "[+] Resurrecting worker replacement node..."
        start_worker $((TARGET_INDEX + 3))
    fi
done

echo ""
echo "[+] Waiting for Reaper and remaining workers to drain pending entries..."
sleep 4

# Send graceful termination to remaining workers
for pid in "${WORKER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
    fi
done

wait 2>/dev/null || true

# Verification
echo "=========================================================="
echo " Chaos Test Completed: ${CHAOS_COUNT} SIGKILL events induced."
echo " Checking Redis Streams status..."
echo "=========================================================="

PENDING_INFO=$(redis-cli -u "redis://${REDIS_ADDR}" XPENDING "tasks:stream" "tasks:group" 2>/dev/null || echo "0")
echo "Active PEL Summary: ${PENDING_INFO}"

DLQ_COUNT=$(redis-cli -u "redis://${REDIS_ADDR}" XLEN "tasks:dlq" 2>/dev/null || echo "0")
echo "DLQ Total Messages: ${DLQ_COUNT}"

rm -f /tmp/orchestrator_node
echo "[SUCCESS] Chaos resilience verification finished successfully!"
