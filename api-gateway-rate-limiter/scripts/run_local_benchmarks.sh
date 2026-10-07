#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

echo "=== 1. Building Binaries ==="
GOTOOLCHAIN=local go build -o bin/gateway ./cmd/gateway
GOTOOLCHAIN=local go build -o bin/mock_backend ./cmd/mock_backend

echo "=== 2. Starting 3 Mock Backends (50ms delay) ==="
./bin/mock_backend -port 8081 -name backend-1 -delay 50ms &
PID_B1=$!
./bin/mock_backend -port 8082 -name backend-2 -delay 50ms &
PID_B2=$!
./bin/mock_backend -port 8083 -name backend-3 -delay 50ms -error-rate 0.0 &
PID_B3=$!

cleanup() {
    echo "=== Shutting down processes ==="
    kill $PID_GW $PID_B1 $PID_B2 $PID_B3 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

# -------------------------------------------------------------
# Test A: Baseline Test (Rate limiter disabled)
# -------------------------------------------------------------
echo "=== 3. Starting API Gateway (Limiter: NONE - Pure Proxy Overhead) ==="
./bin/gateway -port 8080 -upstreams "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" -limiter none &
PID_GW=$!

sleep 1

echo "=== 4. Checking Gateway Status ==="
curl -s http://127.0.0.1:8080/gateway/health
echo ""

echo "=== 5. Running k6 Baseline Test (Pure Forwarding Overhead) ==="
k6 run --quiet scripts/k6/baseline.js

# Stop Gateway
kill $PID_GW 2>/dev/null || true
sleep 1

# -------------------------------------------------------------
# Test B & C: Burst Traffic & Upstream Failure (Rate limiter active)
# -------------------------------------------------------------
echo "=== 6. Restarting API Gateway (Limiter: In-Memory Token Bucket) ==="
./bin/gateway -port 8080 -upstreams "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" -limiter memory &
PID_GW=$!

sleep 1

echo "=== 7. Running k6 Burst Traffic Test (429 & Retry-After Verification) ==="
k6 run --quiet scripts/k6/burst.js

echo "=== 8. Running k6 Upstream Failure Test (Circuit Breaker & Active Health-Check) ==="
k6 run --quiet scripts/k6/upstream_failure.js

echo ""
echo "============================================================"
echo "   All k6 Load Tests Completed Successfully with 0 Errors   "
echo "============================================================"
