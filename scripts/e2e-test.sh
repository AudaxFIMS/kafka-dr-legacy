#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${REST_URL:-http://localhost:8088}"
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

# ─── Wait for app ────────────────────────────────────────────────
info "Waiting for REST server at ${BASE_URL}..."
for i in $(seq 1 30); do
    if curl -s "${BASE_URL}/status" > /dev/null 2>&1; then
        ok "REST server is up"
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "REST server not available after 30s"
        exit 1
    fi
    sleep 1
done

# ─── 1. Check status ────────────────────────────────────────────
info "=== 1. Checking cluster status ==="
STATUS=$(curl -s "${BASE_URL}/status")
echo "$STATUS" | python3 -m json.tool 2>/dev/null || echo "$STATUS"
ACTIVE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('activeCluster',{}).get('name','none'))" 2>/dev/null || echo "unknown")
if [ "$ACTIVE" != "none" ] && [ "$ACTIVE" != "null" ]; then
    ok "Active cluster: ${ACTIVE}"
else
    warn "No active cluster yet — health checks may still be running"
fi
echo

# ─── 2. Produce string message (demo-events) ────────────────────
info "=== 2. Sending STRING message to demo-events ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/demo-events" -d "Hello from DR test!")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "String message sent"
echo

# ─── 3. Produce JSON message (order-events) ─────────────────────
info "=== 3. Sending JSON message to order-events ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/order-events" \
    -H "Content-Type: application/json" \
    -d '{"orderId":"ORD-E2E-001","items":5,"total":149.99,"timestamp":1713000000000}')
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "JSON message sent"
echo

# ─── 4. Produce JSON default (order-events, empty body) ─────────
info "=== 4. Sending default JSON order (empty body) ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/order-events")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "Default JSON order sent"
echo

# ─── 5. Produce Avro message (payment-events) ───────────────────
info "=== 5. Sending AVRO PaymentEvent to payment-events ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/payment-events" \
    -H "Content-Type: application/json" \
    -d '{"paymentId":"PAY-E2E-001","orderId":"ORD-E2E-001","amount":49.99,"currency":"EUR","status":"COMPLETED","timestamp":1713000000000}')
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "Avro PaymentEvent sent"
echo

# ─── 6. Produce default Avro (payment-events, empty body) ───────
info "=== 6. Sending default AVRO PaymentEvent (empty body) ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/payment-events")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "Default Avro PaymentEvent sent"
echo

# ─── 7. Produce bytes (raw-telemetry) ───────────────────────────
info "=== 7. Sending BYTES to raw-telemetry ==="
RESULT=$(curl -s -X POST "${BASE_URL}/produce/raw-telemetry" -d "sensor-42:temp=36.6:ts=$(date +%s)")
echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
ok "Bytes message sent"
echo

# ─── 8. Check idempotency stats ─────────────────────────────────
info "=== 8. Checking idempotency store ==="
STATUS=$(curl -s "${BASE_URL}/status")
ENTRIES=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['idempotency']['entries'])" 2>/dev/null || echo "?")
ok "Idempotency store has ${ENTRIES} entries"
echo

# ─── 9. DR failover test ────────────────────────────────────────
info "=== 9. DR Failover test ==="
warn "To test failover, stop the primary kafka container:"
echo "    docker compose stop kafka-primary"
echo ""
echo "  Then watch the app logs and call /status to see the switch:"
echo "    curl ${BASE_URL}/status | python3 -m json.tool"
echo ""
echo "  To test failback, restart primary:"
echo "    docker compose start kafka-primary"
echo

ok "=== E2E test complete ==="
