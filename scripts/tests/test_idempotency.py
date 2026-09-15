#!/usr/bin/env python3
import requests
import time
import subprocess
import uuid

BASE_URL = "http://localhost:8080"

def main():
    print("================================================================")
    print("  PHASE 5.1: AT-LEAST-ONCE DELIVERY & IDEMPOTENCY DEFENSE TEST")
    print("================================================================")

    # 1. Place a reservation
    order_res = requests.post(f"{BASE_URL}/api/v1/orders/fast?ttl=60", json={"product_id": 1, "user_id": "usr_idempotent"})
    assert order_res.status_code == 202
    res_data = order_res.json()
    reservation_id = res_data["reservation_id"]
    print(f"[RESERVED] Created reservation: {reservation_id}")

    # Wait for consumer to process once
    time.sleep(0.5)

    # Verify order is in DB
    cnt_before = int(subprocess.check_output([
        "psql", "-d", "flashsale", "-t", "-c",
        f"SELECT count(*) FROM orders WHERE reservation_id = '{reservation_id}';"
    ]).decode().strip())
    print(f"[DB STATE] Orders in PostgreSQL with reservation_id: {cnt_before}")
    assert cnt_before == 1

    # Check Tier 1 idempotency key in Redis
    idem_val = subprocess.check_output(["redis-cli", "get", f"idempotency:{reservation_id}"]).decode().strip()
    print(f"[REDIS STATE] idempotency:{reservation_id} = '{idem_val}'")
    assert idem_val in ["PROCESSING", "COMPLETED"]

    # 2. Simulate 5 DUPLICATE message deliveries into Redis Stream
    print("\n[SIMULATING RETRIES] Injecting 5 duplicate messages with identical reservation_id into Redis Stream...")
    for i in range(5):
        subprocess.check_output([
            "redis-cli", "xadd", "orders:stream", "*",
            "reservation_id", reservation_id,
            "product_id", "1",
            "user_id", "usr_idempotent",
            "created_at", time.strftime("%Y-%m-%dT%H:%M:%SZ")
        ])

    # Allow consumer worker to poll and encounter the duplicates
    time.sleep(1.0)

    # 3. Verify PostgreSQL orders table STILL has exactly 1 row (zero duplicate inserts)
    cnt_after = int(subprocess.check_output([
        "psql", "-d", "flashsale", "-t", "-c",
        f"SELECT count(*) FROM orders WHERE reservation_id = '{reservation_id}';"
    ]).decode().strip())
    print(f"[DB STATE AFTER 5 DUPLICATES] Orders in PostgreSQL: {cnt_after}")
    assert cnt_after == 1, f"Idempotency defense failed! Expected 1, found {cnt_after}"

    print("\n>>> PASS: Both Tier 1 (Redis SETNX) and Tier 2 (Postgres UNIQUE constraint) completely prevented duplicate insertion!")
    print("================================================================")

if __name__ == "__main__":
    main()
