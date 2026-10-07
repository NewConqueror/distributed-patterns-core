#!/usr/bin/env python3
import requests
import time
import subprocess
import sys

BASE_URL = "http://localhost:8080"

def get_redis_stock(product_id=1):
    return int(subprocess.check_output(["redis-cli", "get", f"product:{product_id}:stock"]).decode().strip())

def get_db_stock(product_id=1):
    out = subprocess.check_output(["psql", "-d", "flashsale", "-t", "-c", f"SELECT stock FROM products WHERE id = {product_id};"]).decode().strip()
    return int(out)

def get_db_order_status(reservation_id):
    out = subprocess.check_output(["psql", "-d", "flashsale", "-t", "-c", f"SELECT status FROM orders WHERE reservation_id = '{reservation_id}';"]).decode().strip()
    return out

def main():
    print("================================================================")
    print("  PHASE 5.2: TTL EXPIRATION & INVENTORY ROLLBACK TEST")
    print("================================================================")

    # 0. Reset product stock to 100
    res = requests.post(f"{BASE_URL}/api/v1/products/1/reset", json={"stock": 100})
    assert res.status_code == 200, f"Reset failed: {res.text}"
    print("[INIT] Product 1 reset to 100 stock in DB and Redis.")

    # 1. Place an order with a 3-second TTL
    print("\n--- TEST CASE 1: Unpaid Reservation Expiration & Rollback ---")
    order_res = requests.post(f"{BASE_URL}/api/v1/orders/fast?ttl=3", json={"product_id": 1, "user_id": "usr_ttl_expiring"})
    assert order_res.status_code == 202, f"Expected 202, got {order_res.status_code}"
    res_data = order_res.json()
    reservation_id = res_data["reservation_id"]
    print(f"[RESERVED] reservation_id={reservation_id}, status={res_data['status']}, ttl={res_data.get('ttl_seconds')}s")

    # Verify initial decrement
    time.sleep(0.3)
    assert get_redis_stock() == 99, "Redis stock must be 99"
    assert get_db_order_status(reservation_id) == "PENDING", "DB order status must be PENDING"
    print("[VERIFIED] Stock decremented to 99 in Redis. PostgreSQL order status is PENDING.")

    # Wait for TTL expiration (3s TTL -> wait 4s)
    print("[WAITING] Waiting 4.0s for Redis key expiration event...")
    time.sleep(4.0)

    # Verify rollback
    redis_after = get_redis_stock()
    db_after = get_db_stock()
    status_after = get_db_order_status(reservation_id)

    print(f"[AFTER EXPIRATION] Redis Stock: {redis_after} (expected 100)")
    print(f"[AFTER EXPIRATION] PostgreSQL Stock: {db_after} (expected 100)")
    print(f"[AFTER EXPIRATION] PostgreSQL Order Status: {status_after} (expected EXPIRED)")

    assert redis_after == 100, f"Redis stock rollback failed: {redis_after}"
    assert db_after == 100, f"PostgreSQL stock rollback failed: {db_after}"
    assert status_after == "EXPIRED", f"Order status was not updated to EXPIRED: {status_after}"

    # Try to pay for the expired order
    pay_res = requests.post(f"{BASE_URL}/api/v1/orders/pay", json={"reservation_id": reservation_id})
    print(f"[LATE PAYMENT ATTEMPT] Status: {pay_res.status_code}, Response: {pay_res.json()}")
    assert pay_res.status_code == 410, f"Expected 410 Gone, got {pay_res.status_code}"
    print(">>> PASS: Unpaid reservation cleanly rolled back and rejected on late payment!")

    # 2. Test successful payment within TTL window
    print("\n--- TEST CASE 2: Payment Confirmation Within TTL Window ---")
    order_res2 = requests.post(f"{BASE_URL}/api/v1/orders/fast?ttl=10", json={"product_id": 1, "user_id": "usr_paid"})
    assert order_res2.status_code == 202
    res_id2 = order_res2.json()["reservation_id"]
    time.sleep(0.3)

    # Pay before TTL expires
    pay_res2 = requests.post(f"{BASE_URL}/api/v1/orders/pay", json={"reservation_id": res_id2})
    print(f"[PAYMENT] Status: {pay_res2.status_code}, Response: {pay_res2.json()}")
    assert pay_res2.status_code == 200
    assert pay_res2.json()["status"] == "PAID"

    # Verify DB status is PAID
    status_paid = get_db_order_status(res_id2)
    assert status_paid == "PAID", f"DB status is not PAID: {status_paid}"

    # Verify TTL key was disarmed in Redis
    ttl_left = subprocess.check_output(["redis-cli", "ttl", f"order:{res_id2}:ttl"]).decode().strip()
    assert int(ttl_left) == -2, f"Redis TTL key was not deleted: {ttl_left}"
    print(f"[VERIFIED] Order status is PAID. Redis TTL key deleted ({ttl_left}). Rollback permanently disarmed.")

    # Test idempotent duplicate payment
    dup_pay = requests.post(f"{BASE_URL}/api/v1/orders/pay", json={"reservation_id": res_id2})
    assert dup_pay.status_code == 200
    assert "already paid" in dup_pay.json()["message"].lower()
    print("[VERIFIED] Duplicate payment confirmed idempotently.")

    print("\n================================================================")
    print("  ALL PHASE 5.2 TTL & ROLLBACK TESTS PASSED PERFECTLY!")
    print("================================================================")

if __name__ == "__main__":
    main()
