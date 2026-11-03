"""End-to-end test against a running service.

Usage:  python test_service.py https://your-app.onrender.com
        python test_service.py            (defaults to http://localhost:8000)
Uses only the Python standard library.
"""
import json
import sys
import time
import uuid
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read() or b"null"), (time.perf_counter() - start) * 1000
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null"), (time.perf_counter() - start) * 1000


def payload(tid):
    return {"transaction_id": tid, "source_account": "acc_user_789",
            "destination_account": "acc_merchant_456", "amount": 1500, "currency": "INR"}


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        sys.exit(1)


print(f"Testing {BASE}\n")
status, body, _ = req("GET", "/")
check(status == 200 and body["status"] == "HEALTHY", f"health check -> {body}")

# 1. Single transaction
tid = f"txn_{uuid.uuid4().hex[:12]}"
status, _, ms = req("POST", "/v1/webhooks/transactions", payload(tid))
check(status == 202, f"webhook returns 202 ({ms:.0f} ms)")
_, body, _ = req("GET", f"/v1/transactions/{tid}")
check(body[0]["status"] == "PROCESSING", "status is PROCESSING right after webhook")

# 2. Duplicates (sent concurrently)
with ThreadPoolExecutor(10) as ex:
    results = list(ex.map(lambda _: req("POST", "/v1/webhooks/transactions", payload(tid)), range(10)))
check(all(r[0] == 202 for r in results), "10 duplicate webhooks all return 202")
_, body, _ = req("GET", f"/v1/transactions/{tid}")
check(len(body) == 1, "still exactly one transaction stored")

# 3. Performance under load
ids = [f"txn_{uuid.uuid4().hex[:12]}" for _ in range(50)]
with ThreadPoolExecutor(20) as ex:
    results = list(ex.map(lambda i: req("POST", "/v1/webhooks/transactions", payload(i)), ids))
times = sorted(r[2] for r in results)
check(all(r[0] == 202 for r in results), "50 concurrent webhooks all return 202")
print(f"     latency p50={times[len(times)//2]:.0f}ms  max={times[-1]:.0f}ms")
check(times[-1] < 500, "all webhook responses under 500 ms")

# Wait for processing
print("\nWaiting ~35s for background processing...")
time.sleep(35)
_, body, _ = req("GET", f"/v1/transactions/{tid}")
t = body[0]
check(t["status"] == "PROCESSED" and t["processed_at"], f"single transaction PROCESSED: {t}")
done = sum(req("GET", f"/v1/transactions/{i}")[1][0]["status"] == "PROCESSED" for i in ids)
check(done == len(ids), f"{done}/{len(ids)} load-test transactions PROCESSED")

status, _, _ = req("GET", "/v1/transactions/does_not_exist")
check(status == 404, "unknown transaction returns 404")
print("\nAll tests passed.")
