# Transaction Webhook Service

A Python service that receives transaction webhooks (e.g. from RazorPay), acknowledges them immediately with **202 Accepted**, and processes them reliably in the background (with a simulated 30-second external API call), storing results in a cloud PostgreSQL database.

**Live API:** `https://<your-app>.onrender.com` ← replace after deploying

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check → `{"status": "HEALTHY", "current_time": "..."}` |
| `POST` | `/v1/webhooks/transactions` | Receive a webhook → `202 Accepted` |
| `GET` | `/v1/transactions/{transaction_id}` | Transaction status (`PROCESSING` / `PROCESSED`) |

Interactive docs: `/docs` (Swagger UI).

## How it works

```
 Payment processor ──POST──▶ FastAPI ──INSERT ... ON CONFLICT DO NOTHING──▶ PostgreSQL
                              │  (returns 202 in a few ms)                  ▲     │
                              │                                             │     │
                              └── background worker ── claim rows (SKIP LOCKED)    │
                                     sleep 30s (external API) ── mark PROCESSED ───┘
```

1. **Receive:** the webhook is validated and written to the `transactions` table with status `PROCESSING`, then `202` is returned. The request does a single indexed insert, so it responds in milliseconds regardless of processing load.
2. **Process:** a background worker polls every second, atomically *claims* pending rows (`UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)`), waits 30 s to simulate the external call, then sets `status = 'PROCESSED'` and `processed_at`.
3. **Idempotency:** `transaction_id` is the primary key and inserts use `ON CONFLICT DO NOTHING`, so duplicate webhooks (even concurrent ones) are a harmless no-op and still return `202`. The final update also only applies `WHERE status = 'PROCESSING'`, so a transaction can only be marked processed once.
4. **Reliability:** the transaction is persisted *before* acknowledging, so once a `202` is returned it cannot be lost. If the server crashes or restarts mid-processing, the row stays `PROCESSING`; once its claim is older than `CLAIM_TIMEOUT_SECONDS` (default 120 s) the worker re-claims and finishes it. Processing errors are logged to `last_error` and retried the same way. Worker-loop errors (e.g. a DB blip) are caught and retried on the next poll.

## Technical choices

- **FastAPI + asyncpg:** async I/O means thousands of concurrent 30-second "external calls" are just sleeping coroutines, not blocked threads; asyncpg is the fastest Postgres driver for Python.
- **PostgreSQL as the queue (Neon, cloud-hosted):** a single dependency is both the durable store and the job queue. `FOR UPDATE SKIP LOCKED` is the standard pattern for a safe Postgres work queue and lets you scale to multiple instances without double-processing. No Redis/Celery to deploy or keep in sync with the DB, which removes a whole class of "queued but not saved" / "saved but not queued" failures.
- **Worker in the same process:** keeps deployment to one free web service. Because all state lives in the DB, the worker can be split into its own process/service later without code changes (just run more instances).
- **Bounded concurrency:** a semaphore (`MAX_CONCURRENT_JOBS`, default 100) prevents unbounded task growth under heavy load; excess rows simply wait in the table.
- **Schema auto-created on startup:** no migration step is needed to get running.

### Data model

| Column | Notes |
|---|---|
| `transaction_id` | Primary key (idempotency key) |
| `source_account`, `destination_account`, `amount`, `currency` | From the webhook |
| `status` | `PROCESSING` → `PROCESSED` |
| `created_at`, `processed_at` | Timing information |
| `claimed_at`, `attempts`, `last_error` | Worker bookkeeping for recovery/retries |

## Run locally

Requires Python 3.10+ and a PostgreSQL database (local or cloud).

```bash
pip install -r requirements.txt

# Option A: local Postgres via Docker
docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres

# Option B: a Neon connection string
export DATABASE_URL='postgresql://user:pass@ep-xxx.neon.tech/neondb?sslmode=require'

uvicorn app.main:app --reload --port 8000
```

Or with Docker Compose (app + Postgres): `docker compose up --build`

### Configuration (env vars)

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | local Postgres | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external API delay |
| `POLL_INTERVAL_SECONDS` | `1` | Worker poll interval |
| `CLAIM_TIMEOUT_SECONDS` | `120` | When a stuck claim is retried |
| `MAX_CONCURRENT_JOBS` | `100` | Max in-flight transactions per instance |

## Test

Automated end-to-end test (standard library only) covering all four success criteria:

```bash
python test_service.py                                 # local
python test_service.py https://<your-app>.onrender.com # deployed
```

It checks: health check, single transaction processed after ~30 s, 10 concurrent duplicates → one stored transaction, 50 concurrent webhooks all under 500 ms and all processed, and 404 for an unknown id.

Manual testing:

```bash
URL=http://localhost:8000

# 1. Single transaction
curl -i -X POST $URL/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
curl $URL/v1/transactions/txn_abc123def456   # PROCESSING, then PROCESSED after ~30s

# 2. Duplicates: run the POST above several times → still one transaction, processed once
```

## Deploy (Render + Neon, free tiers)

1. **Database:** create a project at [neon.tech](https://neon.tech) and copy the connection string (`...?sslmode=require`).
2. **Code:** push this repo to a public GitHub repository.
3. **Service:** on [render.com](https://render.com) → *New → Web Service* → select the repo. Render detects `render.yaml`; otherwise set:
   - Build command: `pip install -r requirements.txt`
   - Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - Environment variable: `DATABASE_URL` = your Neon connection string
4. Open `https://<your-app>.onrender.com/` → `{"status":"HEALTHY", ...}`.

> Note: Render's free tier spins down after inactivity, so the first request after idling can take ~30-50 s (cold start). Pending transactions are safe in the DB and are processed when the instance wakes. Use a paid instance (or Railway/Fly.io) to avoid cold starts.

## Possible improvements

- Verify the processor's webhook signature (e.g. RazorPay `X-Razorpay-Signature` HMAC).
- Run the worker as a separate service and scale instances independently.
- Exponential backoff + max attempts with a `FAILED` / dead-letter status.
- Use `LISTEN/NOTIFY` instead of polling for lower pickup latency.
