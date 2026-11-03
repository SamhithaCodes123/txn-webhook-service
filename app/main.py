"""Transaction webhook service.

- POST /v1/webhooks/transactions -> persists the webhook and returns 202 immediately
- A background worker (same process) claims pending rows and processes them (30s simulated delay)
- GET /v1/transactions/{id}     -> transaction status
- GET /                          -> health check
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("txn-service")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
PROCESSING_DELAY_SECONDS = float(os.environ.get("PROCESSING_DELAY_SECONDS", "30"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "1"))
# A claimed row that has not finished within this window is assumed lost (crash/restart) and re-claimed.
CLAIM_TIMEOUT_SECONDS = int(os.environ.get("CLAIM_TIMEOUT_SECONDS", "120"))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "100"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_account      TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'PROCESSING',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    claimed_at          TIMESTAMPTZ,
    attempts            INT NOT NULL DEFAULT 0,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (created_at) WHERE status = 'PROCESSING';
"""


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=255)
    source_account: str = Field(min_length=1, max_length=255)
    destination_account: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- background worker
async def process_transaction(pool: asyncpg.Pool, txn_id: str, sem: asyncio.Semaphore) -> None:
    try:
        # Simulates the call to an external API.
        await asyncio.sleep(PROCESSING_DELAY_SECONDS)
        # Only finish if it is still pending: guarantees a single PROCESSED transition.
        result = await pool.execute(
            """UPDATE transactions
               SET status = 'PROCESSED', processed_at = now(), last_error = NULL
               WHERE transaction_id = $1 AND status = 'PROCESSING'""",
            txn_id,
        )
        log.info("processed %s (%s)", txn_id, result)
    except Exception as exc:  # leave the row pending; it will be re-claimed after the timeout
        log.exception("processing failed for %s", txn_id)
        try:
            await pool.execute(
                "UPDATE transactions SET last_error = $2 WHERE transaction_id = $1", txn_id, str(exc)[:500]
            )
        except Exception:
            pass
    finally:
        sem.release()


async def worker_loop(pool: asyncpg.Pool, stop: asyncio.Event) -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    tasks: set[asyncio.Task] = set()
    log.info("worker started")
    while not stop.is_set():
        try:
            free = sem._value  # noqa: SLF001 - number of slots available
            if free > 0:
                # Atomically claim pending rows. SKIP LOCKED makes this safe across multiple instances.
                rows = await pool.fetch(
                    """UPDATE transactions
                       SET claimed_at = now(), attempts = attempts + 1
                       WHERE transaction_id IN (
                           SELECT transaction_id FROM transactions
                           WHERE status = 'PROCESSING'
                             AND (claimed_at IS NULL
                                  OR claimed_at < now() - make_interval(secs => $1))
                           ORDER BY created_at
                           FOR UPDATE SKIP LOCKED
                           LIMIT $2)
                       RETURNING transaction_id""",
                    CLAIM_TIMEOUT_SECONDS,
                    free,
                )
                for r in rows:
                    await sem.acquire()
                    t = asyncio.create_task(process_transaction(pool, r["transaction_id"], sem))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
        except Exception:
            log.exception("worker loop error; retrying")
        try:
            await asyncio.wait_for(stop.wait(), timeout=POLL_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
    for t in tasks:
        t.cancel()
    log.info("worker stopped")


# ---------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10, command_timeout=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    stop = asyncio.Event()
    worker = asyncio.create_task(worker_loop(pool, stop))
    app.state.pool = pool
    yield
    stop.set()
    await worker
    await pool.close()


app = FastAPI(title="Transaction Webhook Service", lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "HEALTHY", "current_time": iso(datetime.now(timezone.utc))}


@app.post("/v1/webhooks/transactions", status_code=202)
async def receive_webhook(payload: TransactionWebhook):
    # Persist first, then acknowledge: once we return 202 the transaction can't be lost.
    # ON CONFLICT DO NOTHING makes duplicate webhooks a harmless no-op (idempotency).
    result = await app.state.pool.execute(
        """INSERT INTO transactions (transaction_id, source_account, destination_account, amount, currency)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (transaction_id) DO NOTHING""",
        payload.transaction_id,
        payload.source_account,
        payload.destination_account,
        payload.amount,
        payload.currency.upper(),
    )
    duplicate = result.endswith(" 0")
    return {"message": "accepted", "transaction_id": payload.transaction_id, "duplicate": duplicate}


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    row = await app.state.pool.fetchrow(
        "SELECT * FROM transactions WHERE transaction_id = $1", transaction_id
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return [
        {
            "transaction_id": row["transaction_id"],
            "source_account": row["source_account"],
            "destination_account": row["destination_account"],
            "amount": float(row["amount"]),
            "currency": row["currency"],
            "status": row["status"],
            "created_at": iso(row["created_at"]),
            "processed_at": iso(row["processed_at"]),
        }
    ]
