# Domain Processing Service

An asynchronous, highly concurrent domain-processing service that accepts batches of domains, normalizes them, and processes them in the background (DNS resolution, IP validation/SSRF checks, and HTTP probing) with strict durability, idempotency, and concurrency controls.

---

## Architecture & Engineering Highlights

The service is designed around an async-first architecture with distinct responsibility boundaries:

- **PostgreSQL as Durable Source of Truth**: Stores `Job`, `Task`, `Domain`, `DomainDetail`, and `IdempotencyRecord` entities with strict relational integrity, UUID primary keys, and specialized composite indexes.
- **Task Manager & Safe DB Polling**: Claims tasks using `SELECT ... FOR UPDATE SKIP LOCKED`, preventing lock contention across horizontal instances without worker lock overhead.
- **Bounded In-Memory Worker Queue**: Dispatches claimed tasks to an async worker pool via a bounded memory queue (`BoundedQueue`), providing natural backpressure and memory protection.
- **Workers Never Poll PostgreSQL Directly**: Workers consume exclusively from the in-memory bounded queue, keeping database query load decoupled from worker concurrency.
- **Short-Lived Worker Database Sessions**: Workers open short-lived database sessions strictly for status transitions and release PostgreSQL connections back to the pool before executing slow external I/O (DNS queries and HTTP probing), preventing DB pool exhaustion under heavy concurrent load.
- **Redis DomainLockManager Singleton**: Coordinates concurrent domain processing using Redis distributed locking with an application-level singleton client lifecycle (zero per-task client allocations).
- **Post-Lock Double-Check & Detail Reuse**: After acquiring a domain lock, workers verify whether another worker completed processing for that domain during the wait window. Fresh `DomainDetail` records (within the freshness window) bypass redundant external network calls.
- **Batch Insertion & N+1 Prevention**: `POST /jobs` resolves existing domains in bulk, creates missing domains in a single batch, and inserts all tasks in one transaction, completely eliminating N+1 database queries.
- **Idempotency Engine**: Supports `Idempotency-Key` (scoped by `X-Client-ID`) with SHA-256 payload hashing. Replaying the same payload returns `200 OK` with the existing `jobId`; sending a different payload with the same key returns `409 Conflict`.
- **Optimistic Concurrency Control (OCC)**: Protects `DomainDetail` against lost updates via atomic version increments (`version = version + 1`) and stale-write rejections.
- **Exponential Backoff & Lease Recovery**: Transient failures trigger exponential backoff with jitter. A background lease recovery loop in the Task Manager safely identifies expired processing leases and resets tasks to `PENDING`.
- **Soft Deactivation & Background Refresh**: Domains that fail persistently across maximum retry attempts are softly deactivated. A refresh scheduler identifies domains due for background re-validation via `next_refresh_at`.
- **DNS-First SSRF Protection**: Resolves A and AAAA DNS records and strictly validates target IP addresses against private (RFC 1918), loopback (127.0.0.0/8, ::1), link-local, multicast, and reserved ranges before initiating HTTP connections.
- **Structured JSON Logging & Observability**: Every request is tagged with a `request_id` correlation header and logged in structured JSON format with latency tracking and log level controls.
- **Graceful Shutdown**: Handles shutdown signals (`SIGTERM`/lifespan context) by stopping task claiming, draining in-flight worker queue items, and cleanly closing database and Redis connection pools.

---

## Technology Stack

- **Language**: Python 3.10+ (tested on Python 3.10 through 3.14)
- **Web Framework**: FastAPI, Starlette
- **Data Validation & DTOs**: Pydantic v2, Pydantic-Settings
- **Database & ORM**: PostgreSQL 15, SQLAlchemy 2.0 (AsyncIO), asyncpg
- **Schema Migrations**: Alembic
- **Distributed Locking / Coordination**: Redis 7, redis-py (asyncio)
- **Testing**: Pytest, Pytest-AsyncIO, HTTPX

---

## Prerequisites

- **Python**: 3.10 or newer
- **Docker & Docker Compose**: For containerized PostgreSQL and Redis
- **pip** (or `uv` / `poetry` for environment management)

---

## Local Infrastructure (Docker Compose)

Start the required PostgreSQL and Redis services using Docker Compose:

```bash
docker compose up -d
```

This starts:
- **db**: PostgreSQL 15 on port `5432` with a `pg_isready` healthcheck and `postgres_data` persistent volume.
- **redis**: Redis 7 on port `6379` with a `redis-cli ping` healthcheck and `redis_data` persistent volume.

---

## Local Setup & Application Startup

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Apply Database Migrations:**
   ```bash
   alembic upgrade head
   ```

3. **Start the Application:**
   Run the service from the repository root using `uvicorn`:
   ```bash
   python -m uvicorn src.domain_processing_service.main:app --host 0.0.0.0 --port 8000
   ```
   *(FastAPI automatically starts the background TaskManager coordinator and WorkerPool on startup).*

---

## Available API Endpoints

### Core Endpoints

#### 1. Submit a Job
`POST /jobs`

Accepts a list of domains for asynchronous processing.

**Headers (Optional):**
- `Idempotency-Key`: Client-provided key for safe retries.
- `X-Client-ID`: Client identifier to scope the idempotency key.

**Request Body:**
```json
{
  "domains": ["example.com", "google.com", "github.com"]
}
```

**Response (`202 Accepted` on new job, `200 OK` on idempotent replay):**
```json
{
  "jobId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "PENDING"
}
```

**Conflict Response (`409 Conflict`):**
Returned if the same `Idempotency-Key` is reused with a different payload.

---

#### 2. Get Job Status & Paginated Results
`GET /jobs/{job_id}`

Retrieves job progress summary and task results.

**Query Parameters:**
- `limit` (optional, default: `100`, max: `1000`): Maximum task records per page.
- `cursor` (optional): Base64-encoded pagination cursor for next page.

**Response (`200 OK`):**
```json
{
  "jobId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "COMPLETED",
  "summary": {
    "total": 3,
    "pending": 0,
    "processing": 0,
    "completed": 2,
    "failed": 1
  },
  "results": [
    {
      "taskId": "a1b2c3d4-0000-0000-0000-000000000001",
      "domain": "example.com",
      "status": "COMPLETED",
      "attempts": 1,
      "domainDetails": {
        "dns": { "a_records": ["93.184.216.34"], "aaaa_records": [] },
        "http": { "status_code": 200, "final_url": "https://example.com" }
      },
      "error": null
    }
  ],
  "nextCursor": null
}
```

---

### Observability Endpoints

- `GET /health/live`: Liveness probe. Returns `200 OK` with JSON `{"status": "live", "service": "domain-processing-service", "request_id": "..."}`.
- `GET /health/ready`: Readiness probe. Verifies PostgreSQL connectivity. Returns `200 OK` when healthy, or `503 Service Unavailable` if database connection fails.
- `GET /metrics`: Prometheus metrics registry endpoint.

---

## Running the Test Suite

The test suite contains 183 automated tests covering normalization, database migrations, repository transactions, task management, worker execution, idempotency, OCC, and end-to-end integration flows.

### Full Test Suite (Primary Verification)
```bash
python -m pytest -q
```

### Focused Test Suites
```bash
# E2E integration & concurrency tests
python -m pytest tests/test_phase14.py -v

# Idempotency, OCC, and soft deactivation tests
python -m pytest tests/test_phase12.py -v

# TaskManager & SKIP LOCKED claiming tests
python -m pytest tests/test_manager.py -v

# Database schema migrations apply/rollback tests
python -m pytest tests/test_migrations.py -v
```

---

## Configuration Reference

Configuration is managed via environment variables prefixed with `DOMAIN_PROCESSING_` (or loaded from a local `.env` file):

| Environment Variable | Purpose | Type | Default | Required | Example |
|---|---|---|---|:---:|---|
| `DOMAIN_PROCESSING_DATABASE_URL` | PostgreSQL connection URL (asyncpg) | `str` | `postgresql+asyncpg://user:password@localhost:5432/domain_processing` | No | `postgresql+asyncpg://user:pass@db:5432/domain_processing` |
| `DOMAIN_PROCESSING_DB_POOL_SIZE` | SQLAlchemy connection pool size | `int` | `5` | No | `10` |
| `DOMAIN_PROCESSING_DB_MAX_OVERFLOW` | Maximum overflow connections beyond pool size | `int` | `10` | No | `20` |
| `DOMAIN_PROCESSING_DB_POOL_TIMEOUT_SECONDS` | Timeout for acquiring connection from pool | `float` | `5.0` | No | `10.0` |
| `DOMAIN_PROCESSING_REDIS_HOST` | Redis server hostname | `str` | `localhost` | No | `redis` |
| `DOMAIN_PROCESSING_REDIS_PORT` | Redis server port | `int` | `6379` | No | `6379` |
| `DOMAIN_PROCESSING_REDIS_PASSWORD` | Redis authentication password | `str` | `None` | No | `secret` |
| `DOMAIN_PROCESSING_REDIS_DB` | Redis database number | `int` | `0` | No | `0` |
| `DOMAIN_PROCESSING_WORKER_CONCURRENCY` | Number of concurrent worker coroutines | `int` | `50` | No | `25` |
| `DOMAIN_PROCESSING_WORKER_QUEUE_CAPACITY` | In-memory bounded queue capacity | `int` | `100` | No | `200` |
| `DOMAIN_PROCESSING_TASK_LEASE_SECONDS` | Duration of task processing lease | `int` | `120` | No | `60` |
| `DOMAIN_PROCESSING_MAX_ATTEMPTS` | Maximum retry attempts before terminal failure | `int` | `3` | No | `5` |
| `DOMAIN_PROCESSING_MAX_DOMAINS_PER_REQUEST` | Maximum domain count in single `POST /jobs` | `int` | `1000` | No | `500` |
| `DOMAIN_PROCESSING_DEFAULT_PAGE_SIZE` | Default page size for `GET /jobs/{id}` | `int` | `100` | No | `50` |
| `DOMAIN_PROCESSING_MAX_PAGE_SIZE` | Maximum allowed page size for `GET /jobs/{id}` | `int` | `1000` | No | `500` |
| `DOMAIN_PROCESSING_DNS_TIMEOUT_SECONDS` | Timeout for DNS resolution (A/AAAA) | `float` | `3.0` | No | `5.0` |
| `DOMAIN_PROCESSING_CONNECT_TIMEOUT_SECONDS` | TCP connection timeout for HTTP probing | `float` | `5.0` | No | `5.0` |
| `DOMAIN_PROCESSING_TLS_TIMEOUT_SECONDS` | TLS handshake timeout | `float` | `5.0` | No | `5.0` |
| `DOMAIN_PROCESSING_READ_TIMEOUT_SECONDS` | HTTP read response timeout | `float` | `10.0` | No | `5.0` |
| `DOMAIN_PROCESSING_TOTAL_PROCESSING_TIMEOUT_SECONDS` | Total timeout per task execution | `float` | `20.0` | No | `15.0` |
| `DOMAIN_PROCESSING_MAX_RESPONSE_READ_BYTES` | Maximum HTTP body bytes read into memory | `int` | `51200` (50 KB) | No | `10240` |
| `DOMAIN_PROCESSING_DOMAIN_DETAIL_FRESHNESS_SECONDS` | Window where cached domain details are reused | `int` | `86400` (24 hr) | No | `43200` |
| `DOMAIN_PROCESSING_REFRESH_INTERVAL_SECONDS` | Interval after which domain is scheduled for refresh | `int` | `1209600` (14 days) | No | `604800` |
| `DOMAIN_PROCESSING_SHUTDOWN_GRACE_SECONDS` | Grace period for worker draining on shutdown | `int` | `30` | No | `15` |
| `DOMAIN_PROCESSING_LOG_LEVEL` | Application logging level | `str` | `INFO` | No | `DEBUG` |
| `DOMAIN_PROCESSING_ENVIRONMENT` | Environment name (`local`, `production`, etc.) | `str` | `local` | No | `production` |
| `DOMAIN_PROCESSING_APP_NAME` | Service application name | `str` | `domain-processing-service` | No | `domain-service` |