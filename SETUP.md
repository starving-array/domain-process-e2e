# Setup & Developer Run Guide

This guide provides two supported ways to run the Domain Processing Service:

- **Option A — Docker Quick Start**: Run the complete stack in containers. No local Python required.
- **Option B — Manual Development Setup**: Run the application natively with containerized PostgreSQL and Redis.

---

## Option A — Docker Quick Start

The fastest way to get started. Requires only **Docker & Docker Compose**.

```bash
git clone https://github.com/starving-array/domain-process-e2e.git
cd domain-process-e2e
docker compose --profile full up -d --build
```

Verify the service is running:
```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
```

Inspect containers and logs:
```bash
docker compose --profile full ps
docker compose logs -f app
```

Run the test suite via Docker:
```bash
docker compose --profile test run --rm tests
```

Stop all containers:
```bash
docker compose --profile full down
```

> **Note:** The Docker quick start runs PostgreSQL, Redis, and the application entirely in containers. This is the easiest setup path but runs the complete stack in containers. The manual setup (Option B) is useful for lower-resource machines because PostgreSQL and Redis can remain containerized while the application runs natively.

---

## Option B — Manual Development Setup

### 1. Prerequisites

- **Python**: Version `>=3.11` (verified with Python 3.11, 3.12, 3.13, 3.14). Not required for Docker quick start.
- **Docker & Docker Compose**: For containerized PostgreSQL 15 and Redis 7.
- **Git**: For source control.
- **pip** (or `uv` / `poetry`): For Python dependency management.

---

## 2. Infrastructure Setup (Docker Compose)

The service relies on PostgreSQL 15 for durable state and Redis 7 for distributed domain locking. Start these services using Docker Compose:

```bash
docker compose up -d
```

### Container Specifications

| Service | Image | Host Port | Healthcheck | Persistent Volume |
|---|---|---|---|---|
| `db` | `postgres:15-alpine` | `5432` | `pg_isready -U user -d domain_processing` | `postgres_data` |
| `redis` | `redis:7-alpine` | `6379` | `redis-cli ping` | `redis_data` |

To verify container health:
```bash
docker compose ps
```

---

## 3. Application Installation & Database Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/starving-array/domain-process-e2e.git
   cd domain-process-e2e
   ```

2. **Create & Activate a Virtual Environment:**
   ```bash
   # On macOS/Linux:
   python3 -m venv .venv
   source .venv/bin/activate

   # On Windows (PowerShell):
   python -m venv .venv
   .venv\Scripts\Activate.ps1
   ```

3. **Install Dependencies:**
   ```bash
   python -m pip install --upgrade pip
   python -m pip install -e ".[dev]"
   # Or using requirements.txt:
   pip install -r requirements.txt
   ```

4. **Apply Database Migrations:**
   Run Alembic to create all required tables, enums, constraints, and composite indexes:
   ```bash
   python -m alembic upgrade head
   ```

---

## 4. Running the Application

Start the FastAPI application from the repository root:

```bash
python -m uvicorn src.domain_processing_service.main:app --host 0.0.0.0 --port 8000
```

On startup, FastAPI's lifespan context automatically:
1. Verifies database connectivity.
2. Initializes the application-level `DomainLockManager` Redis singleton.
3. Spawns the background `TaskManager` polling loop.
4. Starts the 50-coroutine `WorkerPool`.

---

## 5. Verifying the Service

### Health Check Probes
```bash
# Liveness Check
curl http://localhost:8000/health/live

# Readiness Check (verifies DB connectivity)
curl http://localhost:8000/health/ready
```

### Submit a Domain Processing Job
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: sample-batch-1" \
  -d '{"domains": ["example.com", "google.com", "github.com"]}'
```

### Retrieve Job Status & Results
```bash
curl http://localhost:8000/jobs/<job-id>
```

---

## 6. Running the Test Suite

The test suite includes automated unit, integration, migration, concurrency, idempotency, OCC, DNS, HTTP, worker, and end-to-end tests.

### Full Test Suite (Primary Verification)
```bash
python -m pytest -q
```

### Focused Test Suites
```bash
# End-to-End integration & concurrency tests
python -m pytest tests/test_phase14.py -v

# Idempotency, OCC, and soft deactivation tests
python -m pytest tests/test_phase12.py -v

# Task Manager & SKIP LOCKED claim tests
python -m pytest tests/test_manager.py -v

# Database schema migration tests
python -m pytest tests/test_migrations.py -v
```

---

## 7. Configuration Reference Table

Configuration is loaded from environment variables prefixed with `DOMAIN_PROCESSING_` or an optional `.env` file in the project root:

| Environment Variable | Purpose | Type | Default | Required |
|---|---|---|---|:---:|
| `DOMAIN_PROCESSING_DATABASE_URL` | PostgreSQL async connection URL | `str` | `postgresql+asyncpg://user:password@localhost:5432/domain_processing` | No |
| `DOMAIN_PROCESSING_DB_POOL_SIZE` | SQLAlchemy connection pool size | `int` | `5` | No |
| `DOMAIN_PROCESSING_DB_MAX_OVERFLOW` | Maximum overflow connections | `int` | `10` | No |
| `DOMAIN_PROCESSING_DB_POOL_TIMEOUT_SECONDS` | Connection pool checkout timeout | `float` | `5.0` | No |
| `DOMAIN_PROCESSING_REDIS_HOST` | Redis server hostname | `str` | `localhost` | No |
| `DOMAIN_PROCESSING_REDIS_PORT` | Redis server port | `int` | `6379` | No |
| `DOMAIN_PROCESSING_REDIS_PASSWORD` | Redis authentication password | `str` | `None` | No |
| `DOMAIN_PROCESSING_REDIS_DB` | Redis database number | `int` | `0` | No |
| `DOMAIN_PROCESSING_WORKER_CONCURRENCY` | Number of worker coroutines | `int` | `50` | No |
| `DOMAIN_PROCESSING_WORKER_QUEUE_CAPACITY` | Bounded memory queue capacity | `int` | `100` | No |
| `DOMAIN_PROCESSING_TASK_LEASE_SECONDS` | Duration of task processing lease | `int` | `120` | No |
| `DOMAIN_PROCESSING_MAX_ATTEMPTS` | Max retries before terminal failure | `int` | `3` | No |
| `DOMAIN_PROCESSING_MAX_DOMAINS_PER_REQUEST` | Max domains per `POST /jobs` | `int` | `1000` | No |
| `DOMAIN_PROCESSING_DEFAULT_PAGE_SIZE` | Default page limit for `GET /jobs/{id}` | `int` | `100` | No |
| `DOMAIN_PROCESSING_MAX_PAGE_SIZE` | Maximum page limit for `GET /jobs/{id}` | `int` | `1000` | No |
| `DOMAIN_PROCESSING_DNS_NAMESERVERS` | Upstream DNS nameservers for aiodns (supports comma-separated override) | `list[str]` / `str` | `8.8.8.8,1.1.1.1,8.8.4.4` (code default) | No |
| `DOMAIN_PROCESSING_DNS_TIMEOUT_SECONDS` | Timeout for DNS resolution | `float` | `3.0` | No |
| `DOMAIN_PROCESSING_CONNECT_TIMEOUT_SECONDS` | TCP connection timeout for HTTP | `float` | `5.0` | No |
| `DOMAIN_PROCESSING_TLS_TIMEOUT_SECONDS` | TLS handshake timeout | `float` | `5.0` | No |
| `DOMAIN_PROCESSING_READ_TIMEOUT_SECONDS` | HTTP response read timeout | `float` | `10.0` | No |
| `DOMAIN_PROCESSING_TOTAL_PROCESSING_TIMEOUT_SECONDS` | Total execution timeout per task | `float` | `20.0` | No |
| `DOMAIN_PROCESSING_MAX_RESPONSE_READ_BYTES` | Max response body bytes read | `int` | `51200` (50 KB) | No |
| `DOMAIN_PROCESSING_DOMAIN_DETAIL_FRESHNESS_SECONDS` | Freshness window for cache reuse | `int` | `86400` (24 hr) | No |
| `DOMAIN_PROCESSING_REFRESH_INTERVAL_SECONDS` | Periodic refresh interval | `int` | `1209600` (14 days) | No |
| `DOMAIN_PROCESSING_SHUTDOWN_GRACE_SECONDS` | Grace period for worker draining | `int` | `30` | No |
| `DOMAIN_PROCESSING_LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `str` | `INFO` | No |
| `DOMAIN_PROCESSING_ENVIRONMENT` | Environment name | `str` | `local` | No |
| `DOMAIN_PROCESSING_APP_NAME` | Service name | `str` | `domain-processing-service` | No |

---

## 8. Troubleshooting & FAQ

- **Database Connection Error on Startup**: Ensure `docker compose ps` shows `db` in `healthy` state. If migrations have not been applied, run `alembic upgrade head`.
- **Port Conflicts**: If port `5432` or `6379` is already bound by a local PostgreSQL or Redis instance, either stop the local services or map different host ports in `docker-compose.yml` and update `DOMAIN_PROCESSING_DATABASE_URL` / `DOMAIN_PROCESSING_REDIS_PORT`.
- **Database Reset**: To completely wipe and rebuild the database schema:
  ```bash
  python -m alembic downgrade base
  python -m alembic upgrade head
  ```
