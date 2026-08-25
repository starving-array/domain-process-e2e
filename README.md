# Domain Processing Service

An asynchronous, highly concurrent backend service that ingests domain batches, executes safe asynchronous DNS resolution and HTTP probing, and manages domain observations with strict concurrency, idempotency, and durability guarantees.

---

## Overview

The **Domain Processing Service** solves the challenge of safely ingesting and processing large domain lists against slow, unpredictable external networks without starving database connection pools, inducing lock contention, or exposing internal networks to security vulnerabilities.

### Primary Workflow
1. **Ingestion**: Clients submit domain batches via `POST /jobs`, receiving an immediate `202 Accepted` response with a tracking `jobId`.
2. **Persistence & Deduplication**: Domains are normalized, deduplicated, and persisted atomically alongside pending tasks in PostgreSQL in constant time (no N+1 queries).
3. **Safe Orchestration**: A background `TaskManager` claims batches using `SELECT ... FOR UPDATE SKIP LOCKED` and feeds an in-memory `BoundedQueue`.
4. **Decoupled Worker Processing**: A 50-coroutine worker pool dequeues tasks, coordinates concurrent domain probing via a Redis distributed lock (`DomainLockManager`), releases database connections during external network I/O, resolves DNS via `aiodns`, checks for SSRF prohibited IP ranges, probes HTTP/HTTPS endpoints, extracts page `<title>`s, and commits updates via Optimistic Concurrency Control (OCC).
5. **Status & Result Retrieval**: Clients poll or paginate through task results using keyset cursor pagination via `GET /jobs/{job_id}`.

### Target Consumers
Security platforms, web crawlers, asset inventory services, and monitoring pipelines that need reliable, high-throughput domain metadata extraction.

---

## Key Features

- **Asynchronous Background Processing**: Immediate `202 Accepted` job ingestion decoupled from long-running network operations.
- **Batch Job Ingestion & N+1 Prevention**: Bulk domain lookup, batch creation, and bulk task inserts execute in constant time.
- **Keyset Cursor Pagination**: Deterministic `(created_at, id)` cursor pagination on `GET /jobs/{job_id}` scaling seamlessly to large task counts.
- **Redis-Backed Domain Locking**: Distributed locks prevent redundant concurrent outbound requests for identical domains across workers.
- **Post-Lock Double Check & Cache Reuse**: Automatically reuses fresh `DomainDetail` records without issuing redundant external requests.
- **Short-Lived Worker DB Sessions**: Database connections are released back to the pool before slow external DNS/HTTP I/O, preventing connection pool exhaustion.
- **Optimistic Concurrency Control (OCC)**: Version-based updates (`version = version + 1`) on `DomainDetail` prevent lost updates.
- **Idempotency & Replay Protection**: `Idempotency-Key` (scoped by `X-Client-ID`) with SHA-256 payload hashing ensures safe request retries.
- **Exponential Backoff & Transient Retry**: Intelligent transient error classification with exponential backoff and jitter.
- **Lease Recovery Loop**: Automatically detects and reclaims abandoned or timed-out worker leases back to `PENDING`.
- **Soft Deactivation & Background Refresh**: Softly deactivates domains after exceeding max attempts; periodic scheduler re-validates retained domains via `next_refresh_at`.
- **DNS-First SSRF Protection**: Resolves A/AAAA records and strictly blocks requests to private (RFC 1918), loopback, link-local, multicast, and cloud metadata IPs.
- **Bounded Memory Ingestion**: Caps HTTP response body reads to 50 KB and streaming parses `<title>` tags to protect memory.
- **Partial Job Completion**: Failed individual tasks do not fail the overall job; jobs transition to `COMPLETED` when all tasks are terminal.
- **Structured JSON Logging & Correlation IDs**: Every log is formatted as JSON with bound `request_id` and timing metadata.
- **Health & Metrics Probes**: `/health/live`, `/health/ready` (DB verification), and `/metrics` (Prometheus registry).
- **Graceful Shutdown**: Controlled draining of the bounded worker queue and clean disposal of database and Redis pools.

---

## Tech Stack

| Component | Technology | Version | Purpose |
|---|---|---|---|
| **Language** | Python | `>=3.11` | Async-first application runtime |
| **Web Framework** | FastAPI / Starlette | `>=0.100.0` | Async REST API framework |
| **ASGI Server** | Uvicorn | `>=0.23.0` | High-performance ASGI web server |
| **Database** | PostgreSQL | `15-alpine` | Primary durable data store |
| **Database Driver & ORM** | SQLAlchemy / asyncpg | `>=2.0.0` / `>=0.28.0` | Async relational ORM and connection pooling |
| **Schema Migrations** | Alembic | `>=1.11.0` | Relational schema versioning |
| **Coordination & Locking** | Redis / redis-py | `7-alpine` / `>=5.0.0` | Distributed domain locking store |
| **HTTP Client & DNS** | HTTPX / aiodns | `>=0.24.0` / `>=3.0.0` | Outbound HTTP probing and asynchronous DNS resolution |
| **Validation & DTOs** | Pydantic / Pydantic-Settings | `>=2.0.0` | Request validation and environment configuration |
| **Testing** | Pytest / Pytest-AsyncIO | `>=7.4.0` / `>=0.21.1` | Unit, concurrency, and integration testing |

---

## Architecture Summary

```
Client  -->  FastAPI  -->  PostgreSQL (State & Tasks) + Redis (Locks)
                              ^
                              | (SKIP LOCKED claim)
                         Task Manager  -->  BoundedQueue  -->  Workers (DNS / SSRF / HTTP)
```

For complete architecture diagrams, concurrency patterns, and design decisions, see:
👉 **[Detailed Architecture & System Design](./ARCHITECTURE.md)**

---

## Quick Start — Docker

The fastest way to run the complete stack. **No local Python installation required.**

### 1. Clone & Start
```bash
git clone https://github.com/starving-array/domain-process-e2e.git
cd domain-process-e2e
docker compose --profile full up -d --build
```

### 2. Verify
```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
```

### 3. Inspect & Logs
```bash
docker compose --profile full ps
docker compose logs -f app
```

### 4. Stop
```bash
docker compose --profile full down
```

### 5. Run Tests (Docker)
```bash
docker compose --profile test run --rm tests
```

> **Note:** The Docker quick start runs PostgreSQL, Redis, and the application entirely in containers. This is the easiest setup path but runs the complete stack in containers.

---

## Alternative — Manual Development Setup

For developers who prefer running the application natively, or for lower-resource machines where only PostgreSQL and Redis run in containers while the application runs on the host.

**Requires:** Python `>=3.11`

### 1. Clone the Repository
```bash
git clone https://github.com/starving-array/domain-process-e2e.git
cd domain-process-e2e
```

### 2. Start Infrastructure (PostgreSQL 15 & Redis 7)
```bash
docker compose up -d
```
Verify containers are running and healthy:
```bash
docker compose ps
```

### 3. Create & Activate Virtual Environment

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**Linux / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Install Dependencies
```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

### 5. Apply Database Migrations
```bash
python -m alembic upgrade head
```

### 6. Start the Application
```bash
python -m uvicorn src.domain_processing_service.main:app --host 0.0.0.0 --port 8000
```

> **Note:** The manual setup avoids running the application itself inside Docker. PostgreSQL and Redis remain containerized, while the application runs natively with full debugger support.

---

## Verifying the Service & API Examples

### 1. Health & Metrics Probes

**Liveness Probe:**
```bash
curl http://localhost:8000/health/live
```
*Expected Response (200 OK):*
```json
{
  "status": "live",
  "service": "domain-processing-service",
  "request_id": "req-..."
}
```

**Readiness Probe (Verifies PostgreSQL connectivity):**
```bash
curl http://localhost:8000/health/ready
```
*Expected Response (200 OK):*
```json
{
  "status": "ready",
  "service": "domain-processing-service",
  "dependencies": {
    "postgresql": "ok"
  },
  "request_id": "req-..."
}
```

**Prometheus Metrics:**
```bash
curl http://localhost:8000/metrics
```

---

### 2. Submit a Domain Processing Job (`POST /jobs`)

Submit a batch of domains for asynchronous processing. You can optionally supply `Idempotency-Key` and `X-Client-ID` headers to guarantee idempotent submission:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: sample-batch-01" \
  -H "X-Client-ID: client-01" \
  -d '{
    "domains": [
      "example.com",
      "httpbin.org",
      "example.invalid"
    ]
  }'
```

*Expected Response (`202 Accepted` on new job submission, or `200 OK` on idempotent replay):*
```json
{
  "jobId": "<job-id>",
  "status": "PENDING"
}
```

---

### 3. Retrieve Job Status & Results (`GET /jobs/{job_id}`)

Domain processing is asynchronous. Clients should poll `GET /jobs/{job_id}` until the job reaches a terminal state (`COMPLETED`):

```bash
curl http://localhost:8000/jobs/<job-id>
```

*Expected Response (`200 OK`):*
```json
{
  "jobId": "<job-id>",
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
      "taskId": "<task-id>",
      "domain": "example.com",
      "status": "COMPLETED",
      "attempts": 1,
      "domainDetails": {
        "dns": {
          "a_records": ["..."],
          "aaaa_records": ["..."]
        },
        "http": {
          "status_code": 200,
          "response_time_ms": 54,
          "page_title": "Example Domain"
        }
      },
      "error": null
    }
  ],
  "nextCursor": null
}
```

---

## Running the Automated Test Suite

Execute the full automated test suite covering unit, integration, migration, concurrency, idempotency, OCC, DNS, HTTP, worker, and end-to-end tests:

```bash
python -m pytest -q
```

To run focused concurrency and end-to-end suites:
```bash
python -m pytest tests/test_phase14.py -v
```

---

## DNS Resolver Configuration

- **Code Default:** `AppSettings.dns_nameservers` defaults in code to `["8.8.8.8", "1.1.1.1", "8.8.4.4"]`.
- **Runtime Environment Override:** The resolver nameservers can be overridden via the `DOMAIN_PROCESSING_DNS_NAMESERVERS` environment variable using a comma-separated list (e.g. `DOMAIN_PROCESSING_DNS_NAMESERVERS="8.8.8.8,1.1.1.1"`).
- **Resolver Construction:** The application passes these configured nameservers directly into `aiodns.DNSResolver(nameservers=...)`.

---

## Documentation Index

The following root-level documentation files are available in this repository:

- 📘 **[Architecture & System Design](./ARCHITECTURE.md)** — Detailed component design, data models, concurrency controls, and SSRF security.
- 🛠️ **[Setup & Developer Guide](./SETUP.md)** — Comprehensive installation guide, Docker configuration, troubleshooting, and full 27-variable configuration reference.
- 📋 **[Engineering Test & Validation Report](./TEST-VALIDATION-REPORT.md)** — Comprehensive evidence-based validation report covering automated test suites, unmocked DNS resolution, HTTP redirect observation, database persistence proofs, and environment isolation.
