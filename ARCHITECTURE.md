# Architecture & System Design

This document details the architectural principles, component interactions, concurrency controls, and engineering hardening implemented in the Domain Processing Service.

---

## 1. High-Level Architecture Overview

The system is designed as an asynchronous, non-blocking pipeline where API ingestion, task orchestration, distributed coordination, and network I/O operate with decoupled lifecycles:

```
+-----------------------------------------------------------------------------------+
|                                  HTTP Clients                                     |
+-----------------------------------------------------------------------------------+
                                         |
                       POST /jobs, GET /jobs/{id}, /health
                                         v
+-----------------------------------------------------------------------------------+
|                              FastAPI Web Service                                  |
|   - Request Validation (DTOs)          - Structured JSON Logging & Request IDs    |
|   - Bulk Domain Normalization          - Idempotency Engine (SHA-256 Hashing)     |
|   - Atomic Batch DB Insertion          - Prometheus Metrics Registry              |
+-----------------------------------------------------------------------------------+
          |                                                       |
          |  (1) Atomic Job/Task Insert                           |  (4) Acquire Domain Lock
          v                                                       v
+-----------------------------+                         +---------------------------+
|      PostgreSQL 15          |                         |          Redis 7          |
|  - Durable State Store      |                         |  - Distributed Lock Store |
|  - Task Queue (SKIP LOCKED) |                         |  - Singleton Connection   |
|  - OCC on DomainDetail      |                         |  - Cross-Worker Domain    |
|  - Composite Cursor Indexes |                         |    Coordination           |
+-----------------------------+                         +---------------------------+
          ^                                                       ^
          | (2) Polling / Lease Recovery                          |
          |                                                       |
+-------------------------------------------------------------+   |
|                      Task Manager                           |   |
|  - Background Poller (FOR UPDATE SKIP LOCKED)               |   |
|  - Dispatches Tasks to BoundedQueue                         |   |
|  - Recovers Expired Processing Leases                       |   |
|  - Refreshes Retained Domains                               |   |
+-------------------------------------------------------------+   |
                               |                                  |
                   Enqueues claimed tasks                         |
                               v                                  |
+-------------------------------------------------------------+   |
|                 In-Memory Bounded Queue                     |   |
|           (Backpressure & Memory Protection)                |   |
+-------------------------------------------------------------+   |
                               |                                  |
                   Dequeues in-flight tasks                       |
                               v                                  |
+-------------------------------------------------------------+   |
|                  Async Worker Pool (50 Coroutines)          |   |
|  - Consumes from BoundedQueue (Never polls PostgreSQL)      |   |
|  - Short-lived DB sessions (Release connections before I/O)-+---+
|  - Coordinates via Redis Lock + Post-Lock Double Check      |
|  - DNS-First Resolution & Strict SSRF IP Validation         |
|  - Bounded HTTP/HTTPS Probing & HTML Title Extraction       |
|  - OCC DomainDetail Update & Task Terminal Transition       |
+-------------------------------------------------------------+
```

---

## 2. Core Architectural Principles

1. **PostgreSQL is the Durable Source of Truth**: All domain identities, task lifecycle states, job aggregations, idempotency records, and cached observations reside durably in PostgreSQL with strict relational integrity, UUID primary keys, and specialized composite indexes.
2. **Zero Polling by Workers**: Worker coroutines **never** query or poll PostgreSQL for work. They consume exclusively from an in-memory `BoundedQueue` fed by the centralized `TaskManager`.
3. **Short-Lived Worker Database Sessions**: Database connections are checked out of the pool strictly for brief reads/writes (claim confirmation, initial detail check, and terminal OCC write). Connections are **explicitly released back to the pool before executing slow external network I/O** (DNS queries and HTTP probing).
4. **Single Application-Level Redis Client**: A singleton `DomainLockManager` client is initialized in FastAPI's lifespan startup and shared across all workers, completely eliminating per-task Redis connection allocations.
5. **No N+1 Query Patterns**: Batch domain lookups (`WHERE normalized_domain IN (...)`), bulk domain creation, and batch task inserts ensure constant-time database operations regardless of request batch size.

---

## 3. Data Model & Database Design

### Entity Relationships

```
+-------------------+        1:N         +-------------------+
|        Job        | -----------------> |       Task        |
|  - id (UUID)      |                    |  - id (UUID)      |
|  - status (Enum)  |                    |  - job_id (FK)    |
|  - created_at     |                    |  - domain_id (FK) |
|  - updated_at     |                    |  - status (Enum)  |
+-------------------+                    |  - attempts (int) |
                                         |  - lease_expires  |
                                         |  - next_attempt_at|
                                         +-------------------+
                                                   |
                                                   | N:1
                                                   v
+-------------------+        1:1         +-------------------+
|   DomainDetail    | <----------------- |      Domain       |
|  - domain_id (FK) |                    |  - id (UUID)      |
|  - dns_records    |                    |  - normalized (UQ)|
|  - http_status    |                    |  - is_active      |
|  - html_title     |                    |  - deactivated_at |
|  - fetched_at     |                    +-------------------+
|  - version (OCC)  |
|  - next_refresh_at|
+-------------------+
```

### Specialized Indexing Strategy

- `idx_tasks_claim` on `task (status, next_attempt_at, type)`: Powers high-throughput `FOR UPDATE SKIP LOCKED` task claiming by the Task Manager.
- `idx_tasks_lease` on `task (status, lease_expires_at)`: Powers the background lease recovery loop to quickly identify abandoned or timed-out worker tasks.
- `idx_tasks_cursor` on `task (job_id, created_at, id)`: Enables deterministic, efficient keyset cursor pagination for `GET /jobs/{id}`.
- `idx_domain_normalized` on `domain (normalized_domain)` (UNIQUE): Guarantees global domain identity and fast bulk lookups.
- `idx_domain_detail_refresh` on `domain_detail (next_refresh_at)`: Allows the background scheduler to quickly fetch domains due for periodic re-verification.

---

## 4. Concurrency & Synchronization Controls

### Task Claiming via `SKIP LOCKED`
The `TaskManager` periodically executes:
```sql
SELECT * FROM task
WHERE status = 'PENDING' AND next_attempt_at <= NOW()
ORDER BY created_at ASC
LIMIT :batch_size
FOR UPDATE SKIP LOCKED;
```
This guarantees that multiple Task Manager instances or concurrent query cycles never block each other or claim duplicate tasks.

### Single-Ownership Attempt & Lease Lifecycle
- **Claim Ownership**: `claim_tasks()` is the **sole owner** of incrementing `task.attempts` and setting `task.lease_expires_at = now + lease_seconds`.
- **Transient Reschedule**: When a task encounters a retryable network failure (e.g. DNS timeout), `_write_reschedule_task()` clears `lease_expires_at = None`, calculates exponential backoff (`next_attempt_at = now + delay`), and transitions status back to `PENDING` without double-incrementing attempts.
- **Terminal Threshold**: If `task.attempts >= max_attempts`, the task is immediately transitioned to `FAILED` with error code `MAX_ATTEMPTS_EXCEEDED` and triggers domain deactivation.

### Distributed Domain Locking & Post-Lock Double Check
To prevent multiple concurrent workers from probing the same external domain simultaneously:
1. The worker requests an async distributed lock in Redis keyed by `domain_lock:{normalized_domain}` with a 60-second TTL.
2. If another worker holds the lock, the worker waits with backpressure.
3. **Post-Lock Double Check**: Upon acquiring the lock, the worker opens a short-lived DB session to inspect `domain_detail`. If another worker completed the domain while this worker was waiting, the fresh `DomainDetail` is reused immediately, bypassing redundant external DNS/HTTP requests entirely.

### Optimistic Concurrency Control (OCC)
When persisting updated `DomainDetail` records, workers execute an atomic OCC update:
```sql
UPDATE domain_detail
SET dns_records = :dns, http_status = :status, html_title = :title,
    fetched_at = :now, next_refresh_at = :next_refresh, version = version + 1
WHERE domain_id = :domain_id AND version = :expected_version;
```
If another transaction updated the record concurrently, the zero row-count match triggers OCC conflict handling, preserving data integrity without table-level locking.

---

## 5. Security & Inbound/Outbound Protection

### DNS-First SSRF (Server-Side Request Forgery) Protection
Before initiating any outbound HTTP or HTTPS connection, the service executes a strict two-stage security gate:
1. **DNS Resolution**: Resolves A and AAAA records for the target domain using `aiodns`.
2. **IP Blacklist Validation**: Evaluates all resolved IP addresses against prohibited network ranges using Python's `ipaddress` module:
   - IPv4 Loopback (`127.0.0.0/8`)
   - IPv4 Private Networks (RFC 1918: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`)
   - IPv4 Link-Local (`169.254.0.0/16`)
   - IPv4 Multicast (`224.0.0.0/4`) & Broadcast (`255.255.255.255/32`)
   - IPv6 Loopback (`::1`) & Unspecified (`::`)
   - IPv6 Unique Local (`fc00::/7`) & Link-Local (`fe80::/10`)
   - Cloud Metadata Services (`169.254.169.254`)
3. **Enforcement**: If any resolved IP falls within a prohibited range, outbound HTTP probing is aborted immediately, and the task is marked `FAILED` with error code `SSRF_PROHIBITED_IP`.

### Bounded HTTP Response Ingestion
To protect against memory exhaustion (zip bombs, unbounded streaming endpoints):
- Streaming response bodies are read up to a strict maximum limit (`max_response_read_bytes = 50 KB`).
- Only the HTML `<head>` section is parsed using a streaming regex parser to extract `<title>` text, releasing the connection immediately.

---

## 6. Observability & Graceful Shutdown

- **Contextual Structured JSON Logging**: Every log entry includes ISO-8601 timestamps, log level, event name, `request_id`, asyncio `taskName`, and structured metadata.
- **Correlation ID Tracking**: The `CorrelationIdMiddleware` extracts incoming `X-Request-ID` headers or generates new `req-{uuid}` values, binding them to asyncio `contextvars`.
- **Prometheus Metrics (`GET /metrics`)**: Exposes operational counters and histograms for request latencies, task claims, processing outcomes, and queue depth.
- **Graceful Shutdown Protocol**: On receiving `SIGTERM` or `SIGINT`, FastAPI lifespan context initiates orderly shutdown:
  1. Task Manager stops polling for new tasks.
  2. In-memory `BoundedQueue` drains remaining in-flight tasks up to `shutdown_grace_seconds`.
  3. Database and Redis connection pools are safely disposed.
