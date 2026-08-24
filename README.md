# Domain Processing Service

An asynchronous domain-processing service that accepts batches of domains, normalizes them, and schedules them for background processing (DNS resolution, HTTP checks).

## Architecture & Components

The application is built using a highly concurrent, async-first architecture:
- **FastAPI**: Provides high-performance async REST APIs.
- **SQLAlchemy (asyncpg)**: Manages database interactions, including transactions, pessimistic locking (`SKIP LOCKED`), and Optimistic Concurrency Control (OCC).
- **PostgreSQL**: The primary datastore for Jobs, Tasks, Domains, and Idempotency records.
- **Redis**: Used for distributed domain locking to coordinate processing across concurrent workers.
- **Worker Pool & Task Manager**: In-memory async worker pool that safely dequeues and processes tasks with backpressure support and graceful shutdown.
- **Idempotency Engine**: Guarantees safe retries via `Idempotency-Key` and `X-Client-ID` headers.

## Technology Stack

- **Python**: 3.10+
- **Framework**: FastAPI
- **Database**: PostgreSQL 15, SQLAlchemy (asyncpg)
- **Cache/Locking**: Redis 7, redis-py
- **Validation**: Pydantic v2
- **Testing**: Pytest, HTTPX

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- `pip` (or `poetry`/`uv` for dependency management)

## Local Setup & Configuration

1. **Clone the repository.**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Environment Variables:**
   Configuration is managed via environment variables with a `DOMAIN_PROCESSING_` prefix. Key variables include:
   - `DOMAIN_PROCESSING_DATABASE_URL` (default: `postgresql+asyncpg://user:password@localhost:5432/domain_processing`)
   - `DOMAIN_PROCESSING_REDIS_HOST` (default: `localhost`)
   - `DOMAIN_PROCESSING_WORKER_CONCURRENCY` (default: `50`)

## Docker Setup

Start the required infrastructure (PostgreSQL and Redis) using Docker Compose:

```bash
docker compose up -d
```

This will spin up:
- **db**: PostgreSQL 15 on port `5432`.
- **redis**: Redis 7 on port `6379`.

*Note: The application itself runs locally on your host machine, connecting to these containerized services.*

## Running the Application

Before starting the app, ensure database migrations have been applied:
```bash
alembic upgrade head
```

Start the FastAPI application (which automatically starts the background Worker Pool and Task Manager):
```bash
uvicorn domain_processing_service.main:app --host 0.0.0.0 --port 8000
```

## Available API Endpoints

### Core Endpoints

#### Submit a Job
`POST /jobs`
Accepts a list of domains for asynchronous processing.
**Headers:**
- `Idempotency-Key` (optional, for safe retries)
- `X-Client-ID` (optional, scopes the idempotency key)

**Body:**
```json
{
  "domains": ["example.com", "google.com"]
}
```

**Response (202 Accepted / 200 OK):**
```json
{
  "jobId": "uuid",
  "status": "PENDING"
}
```

#### Get Job Status
`GET /jobs/{job_id}`
Retrieves job status, summary counts, and paginated task results.
**Query Parameters:**
- `limit` (default: 100)
- `cursor` (optional, for pagination)

### Observability Endpoints

- `GET /health/live`: Liveness probe (returns 200 if app is running).
- `GET /health/ready`: Readiness probe (verifies DB connection).
- `GET /metrics`: Prometheus metrics endpoint.

## Running the Test Suite

The test suite validates integration, concurrency, idempotency, and graceful shutdown behavior.

```bash
python -m pytest tests/test_phase14.py -v
```