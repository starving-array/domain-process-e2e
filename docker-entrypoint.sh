#!/bin/bash
set -e

echo "Running database migrations..."
python -m alembic upgrade head

echo "Starting application..."
exec python -m uvicorn src.domain_processing_service.main:app --host 0.0.0.0 --port 8000
