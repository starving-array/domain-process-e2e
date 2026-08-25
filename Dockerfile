FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for asyncpg (PostgreSQL client libs)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy project files (filtered by .dockerignore)
COPY . .

# Install the project with dev dependencies (supports both app and test services)
RUN pip install --no-cache-dir ".[dev]" && \
    chmod +x docker-entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["./docker-entrypoint.sh"]
