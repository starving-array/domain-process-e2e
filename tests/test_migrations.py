import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

from alembic import command

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/domain_processing"
REQUIRED_TABLES = {"job", "task", "domain", "domain_detail", "idempotency_record"}
REQUIRED_INDEXES = {
    "idx_tasks_claim": ("task", ("status", "next_attempt_at", "type"), False),
    "idx_tasks_lease": ("task", ("status", "lease_expires_at"), False),
    "idx_tasks_job_id": ("task", ("job_id",), False),
    "idx_tasks_cursor": ("task", ("job_id", "created_at", "id"), False),
    "idx_domain_normalized": ("domain", ("normalized_domain",), True),
    "idx_domain_detail_refresh": ("domain_detail", ("next_refresh_at",), False),
}


def get_test_database_url() -> str:
    return os.environ.get("DOMAIN_PROCESSING_TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)


def alembic_config() -> Config:
    config = Config(PROJECT_ROOT / "alembic.ini")
    config.set_main_option("sqlalchemy.url", get_test_database_url())
    return config


def reset_phase2_schema() -> None:
    engine = create_engine(get_test_database_url())
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DROP TABLE IF EXISTS
                        idempotency_record,
                        domain_detail,
                        task,
                        domain,
                        job,
                        alembic_version
                    CASCADE
                    """
                )
            )
            connection.execute(text("DROP TYPE IF EXISTS task_type CASCADE"))
            connection.execute(text("DROP TYPE IF EXISTS task_status CASCADE"))
    finally:
        engine.dispose()


@pytest.fixture()
def migration_test_database() -> Iterator[Engine]:
    config = alembic_config()
    reset_phase2_schema()
    command.upgrade(config, "head")
    try:
        engine = create_engine(get_test_database_url())
        try:
            yield engine
        finally:
            engine.dispose()
    finally:
        # Re-upgrade to leave database in migrated state for subsequent tests
        command.upgrade(config, "head")


def fetch_all(
    engine: Engine,
    statement: str,
    params: dict[str, object] | None = None,
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        result = connection.execute(text(statement), params or {})
        return [dict(row) for row in result.mappings().all()]


def test_fresh_migration_creates_required_tables(migration_test_database: Engine) -> None:
    rows = fetch_all(
        migration_test_database,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name != 'alembic_version'
        """,
    )

    assert {row["table_name"] for row in rows} == REQUIRED_TABLES


def test_required_enums_exist(migration_test_database: Engine) -> None:
    rows = fetch_all(
        migration_test_database,
        """
        SELECT t.typname, e.enumlabel
        FROM pg_type t
        JOIN pg_enum e ON e.enumtypid = t.oid
        WHERE t.typname IN ('task_status', 'task_type')
        ORDER BY t.typname, e.enumsortorder
        """,
    )

    enum_values: dict[str, list[str]] = {}
    for row in rows:
        enum_values.setdefault(str(row["typname"]), []).append(str(row["enumlabel"]))

    assert enum_values == {
        "task_status": ["PENDING", "PROCESSING", "COMPLETED", "FAILED"],
        "task_type": ["USER_REQUEST", "REFRESH"],
    }


def test_required_columns_and_nullability_exist(migration_test_database: Engine) -> None:
    rows = fetch_all(
        migration_test_database,
        """
        SELECT table_name, column_name, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('job', 'task', 'domain', 'domain_detail', 'idempotency_record')
        """,
    )
    columns = {(row["table_name"], row["column_name"]): row for row in rows}

    expected_columns = {
        ("job", "id"),
        ("job", "status"),
        ("job", "created_at"),
        ("job", "updated_at"),
        ("task", "id"),
        ("task", "job_id"),
        ("task", "domain_id"),
        ("task", "type"),
        ("task", "status"),
        ("task", "attempts"),
        ("task", "next_attempt_at"),
        ("task", "lease_expires_at"),
        ("task", "error_payload"),
        ("task", "created_at"),
        ("task", "updated_at"),
        ("domain", "id"),
        ("domain", "normalized_domain"),
        ("domain", "is_active"),
        ("domain", "deactivated_at"),
        ("domain", "created_at"),
        ("domain", "updated_at"),
        ("domain_detail", "domain_id"),
        ("domain_detail", "ip_addresses"),
        ("domain_detail", "dns_records"),
        ("domain_detail", "http_status"),
        ("domain_detail", "page_title"),
        ("domain_detail", "response_time"),
        ("domain_detail", "response_headers"),
        ("domain_detail", "fetched_at"),
        ("domain_detail", "next_refresh_at"),
        ("domain_detail", "version"),
        ("idempotency_record", "id"),
        ("idempotency_record", "client_id"),
        ("idempotency_record", "idempotency_key"),
        ("idempotency_record", "request_hash"),
        ("idempotency_record", "job_id"),
        ("idempotency_record", "created_at"),
    }

    assert set(columns) == expected_columns
    assert columns[("task", "job_id")]["is_nullable"] == "YES"
    assert columns[("task", "lease_expires_at")]["is_nullable"] == "YES"
    assert columns[("task", "error_payload")]["is_nullable"] == "YES"
    assert columns[("task", "status")]["udt_name"] == "task_status"
    assert columns[("task", "type")]["udt_name"] == "task_type"
    assert columns[("domain_detail", "ip_addresses")]["udt_name"] == "jsonb"
    assert columns[("domain_detail", "dns_records")]["udt_name"] == "jsonb"
    assert columns[("domain_detail", "response_headers")]["udt_name"] == "jsonb"


def test_primary_keys_and_foreign_keys(migration_test_database: Engine) -> None:
    primary_key_rows = fetch_all(
        migration_test_database,
        """
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name IN ('job', 'task', 'domain', 'domain_detail', 'idempotency_record')
        """,
    )
    assert {(row["table_name"], row["column_name"]) for row in primary_key_rows} == {
        ("job", "id"),
        ("task", "id"),
        ("domain", "id"),
        ("domain_detail", "domain_id"),
        ("idempotency_record", "id"),
    }

    foreign_key_rows = fetch_all(
        migration_test_database,
        """
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
          ON rc.constraint_name = tc.constraint_name
         AND rc.constraint_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
        """,
    )

    assert {
        (
            row["table_name"],
            row["column_name"],
            row["foreign_table_name"],
            row["foreign_column_name"],
            row["delete_rule"],
        )
        for row in foreign_key_rows
    } == {
        ("task", "job_id", "job", "id", "RESTRICT"),
        ("task", "domain_id", "domain", "id", "RESTRICT"),
        ("domain_detail", "domain_id", "domain", "id", "RESTRICT"),
        ("idempotency_record", "job_id", "job", "id", "RESTRICT"),
    }


def test_required_unique_constraints_and_indexes(migration_test_database: Engine) -> None:
    index_rows = fetch_all(
        migration_test_database,
        """
        SELECT
            i.relname AS index_name,
            t.relname AS table_name,
            ix.indisunique AS is_unique,
            array_agg(a.attname ORDER BY keys.ordinality) AS column_names
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ordinality) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = keys.attnum
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public'
          AND t.relname IN ('job', 'task', 'domain', 'domain_detail', 'idempotency_record')
          AND i.relname NOT LIKE '%_pkey'
        GROUP BY i.relname, t.relname, ix.indisunique
        """,
    )
    indexes = {
        row["index_name"]: (row["table_name"], tuple(row["column_names"]), row["is_unique"])
        for row in index_rows
    }

    assert indexes == REQUIRED_INDEXES | {
        "uq_idempotency_record_client_id_idempotency_key": (
            "idempotency_record",
            ("client_id", "idempotency_key"),
            True,
        )
    }


def test_uniqueness_and_foreign_key_integrity(migration_test_database: Engine) -> None:
    now = datetime.now(tz=UTC)
    job_id = uuid.uuid4()
    domain_id = uuid.uuid4()

    with migration_test_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO job (id, status, created_at, updated_at)
                VALUES (:id, 'PENDING', :now, :now)
                """
            ),
            {"id": job_id, "now": now},
        )
        connection.execute(
            text(
                """
                INSERT INTO domain (id, normalized_domain, is_active, created_at, updated_at)
                VALUES (:id, 'example.com', true, :now, :now)
                """
            ),
            {"id": domain_id, "now": now},
        )
        connection.execute(
            text(
                """
                INSERT INTO task (
                    id, job_id, domain_id, type, status, attempts,
                    next_attempt_at, created_at, updated_at
                )
                VALUES (
                    :id, NULL, :domain_id, 'REFRESH', 'PENDING', 0,
                    :next_attempt_at, :now, :now
                )
                """
            ),
            {"id": uuid.uuid4(), "domain_id": domain_id, "next_attempt_at": now, "now": now},
        )
        connection.execute(
            text(
                """
                INSERT INTO domain_detail (
                    domain_id, ip_addresses, dns_records, response_headers,
                    fetched_at, next_refresh_at, version
                )
                VALUES (:domain_id, '[]', '{}', '{}', :now, :next_refresh_at, 1)
                """
            ),
            {"domain_id": domain_id, "now": now, "next_refresh_at": now + timedelta(days=14)},
        )
        connection.execute(
            text(
                """
                INSERT INTO idempotency_record (
                    id, client_id, idempotency_key, request_hash, job_id, created_at
                )
                VALUES (:id, 'client', 'key', 'hash', :job_id, :now)
                """
            ),
            {"id": uuid.uuid4(), "job_id": job_id, "now": now},
        )

    duplicate_domain_error = None
    try:
        with migration_test_database.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO domain (id, normalized_domain, is_active, created_at, updated_at)
                    VALUES (:id, 'example.com', true, :now, :now)
                    """
                ),
                {"id": uuid.uuid4(), "now": now},
            )
    except Exception as exc:
        duplicate_domain_error = exc
    assert duplicate_domain_error is not None

    duplicate_idempotency_error = None
    try:
        with migration_test_database.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO idempotency_record (
                        id, client_id, idempotency_key, request_hash, job_id, created_at
                    )
                    VALUES (:id, 'client', 'key', 'hash-2', :job_id, :now)
                    """
                ),
                {"id": uuid.uuid4(), "job_id": job_id, "now": now},
            )
    except Exception as exc:
        duplicate_idempotency_error = exc
    assert duplicate_idempotency_error is not None

    foreign_key_error = None
    try:
        with migration_test_database.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO domain_detail (
                        domain_id, ip_addresses, dns_records, response_headers,
                        fetched_at, next_refresh_at, version
                    )
                    VALUES (:domain_id, '[]', '{}', '{}', :now, :next_refresh_at, 1)
                    """
                ),
                {
                    "domain_id": uuid.uuid4(),
                    "now": now,
                    "next_refresh_at": now + timedelta(days=14),
                },
            )
    except Exception as exc:
        foreign_key_error = exc
    assert foreign_key_error is not None


def test_migration_downgrade_and_fresh_upgrade_again() -> None:
    config = alembic_config()
    reset_phase2_schema()
    command.upgrade(config, "head")
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    engine = create_engine(get_test_database_url())
    try:
        rows = fetch_all(
            engine,
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('job', 'task', 'domain', 'domain_detail', 'idempotency_record')
            """,
        )
    finally:
        engine.dispose()

    assert {row["table_name"] for row in rows} == REQUIRED_TABLES
