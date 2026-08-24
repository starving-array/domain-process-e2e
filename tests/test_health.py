from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker

from domain_processing_service.app import create_app
from domain_processing_service.config import AppSettings


class FakeDatabase:
    def __init__(self, ready: bool = True) -> None:
        self.ready = ready
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def is_ready(self) -> bool:
        return self.ready

    @property
    def session_maker(self) -> async_sessionmaker:
        # Use NullPool to avoid connection attempts
        return async_sessionmaker(
            bind=None,
            expire_on_commit=False,
            class_=None,
        )


def test_health_live_returns_200_and_request_id_header() -> None:
    database = FakeDatabase()
    app = create_app(
        AppSettings(database_url="postgresql+asyncpg://u:p@localhost/db"), 
        database,
        enable_worker_pool=False,
    )

    with TestClient(app) as client:
        response = client.get("/health/live", headers={"X-Request-ID": "req-test"})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "req-test"
    assert response.json() == {
        "status": "live",
        "service": "domain-processing-service",
        "request_id": "req-test",
    }
    assert database.connected is True
    assert database.closed is True


def test_health_ready_returns_200_when_database_is_ready() -> None:
    app = create_app(
        AppSettings(database_url="postgresql+asyncpg://u:p@localhost/db"),
        FakeDatabase(ready=True),
        enable_worker_pool=False,
    )

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["dependencies"] == {"postgresql": "ok"}


def test_health_ready_returns_503_when_database_is_unavailable() -> None:
    app = create_app(
        AppSettings(database_url="postgresql+asyncpg://u:p@localhost/db"),
        FakeDatabase(ready=False),
        enable_worker_pool=False,
    )

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["dependencies"] == {"postgresql": "unavailable"}
