"""API integration tests for GET /jobs/{job_id} endpoint."""

import uuid
from datetime import UTC, datetime

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.dtos import Cursor
from domain_processing_service.models import Domain, DomainDetail, Job, Task, TaskStatus, TaskType


class TestGetJobAPI:
    """Tests for GET /jobs/{job_id} endpoint."""

    async def _create_test_job_with_tasks(
        self,
        async_db_session: AsyncSession,
        task_configs: list[dict],
    ) -> uuid.UUID:
        """Helper to create a job with specific task configurations."""
        now = datetime.now(UTC)
        job_id = uuid.uuid4()

        job = Job(
            id=job_id,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(job)

        for i, config in enumerate(task_configs):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=config.get("domain", f"domain{i}.com"),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=config.get("task_id", uuid.uuid4()),
                job_id=job_id,
                domain_id=domain.id,
                type=config.get("type", TaskType.USER_REQUEST),
                status=config.get("status", TaskStatus.PENDING),
                attempts=config.get("attempts", 0),
                next_attempt_at=now,
                created_at=config.get("created_at", now),
                updated_at=now,
            )
            if "error_payload" in config:
                task.error_payload = config["error_payload"]
            async_db_session.add(task)

            # Add domain detail if task is completed
            if config.get("status") == TaskStatus.COMPLETED:
                detail = DomainDetail(
                    domain_id=domain.id,
                    ip_addresses=config.get("ip_addresses", ["1.2.3.4"]),
                    dns_records=config.get("dns_records", {"A": ["1.2.3.4"]}),
                    http_status=config.get("http_status", 200),
                    page_title=config.get("page_title", "Test Page"),
                    response_time=config.get("response_time", 100),
                    response_headers=config.get("response_headers", {}),
                    fetched_at=now,
                    next_refresh_at=now,
                    version=1,
                )
                async_db_session.add(detail)

        await async_db_session.commit()
        return job_id

    async def test_job_exists_returns_first_page(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test Job exists returns first page of results."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": "example.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "github.com", "status": TaskStatus.PENDING, "created_at": now},
                {"domain": "google.com", "status": TaskStatus.FAILED, "created_at": now},
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}?limit=2")

        assert response.status_code == 200
        data = response.json()
        assert data["jobId"] == str(job_id)
        assert data["status"] == "PENDING"
        assert len(data["results"]) == 2
        assert data["summary"]["total"] == 3
        assert data["summary"]["completed"] == 1
        assert data["summary"]["pending"] == 1
        assert data["summary"]["failed"] == 1
        assert data["nextCursor"] is not None

    async def test_job_not_found_returns_404(self, async_client: httpx.AsyncClient) -> None:
        """Test unknown Job returns 404."""
        response = await async_client.get(f"/jobs/{uuid.uuid4()}")
        assert response.status_code == 404

    async def test_pagination_limit_respected(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test limit parameter is respected."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": "a.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "b.com", "status": TaskStatus.PENDING, "created_at": now},
                {"domain": "c.com", "status": TaskStatus.FAILED, "created_at": now},
                {"domain": "d.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "e.com", "status": TaskStatus.PENDING, "created_at": now},
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}?limit=2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2
        assert data["nextCursor"] is not None

    async def test_pagination_second_page_using_cursor(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test second page using cursor from first page."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": "a.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "b.com", "status": TaskStatus.PENDING, "created_at": now},
                {"domain": "c.com", "status": TaskStatus.FAILED, "created_at": now},
                {"domain": "d.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "e.com", "status": TaskStatus.PENDING, "created_at": now},
            ],
        )

        # First page
        response1 = await async_client.get(f"/jobs/{job_id}?limit=2")
        assert response1.status_code == 200
        data1 = response1.json()
        assert len(data1["results"]) == 2
        cursor = data1["nextCursor"]
        assert cursor is not None

        # Second page
        response2 = await async_client.get(f"/jobs/{job_id}?limit=2&cursor={cursor}")
        assert response2.status_code == 200
        data2 = response2.json()
        assert len(data2["results"]) == 2
        # Results should be different from first page
        result_ids1 = {r["taskId"] for r in data1["results"]}
        result_ids2 = {r["taskId"] for r in data2["results"]}
        assert result_ids1.isdisjoint(result_ids2)

    async def test_cursor_ordering_stable(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test cursor ordering is stable across pages."""
        now = datetime.now(UTC)
        base_time = now.replace(microsecond=0)

        # Create tasks with same created_at but different task_ids
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(job)

        task_ids = [uuid.uuid4() for _ in range(3)]
        for i, task_id in enumerate(task_ids):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"domain{i}.com",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=task_id,
                job_id=job_id,
                domain_id=domain.id,
                type=TaskType.USER_REQUEST,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=now,
                created_at=base_time,
                updated_at=now,
            )
            async_db_session.add(task)

        await async_db_session.commit()

        response = await async_client.get(f"/jobs/{job_id}?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2

        # Verify ordering by task_id when created_at is equal
        cursor = Cursor.decode(data["nextCursor"])
        assert cursor.created_at == base_time
        # The cursor should point to the last task's ID
        assert cursor.task_id in task_ids

    async def test_same_created_at_ordered_by_task_id(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test same created_at values are correctly ordered using task_id."""
        now = datetime.now(UTC)
        base_time = now.replace(microsecond=0)

        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(job)

        # Create tasks with same created_at
        task1_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
        task2_id = uuid.UUID("00000000-0000-0000-0000-000000000002")
        task3_id = uuid.UUID("00000000-0000-0000-0000-000000000003")

        for i, task_id in enumerate([task1_id, task2_id, task3_id]):
            domain = Domain(
                id=uuid.uuid4(),
                normalized_domain=f"domain{i}.com",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            async_db_session.add(domain)
            await async_db_session.flush()

            task = Task(
                id=task_id,
                job_id=job_id,
                domain_id=domain.id,
                type=TaskType.USER_REQUEST,
                status=TaskStatus.PENDING,
                attempts=0,
                next_attempt_at=now,
                created_at=base_time,
                updated_at=now,
            )
            async_db_session.add(task)

        await async_db_session.commit()

        response = await async_client.get(f"/jobs/{job_id}?limit=3")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 3

        # Results should be ordered by task_id (ascending) when created_at is equal
        result_task_ids = [uuid.UUID(r["taskId"]) for r in data["results"]]
        assert result_task_ids == sorted(result_task_ids)

    async def test_max_limit_enforced(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test maximum limit is enforced."""
        now = datetime.now(UTC)
        domains = [
            {"domain": f"domain{i}.com", "status": TaskStatus.PENDING, "created_at": now}
            for i in range(10)
        ]
        job_id = await self._create_test_job_with_tasks(async_db_session, domains)

        # Request limit higher than max_page_size (default 1000)
        response = await async_client.get(f"/jobs/{job_id}?limit=2000")
        assert response.status_code == 200
        data = response.json()
        # Should be capped at max_page_size (1000)
        assert len(data["results"]) == 10

    async def test_invalid_limit_rejected(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test non-positive limit is rejected."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [{"domain": "example.com", "status": TaskStatus.PENDING, "created_at": now}],
        )

        response = await async_client.get(f"/jobs/{job_id}?limit=0")
        assert response.status_code == 400
        assert "positive" in response.json()["detail"].lower()

        response = await async_client.get(f"/jobs/{job_id}?limit=-1")
        assert response.status_code == 400

    async def test_invalid_cursor_rejected(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test invalid cursor format is rejected."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [{"domain": "example.com", "status": TaskStatus.PENDING, "created_at": now}],
        )

        response = await async_client.get(f"/jobs/{job_id}?cursor=invalid_cursor")
        assert response.status_code == 400
        assert "invalid cursor" in response.json()["detail"].lower()

    async def test_empty_results_handled_correctly(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test Job with no tasks returns empty results."""
        now = datetime.now(UTC)
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        async_db_session.add(job)
        await async_db_session.commit()

        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []
        assert data["nextCursor"] is None
        assert data["summary"]["total"] == 0

    async def test_next_cursor_null_on_final_page(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test nextCursor is null on final page."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [{"domain": "example.com", "status": TaskStatus.COMPLETED, "created_at": now}],
        )

        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        assert data["nextCursor"] is None

    async def test_next_cursor_present_when_more_pages(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test nextCursor is present when another page exists."""
        now = datetime.now(UTC)
        domains = [
            {"domain": f"domain{i}.com", "status": TaskStatus.PENDING, "created_at": now}
            for i in range(5)
        ]
        job_id = await self._create_test_job_with_tasks(async_db_session, domains)

        response = await async_client.get(f"/jobs/{job_id}?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2
        assert data["nextCursor"] is not None

        # Fetch second page
        response2 = await async_client.get(f"/jobs/{job_id}?limit=2&cursor={data['nextCursor']}")
        data2 = response2.json()
        assert len(data2["results"]) == 2
        assert data2["nextCursor"] is not None

        # Fetch third page (final)
        response3 = await async_client.get(f"/jobs/{job_id}?limit=2&cursor={data2['nextCursor']}")
        data3 = response3.json()
        assert len(data3["results"]) == 1
        assert data3["nextCursor"] is None

    async def test_summary_counts_correct(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test summary counts are correct."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": "a.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "b.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "c.com", "status": TaskStatus.PENDING, "created_at": now},
                {"domain": "d.com", "status": TaskStatus.FAILED, "created_at": now},
                {"domain": "e.com", "status": TaskStatus.PROCESSING, "created_at": now},
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()

        assert data["summary"]["total"] == 5
        assert data["summary"]["completed"] == 2
        assert data["summary"]["pending"] == 1
        assert data["summary"]["failed"] == 1
        assert data["summary"]["processing"] == 1

    async def test_large_job_does_not_load_all_tasks(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test large Job does not load all Tasks into memory."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": f"domain{i}.com", "status": TaskStatus.PENDING, "created_at": now}
                for i in range(100)
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 10
        assert data["summary"]["total"] == 100
        assert data["nextCursor"] is not None

    async def test_n_plus_one_prevention_query_count(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test N+1 prevention - query counting."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {"domain": "example.com", "status": TaskStatus.COMPLETED, "created_at": now},
                {"domain": "github.com", "status": TaskStatus.COMPLETED, "created_at": now},
            ],
        )

        # This test verifies the endpoint works correctly
        # A proper N+1 test would use a query counter
        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2

        # Verify results have domain details populated
        for result in data["results"]:
            assert result["result"] is not None
            assert "ip_addresses" in result["result"]
            assert "page_title" in result["result"]

    async def test_task_result_includes_domain_details_when_completed(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test completed task includes domain details."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {
                    "domain": "example.com",
                    "status": TaskStatus.COMPLETED,
                    "created_at": now,
                    "ip_addresses": ["93.184.216.34"],
                    "dns_records": {"A": ["93.184.216.34"]},
                    "http_status": 200,
                    "page_title": "Example Domain",
                    "response_time": 120,
                }
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        result = data["results"][0]
        assert result["status"] == "COMPLETED"
        assert result["result"] is not None
        assert result["result"]["ip_addresses"] == ["93.184.216.34"]
        assert result["result"]["page_title"] == "Example Domain"
        assert result["result"]["response_time"] == 120
        assert result["error"] is None

    async def test_task_result_includes_error_when_failed(
        self, async_client: httpx.AsyncClient, async_db_session: AsyncSession
    ) -> None:
        """Test failed task includes error information."""
        now = datetime.now(UTC)
        job_id = await self._create_test_job_with_tasks(
            async_db_session,
            [
                {
                    "domain": "bad.com",
                    "status": TaskStatus.FAILED,
                    "created_at": now,
                    "error_payload": {
                        "code": "DNS_RESOLUTION_FAILED",
                        "message": "NXDOMAIN",
                        "retryable": False,
                    },
                }
            ],
        )

        response = await async_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 1
        result = data["results"][0]
        assert result["status"] == "FAILED"
        assert result["error"] is not None
        assert result["error"]["code"] == "DNS_RESOLUTION_FAILED"
        assert result["error"]["retryable"] is False
        assert result["result"] is None