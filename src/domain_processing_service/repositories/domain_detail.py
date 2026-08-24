"""DomainDetail repository for data access."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.models import DomainDetail


class DomainDetailRepository:
    """Repository for DomainDetail persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, detail: DomainDetail) -> DomainDetail:
        """Insert a new DomainDetail."""
        self._session.add(detail)
        await self._session.flush()
        return detail

    async def get(self, domain_id: uuid.UUID) -> DomainDetail | None:
        """Retrieve a DomainDetail by domain_id."""
        stmt = select(DomainDetail).where(DomainDetail.domain_id == domain_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert(self, detail: DomainDetail) -> DomainDetail:
        """
        Insert or update a DomainDetail.

        Uses ON CONFLICT DO UPDATE to atomically upsert.
        """
        from sqlalchemy.dialects.postgresql import insert

        stmt = insert(DomainDetail).values(
            domain_id=detail.domain_id,
            ip_addresses=detail.ip_addresses,
            dns_records=detail.dns_records,
            http_status=detail.http_status,
            page_title=detail.page_title,
            response_time=detail.response_time,
            response_headers=detail.response_headers,
            fetched_at=detail.fetched_at,
            next_refresh_at=detail.next_refresh_at,
            version=detail.version,
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["domain_id"],
            set_={
                "ip_addresses": stmt.excluded.ip_addresses,
                "dns_records": stmt.excluded.dns_records,
                "http_status": stmt.excluded.http_status,
                "page_title": stmt.excluded.page_title,
                "response_time": stmt.excluded.response_time,
                "response_headers": stmt.excluded.response_headers,
                "fetched_at": stmt.excluded.fetched_at,
                "next_refresh_at": stmt.excluded.next_refresh_at,
                "version": stmt.excluded.version,
            },
        )

        await self._session.execute(stmt)
        await self._session.flush()

        # Return the upserted detail
        upserted = await self.get(detail.domain_id)
        if upserted is None:
            raise RuntimeError("DomainDetail not found after upsert")
        return upserted

    async def upsert_with_occ(
        self,
        detail: DomainDetail,
        expected_version: int,
    ) -> DomainDetail:
        """
        Insert or update a DomainDetail with Optimistic Concurrency Control.

        Uses version check to prevent stale concurrent writes.
        Only updates if the current version matches expected_version.

        Args:
            detail: DomainDetail with new values to persist
            expected_version: The version number that must match for update to proceed

        Returns:
            The updated DomainDetail

        Raises:
            RuntimeError: If OCC conflict (0 rows updated) or record not found
        """
        from sqlalchemy import update
        from sqlalchemy.dialects.postgresql import insert

        # Try to update with version check first
        stmt = (
            update(DomainDetail)
            .where(
                DomainDetail.domain_id == detail.domain_id,
                DomainDetail.version == expected_version,
            )
            .values(
                ip_addresses=detail.ip_addresses,
                dns_records=detail.dns_records,
                http_status=detail.http_status,
                page_title=detail.page_title,
                response_time=detail.response_time,
                response_headers=detail.response_headers,
                fetched_at=detail.fetched_at,
                next_refresh_at=detail.next_refresh_at,
                version=expected_version + 1,
            )
        )

        result = await self._session.execute(stmt)
        await self._session.flush()

        if result.rowcount == 0:  # type: ignore[attr-defined]
            # Check if record exists at all
            existing = await self.get(detail.domain_id)
            if existing is None:
                # Record doesn't exist - try insert (first time)
                insert_stmt = insert(DomainDetail).values(
                    domain_id=detail.domain_id,
                    ip_addresses=detail.ip_addresses,
                    dns_records=detail.dns_records,
                    http_status=detail.http_status,
                    page_title=detail.page_title,
                    response_time=detail.response_time,
                    response_headers=detail.response_headers,
                    fetched_at=detail.fetched_at,
                    next_refresh_at=detail.next_refresh_at,
                    version=1,  # First version is 1
                )
                await self._session.execute(insert_stmt)
                await self._session.flush()
            else:
                # Record exists but version mismatch - OCC conflict
                raise RuntimeError(
                    f"OCC conflict: expected version {expected_version}, "
                    f"found version {existing.version}"
                )
        # Else: update succeeded (1 row affected)

        # Return the upserted detail
        upserted = await self.get(detail.domain_id)
        if upserted is None:
            raise RuntimeError("DomainDetail not found after upsert_with_occ")
        return upserted

    async def get_stale_domains(
        self,
        limit: int,
        now: datetime | None = None,
    ) -> list[DomainDetail]:
        """Retrieve DomainDetails that are due for refresh."""
        if now is None:
            now = datetime.now(UTC)

        stmt = (
            select(DomainDetail)
            .where(DomainDetail.next_refresh_at <= now)
            .order_by(DomainDetail.next_refresh_at)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_domains_needing_refresh(
        self,
        limit: int,
        now: datetime | None = None,
    ) -> list[tuple[uuid.UUID, str]]:
        """
        Retrieve active domains that need refresh, joined with Domain.

        Returns a list of (domain_id, normalized_domain) tuples for domains
        that are active and have next_refresh_at <= now.
        """
        from domain_processing_service.models import Domain

        if now is None:
            now = datetime.now(UTC)

        stmt = (
            select(DomainDetail.domain_id, Domain.normalized_domain)
            .join(Domain, Domain.id == DomainDetail.domain_id)
            .where(
                Domain.is_active.is_(True),
                DomainDetail.next_refresh_at <= now,
            )
            .order_by(DomainDetail.next_refresh_at)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [(row[0], row[1]) for row in result.all()]