"""Domain repository for data access."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain_processing_service.models import Domain, DomainDetail


class DomainRepository:
    """Repository for Domain persistence operations."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, domain: Domain) -> Domain:
        """Insert a new Domain."""
        self._session.add(domain)
        await self._session.flush()
        return domain

    async def get(self, domain_id: uuid.UUID) -> Domain | None:
        """Retrieve a Domain by ID."""
        stmt = select(Domain).where(Domain.id == domain_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_normalized_domain(self, normalized_domain: str) -> Domain | None:
        """Retrieve a Domain by its normalized domain name."""
        stmt = select(Domain).where(Domain.normalized_domain == normalized_domain)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_normalized_domains(
        self, normalized_domains: list[str]
    ) -> list[Domain]:
        """Retrieve multiple Domains by their normalized domain names in one bulk query."""
        if not normalized_domains:
            return []
        stmt = select(Domain).where(Domain.normalized_domain.in_(normalized_domains))
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def create_batch(self, domains: list[Domain]) -> list[Domain]:
        """Insert multiple Domains in a single batch."""
        if not domains:
            return []
        self._session.add_all(domains)
        await self._session.flush()
        return domains

    async def get_or_create(self, normalized_domain: str) -> Domain:
        """Get an existing Domain or create a new one."""
        domain = await self.get_by_normalized_domain(normalized_domain)
        if domain is not None:
            return domain

        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain=normalized_domain,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        self._session.add(domain)
        await self._session.flush()
        return domain

    async def update(
        self,
        domain_id: uuid.UUID,
        *,
        is_active: bool | None = None,
        deactivated_at: datetime | None = None,
    ) -> Domain | None:
        """Update Domain fields."""
        domain = await self.get(domain_id)
        if domain is None:
            return None

        if is_active is not None:
            domain.is_active = is_active
        if deactivated_at is not None:
            domain.deactivated_at = deactivated_at

        domain.updated_at = datetime.now(UTC)
        await self._session.flush()
        return domain

    async def get_or_create_with_reactivation(
        self,
        normalized_domain: str,
    ) -> tuple[Domain, bool]:
        """
        Get or create a Domain, reactivating if inactive.

        Returns:
            Tuple of (domain, was_reactivated)
        """
        domain = await self.get_by_normalized_domain(normalized_domain)
        if domain is not None:
            was_reactivated = False
            if not domain.is_active:
                # Reactivate inactive domain
                domain.is_active = True
                domain.deactivated_at = None
                domain.updated_at = datetime.now(UTC)
                await self._session.flush()
                was_reactivated = True
            return domain, was_reactivated

        now = datetime.now(UTC)
        domain = Domain(
            id=uuid.uuid4(),
            normalized_domain=normalized_domain,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        self._session.add(domain)
        await self._session.flush()
        return domain, False

    async def clear_domain_detail(self, domain_id: uuid.UUID) -> None:
        """Delete DomainDetail for a domain (used during reactivation)."""
        from sqlalchemy import delete
        stmt = delete(DomainDetail).where(DomainDetail.domain_id == domain_id)
        await self._session.execute(stmt)
        await self._session.flush()

    async def clear_domain_details_batch(self, domain_ids: list[uuid.UUID]) -> None:
        """Delete DomainDetail records for multiple domains in one query."""
        if not domain_ids:
            return
        from sqlalchemy import delete
        stmt = delete(DomainDetail).where(DomainDetail.domain_id.in_(domain_ids))
        await self._session.execute(stmt)
        await self._session.flush()
