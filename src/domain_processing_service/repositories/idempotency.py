"""IdempotencyRecord repository for data access."""
 
 
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
 
from domain_processing_service.models import IdempotencyRecord
 
 
class IdempotencyRecordRepository:
    """Repository for IdempotencyRecord persistence operations."""
 
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
 
    async def create(self, record: IdempotencyRecord) -> IdempotencyRecord:
        """Insert a new IdempotencyRecord."""
        self._session.add(record)
        await self._session.flush()
        return record
 
    async def get(self, client_id: str, idempotency_key: str) -> IdempotencyRecord | None:
        """Retrieve an IdempotencyRecord by client_id and idempotency_key."""
        stmt = select(IdempotencyRecord).where(
            IdempotencyRecord.client_id == client_id,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
 