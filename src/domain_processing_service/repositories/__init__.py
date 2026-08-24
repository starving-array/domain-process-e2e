"""Repository layer for data access."""

from domain_processing_service.repositories.domain import DomainRepository
from domain_processing_service.repositories.domain_detail import DomainDetailRepository
from domain_processing_service.repositories.idempotency import IdempotencyRecordRepository
from domain_processing_service.repositories.job import JobRepository
from domain_processing_service.repositories.task import TaskRepository

__all__ = [
    "JobRepository",
    "TaskRepository",
    "DomainRepository",
    "DomainDetailRepository",
    "IdempotencyRecordRepository",
]