"""Data Transfer Objects for API boundaries."""

import base64
import json
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class CreateJobRequest(BaseModel):
    """Request DTO for POST /jobs."""

    domains: list[str] = Field(
        default_factory=list,
        description="List of domains to process",
    )


class CreateJobResponse(BaseModel):
    """Response DTO for POST /jobs."""

    jobId: UUID = Field(..., description="Unique identifier for the created job")
    status: str = Field(..., description="Job status")

    class Config:
        from_attributes = True


class ErrorDTO(BaseModel):
    """Error DTO for task-level errors."""

    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Human-readable error message")
    retryable: bool = Field(..., description="Whether the error is retryable")


class TaskResultDTO(BaseModel):
    """DTO for a single task result in GET /jobs/{job_id}."""

    taskId: UUID = Field(..., description="Unique identifier for the task")
    domain: str = Field(..., description="Normalized domain name")
    status: str = Field(..., description="Task status")
    result: dict[str, Any] | None = Field(None, description="Task result data when completed")
    error: ErrorDTO | None = Field(None, description="Error information when failed")


class JobSummaryDTO(BaseModel):
    """Summary counts for a job."""

    total: int = Field(..., description="Total number of tasks")
    completed: int = Field(..., description="Number of completed tasks")
    failed: int = Field(..., description="Number of failed tasks")
    pending: int = Field(..., description="Number of pending tasks")
    processing: int = Field(..., description="Number of processing tasks")


class JobResponse(BaseModel):
    """Response DTO for GET /jobs/{job_id}."""

    jobId: UUID = Field(..., description="Unique identifier for the job")
    status: str = Field(..., description="Job status")
    summary: JobSummaryDTO = Field(..., description="Task summary counts")
    results: list[TaskResultDTO] = Field(..., description="Paginated task results")
    nextCursor: str | None = Field(None, description="Opaque cursor for next page")


class Cursor(BaseModel):
    """Internal cursor representation for pagination."""

    created_at: datetime
    task_id: UUID

    def encode(self) -> str:
        """Encode cursor to opaque base64 string."""
        data = {
            "created_at": self.created_at.isoformat(),
            "task_id": str(self.task_id),
        }
        json_data = json.dumps(data, separators=(",", ":"))
        return base64.urlsafe_b64encode(json_data.encode()).decode()

    @classmethod
    def decode(cls, cursor: str) -> "Cursor":
        """Decode opaque cursor string to Cursor object."""
        try:
            json_data = base64.urlsafe_b64decode(cursor.encode()).decode()
            data = json.loads(json_data)
            return cls(
                created_at=datetime.fromisoformat(data["created_at"]),
                task_id=UUID(data["task_id"]),
            )
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            raise ValueError("Invalid cursor format") from e