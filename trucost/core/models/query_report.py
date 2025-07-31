from datetime import datetime, timezone
from enum import Enum
from functools import partial

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Integer,
    ForeignKey,
    Enum as SQLEnum,
)
from sqlalchemy.orm import relationship

from trucost.core.models.base import Base
from trucost.core.models.athena_query import AthenaQueryResponse
from trucost.core.models.common.pagination import PaginationMetadata


class QueryExecutionStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AthenaQueryExecution(Base):
    __tablename__ = "athena_query_executions"

    id = Column(Integer, primary_key=True, index=True)
    query_id = Column(Integer, ForeignKey("athena_queries.id"), nullable=False)
    user_id = Column(
        Integer, ForeignKey("users.id"), nullable=False
    )  # User who executed the query
    execution_id = Column(
        String, nullable=False, unique=True
    )  # Athena's query execution ID
    batch_id = Column(String, nullable=True)  # Batch ID if the query is part of a batch
    status = Column(
        SQLEnum(QueryExecutionStatus),
        nullable=False,
        default=QueryExecutionStatus.PENDING,
    )
    executed_query = Column(String, nullable=True)  # Executed query
    error_message = Column(String, nullable=True)  # Error message if the query failed
    created_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        nullable=False,
    )
    updated_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )
    deleted_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        nullable=True,
    )

    # Hacky way to map query-template-assignment to query-execution
    # TODO: have a proper relationship between query-template-assignment and query-execution
    query_template_assignment_id = Column(String, nullable=True)

    # Add relationships
    user = relationship("User", back_populates="query_executions")
    query = relationship("AthenaQuery", back_populates="executions")

    # User settings
    user_settings_id = Column(Integer, ForeignKey("user_settings.id"), nullable=False)
    user_settings = relationship(
        "UserSettings",
        back_populates="query_executions",
    )


class AthenaQueryExecutionCreate(BaseModel):
    user_settings_id: int
    query_id: int
    years: list[int]
    months: list[int]

    def to_dict(self) -> dict:
        return {
            "user_settings_id": self.user_settings_id,
            "query_id": self.query_id,
            "years": self.years,
            "months": self.months,
        }


class AthenaQueryExecutionCreateBatch(BaseModel):
    user_settings_id: int
    years: list[int]
    months: list[int]
    query_ids: list[int] | None = None
    all_queries: bool = False
    category: str = "Cost Optimization"

    def to_dict(self) -> dict:
        return {
            "user_settings_id": self.user_settings_id,
            "all_queries": self.all_queries,
        }


class QueryExecutionBatchDashboard(BaseModel):
    query_id: int
    query_template_assignment_id: str
    years: list[int]
    months: list[int]


class AthenaQueryExecutionCreateBatchDashboard(BaseModel):
    user_settings_id: int
    queries: list[QueryExecutionBatchDashboard]


class AthenaQueryExecutionCreateWithUser(BaseModel):
    query_id: int
    user_id: int
    execution_id: str
    user_settings_id: int
    batch_id: str | None = None
    error_message: str | None = None
    executed_query: str | None = None

    # TODO: have a proper relationship between query-template-assignment and query-execution
    query_template_assignment_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "query_id": self.query_id,
            "user_id": self.user_id,
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "user_settings_id": self.user_settings_id,
            "error_message": self.error_message,
            "executed_query": self.executed_query,
            "query_template_assignment_id": self.query_template_assignment_id,
        }


class AthenaQueryExecutionUpdate(BaseModel):
    id: int
    status: QueryExecutionStatus
    error_message: str | None = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "error_message": self.error_message,
            "updated_at": datetime.now(timezone.utc),
        }


class AthenaQueryExecutionResponse(BaseModel):
    id: int
    query_id: int
    user_id: int
    execution_id: str
    batch_id: str | None
    status: QueryExecutionStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    user_settings_id: int

    class Config:
        from_attributes = True


class UserSettingsResponse(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True

    def __repr__(self) -> str:
        return f"<UserSettings(id={self.id}, name={self.name})>"


class AthenaQueryExecutionResponseWithQuery(AthenaQueryExecutionResponse):
    """
    Response for a query execution with the query details
    """

    executed_query: str | None = None
    query: AthenaQueryResponse | None = None
    user_settings: UserSettingsResponse | None = None

    class Config:
        from_attributes = True


class AthenaQueryExecutionList(BaseModel):
    executions: list[AthenaQueryExecutionResponse]

    class Config:
        from_attributes = True


class AthenaQueryExecutionPagination(BaseModel):
    data: list[AthenaQueryExecutionResponseWithQuery]
    pagination: PaginationMetadata

    class Config:
        from_attributes = True
