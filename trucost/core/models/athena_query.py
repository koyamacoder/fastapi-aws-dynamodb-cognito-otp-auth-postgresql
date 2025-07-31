import hashlib
from datetime import datetime, timezone
from functools import partial

import sqlglot
from sqlglot.expressions import (
    Select as SqlglotSelect,
    Column as SqlglotColumn,
)
from pydantic import BaseModel
from sqlalchemy import Column, String, DateTime, Integer, UniqueConstraint, JSON
from sqlalchemy.orm import relationship

from trucost.core.models.base import Base
from trucost.core.models.common.pagination import PaginationMetadata


class AthenaQuery(Base):
    __tablename__ = "athena_queries"

    id = Column(Integer, primary_key=True, index=True)
    query = Column(String)
    category = Column(String)
    category_type = Column(String)
    query_type = Column(String)
    query_subtype = Column(String)
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
    query_hash = Column(String, nullable=False, unique=True)
    query_metadata = Column(JSON, nullable=True)

    # Add relationship to query executions
    executions = relationship(
        "AthenaQueryExecution", back_populates="query", cascade="all, delete-orphan"
    )

    # Add relationship to templates
    templates_assigned = relationship(
        "QueryTemplateAssignment",
        back_populates="query",
        cascade="all, delete-orphan",
        uselist=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "category",
            "category_type",
            "query_type",
            "query_subtype",
            name="uq_athena_query_combination",
        ),
    )

    @staticmethod
    def get_query_hash(query: str) -> str:
        return hashlib.sha256(query.encode()).hexdigest()

    @staticmethod
    def get_top_level_select_columns(sql: str) -> list[str]:
        # Parse the query
        sql = (
            sql.replace("${table_name}$", "table_name")
            .replace("${year}$", "2024")
            .replace("${month}$", "01")
        )

        parsed = sqlglot.parse_one(sql)

        # Find only the top-level SELECT (ignore subqueries)
        top_level_select = parsed.find(SqlglotSelect)

        # Extract column aliases or names
        columns = []
        for expr in top_level_select.expressions:
            alias = expr.alias
            if alias:
                columns.append(alias)
            elif isinstance(expr, SqlglotColumn):
                columns.append(expr.name)
        return columns


class AthenaQueryCreate(BaseModel):
    query: str
    category: str
    category_type: str
    query_type: str
    query_subtype: str

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "query_hash": AthenaQuery.get_query_hash(self.query),
            "category": self.category,
            "category_type": self.category_type,
            "query_type": self.query_type,
            "query_subtype": self.query_subtype,
        }


class AthenaQueryUpdate(BaseModel):
    id: int
    query: str

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "query_hash": AthenaQuery.get_query_hash(self.query),
        }


class AthenaQueryDbUpdate(BaseModel):
    query: str
    updated_at: datetime | None = None

    def __init__(self, **data):
        super().__init__(**data)
        self.updated_at = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "query_hash": AthenaQuery.get_query_hash(self.query),
            "updated_at": self.updated_at,
        }


class AthenaQueryResponse(BaseModel):
    id: int
    query: str
    category: str
    category_type: str
    query_type: str
    query_subtype: str
    created_at: datetime
    updated_at: datetime
    query_metadata: list[str]

    class Config:
        from_attributes = True

    def __repr__(self) -> str:
        return f"<AthenaQuery(id={self.id}, query={self.query} category={self.category} category_type={self.category_type} query_type={self.query_type} query_subtype={self.query_subtype})>"


class AthenaQueryDelete(BaseModel):
    id: int


class AthenaQueryList(BaseModel):
    queries: list[AthenaQueryResponse]

    class Config:
        from_attributes = True


class AthenaQueryPagination(BaseModel):
    data: list[AthenaQueryResponse]
    pagination: PaginationMetadata

    class Config:
        from_attributes = True
