from typing import List, Tuple

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func

from trucost.core.models.query_report import (
    AthenaQueryExecution,
    AthenaQueryExecutionCreateWithUser,
    QueryExecutionStatus,
)

from trucost.core.services.base import BaseService

import logging

logger = logging.getLogger(__name__)


class QueryExecutionNotFoundError(Exception):
    """Exception raised when a query execution is not found"""


class QueryReportRepository(BaseService):
    """Repository for query execution-related database operations"""

    async def get_by_id(
        self, session: AsyncSession, id: int, user_id: int
    ) -> AthenaQueryExecution | None:
        """Get a query execution by id"""
        result = await session.execute(
            select(AthenaQueryExecution).filter(
                AthenaQueryExecution.id == id,
                AthenaQueryExecution.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_by_execution_id(
        self, session: AsyncSession, execution_id: str, user_id: int
    ) -> AthenaQueryExecution | None:
        """Get a query execution by Athena execution id"""
        result = await session.execute(
            select(AthenaQueryExecution)
            .options(
                selectinload(AthenaQueryExecution.query),
                selectinload(AthenaQueryExecution.user_settings),
            )
            .filter(
                AthenaQueryExecution.execution_id == execution_id,
                AthenaQueryExecution.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_by_template_assignment_id(
        self, session: AsyncSession, template_assignment_id: str, user_id: int
    ) -> AthenaQueryExecution | None:
        """Get a query execution by template assignment id"""
        result = await session.execute(
            select(AthenaQueryExecution)
            .options(
                selectinload(AthenaQueryExecution.query),
                selectinload(AthenaQueryExecution.user_settings),
            )
            .filter(
                AthenaQueryExecution.query_template_assignment_id
                == template_assignment_id,
                AthenaQueryExecution.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_by_execution_ids(
        self, session: AsyncSession, execution_ids: List[str], user_id: int
    ) -> List[AthenaQueryExecution]:
        """Get a query execution by Athena execution ids"""
        result = await session.execute(
            select(AthenaQueryExecution).filter(
                AthenaQueryExecution.execution_id.in_(execution_ids),
                AthenaQueryExecution.user_id == user_id,
            )
        )
        return result.scalars().all()

    async def create(
        self,
        session: AsyncSession,
        execution: AthenaQueryExecutionCreateWithUser,
    ) -> AthenaQueryExecution:
        """Create a new query execution"""

        status = (
            QueryExecutionStatus.FAILED
            if execution.error_message
            else QueryExecutionStatus.PENDING
        )

        query_execution = AthenaQueryExecution(**execution.to_dict(), status=status)

        session.add(query_execution)
        await session.commit()
        await session.refresh(query_execution)
        return query_execution

    async def create_many(
        self,
        session: AsyncSession,
        executions: List[AthenaQueryExecutionCreateWithUser],
    ) -> List[AthenaQueryExecution]:
        """Create a new query executions"""
        query_executions = [
            AthenaQueryExecution(
                **execution.to_dict(),
                status=(
                    QueryExecutionStatus.FAILED
                    if execution.error_message
                    else QueryExecutionStatus.PENDING
                ),
            )
            for execution in executions
        ]

        session.add_all(query_executions)
        await session.commit()

        # Refresh each execution individually
        for execution in query_executions:
            await session.refresh(execution)

        return query_executions

    async def list_by_user(
        self, session: AsyncSession, user_id: int, offset: int = 0, limit: int = 100
    ) -> Tuple[List[AthenaQueryExecution], int]:
        """List all query executions for a specific user"""

        # Get total count
        count_result = await session.execute(
            select(func.count())
            .select_from(AthenaQueryExecution)
            .filter(AthenaQueryExecution.user_id == user_id)
        )
        total = count_result.scalar_one()

        # Get paginated results
        result = await session.execute(
            select(AthenaQueryExecution)
            .options(
                selectinload(AthenaQueryExecution.query),
                selectinload(AthenaQueryExecution.user_settings),
            )
            .filter(AthenaQueryExecution.user_id == user_id)
            .order_by(AthenaQueryExecution.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return result.scalars().all(), total

    async def list_by_query(
        self,
        session: AsyncSession,
        query_id: int,
        user_id: int,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[AthenaQueryExecution], int]:
        """List all executions of a specific query"""

        # Get total count
        count_result = await session.execute(
            select(func.count())
            .select_from(AthenaQueryExecution)
            .filter(
                AthenaQueryExecution.query_id == query_id,
                AthenaQueryExecution.user_id == user_id,
            )
        )
        total = count_result.scalar_one()

        result = await session.execute(
            select(AthenaQueryExecution)
            .filter(
                AthenaQueryExecution.query_id == query_id,
                AthenaQueryExecution.user_id == user_id,
            )
            .order_by(AthenaQueryExecution.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return result.scalars().all(), total

    async def get_by_batch_id(
        self, session: AsyncSession, batch_id: str, user_id: int
    ) -> List[AthenaQueryExecution]:
        """Get a query execution by batch id"""
        result = await session.execute(
            select(AthenaQueryExecution)
            .options(selectinload(AthenaQueryExecution.query))
            .filter(
                AthenaQueryExecution.batch_id == batch_id,
                AthenaQueryExecution.user_id == user_id,
            )
        )

        return result.scalars().all()
