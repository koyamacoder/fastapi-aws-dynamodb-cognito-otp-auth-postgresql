from typing import List

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from trucost.core.models.athena_query import (
    AthenaQuery,
    AthenaQueryCreate,
    AthenaQueryDelete,
    AthenaQueryUpdate,
    AthenaQueryDbUpdate,
)

from trucost.core.services.base import BaseService
import logging

logger = logging.getLogger(__name__)


class QueryNotFoundError(Exception):
    """Exception raised when a query is not found"""


class QueryAlreadyExistsError(Exception):
    """Exception raised when a query already exists"""


class AthenaQueryRepository(BaseService):
    """
    Repository for athena query creation and deletion
    The queries are used by the query-report service to trigger the athena query execution
    """

    async def get_by_id(self, session: AsyncSession, id: int) -> AthenaQuery | None:
        """Get a athena query by id"""
        result = await session.execute(select(AthenaQuery).filter(AthenaQuery.id == id))
        return result.scalar_one_or_none()

    async def get_by_query_hash(
        self, session: AsyncSession, query_hash: str
    ) -> AthenaQuery | None:
        """Get a athena query by query hash"""
        result = await session.execute(
            select(AthenaQuery).filter(AthenaQuery.query_hash == query_hash)
        )
        return result.scalar_one_or_none()

    async def create(
        self, session: AsyncSession, athena_queries: List[AthenaQueryCreate]
    ) -> List[AthenaQuery]:
        """Create multiple athena queries"""

        queries = []
        for athena_query in athena_queries:
            if await self.get_by_query_hash(
                session, AthenaQuery.get_query_hash(athena_query.query)
            ):
                raise QueryAlreadyExistsError("Query already exists")

            print(f"Creating query: {athena_query.to_dict()}")

            queries.append(
                AthenaQuery(
                    **athena_query.to_dict(),
                    query_metadata=AthenaQuery.get_top_level_select_columns(
                        athena_query.query
                    ),
                )
            )

        session.add_all(queries)
        await session.commit()
        return queries

    async def delete(
        self, session: AsyncSession, athena_query: AthenaQueryDelete
    ) -> None:
        """
        Delete a athena query
        """

        query = await self.get_by_id(session, athena_query.id)
        if query is None:
            raise QueryNotFoundError("Query not found")

        await session.delete(query)
        await session.commit()

    async def list(
        self, session: AsyncSession, offset: int, limit: int
    ) -> List[AthenaQuery]:
        """
        List all athena queries
        """

        # Get total count
        count_result = await session.execute(
            select(func.count()).select_from(AthenaQuery)
        )
        total = count_result.scalar_one()

        # Get paginated results
        result = await session.execute(select(AthenaQuery).offset(offset).limit(limit))
        return result.scalars().all(), total

    async def update(
        self, session: AsyncSession, athena_queries: List[AthenaQueryUpdate]
    ) -> List[AthenaQuery]:
        """
        Update many athena queries. The operation is atomic, meaning all updates
        must succeed for the transaction to be committed.
        Returns the updated AthenaQuery objects.
        """
        async with session.begin():  # Start a transaction
            # Get the existing queries
            for athena_query in athena_queries:
                existing_query = await self.get_by_id(session, athena_query.id)

                if existing_query is None:
                    raise QueryNotFoundError("Query not found")

            # Update the queries and return the updated objects
            updated_queries = []
            for athena_query in athena_queries:
                db_update = AthenaQueryDbUpdate(**athena_query.to_dict())
                result = await session.execute(
                    update(AthenaQuery)
                    .where(AthenaQuery.id == athena_query.id)
                    .values(
                        **db_update.to_dict(),
                        query_metadata=AthenaQuery.get_top_level_select_columns(
                            athena_query.query
                        ),
                    )
                    .returning(AthenaQuery)
                )
                updated_query = result.scalar_one()
                updated_queries.append(updated_query)
            await session.commit()
            return updated_queries

    async def get_by_ids(
        self, session: AsyncSession, ids: List[int]
    ) -> List[AthenaQuery]:
        """Get athena queries by ids"""
        result = await session.execute(
            select(AthenaQuery).filter(AthenaQuery.id.in_(ids))
        )
        return result.scalars().all()

    async def get_by_all(
        self, session: AsyncSession, category: str | None = None
    ) -> List[AthenaQuery]:
        """Get all athena queries"""
        query = select(AthenaQuery)
        if category:
            query = query.filter(AthenaQuery.category == category)

        result = await session.execute(query)
        return result.scalars().all()
