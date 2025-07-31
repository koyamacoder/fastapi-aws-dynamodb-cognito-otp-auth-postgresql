from typing import List, Optional, Dict, Any, Tuple

from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from trucost.core.models.cost_optimization import (
    CostOptimize,
    CostOptimizeResponse,
)
from trucost.core.models.resource_owner import ResourceOwner
from trucost.core.models.common.filter import FilterOperator, GroupByConfig, SortConfig
from trucost.core.services.base import BaseService
from trucost.core.services.filter import Filter


class CostOptimizationRepository(BaseService, Filter):
    """Repository for cost optimization data retrieval"""

    async def get_aggregated_cost_data(
        self,
        session: AsyncSession,
        filters: Optional[List[FilterOperator]] = None,
        group_by: Optional[GroupByConfig] = None,
        sort: Optional[List[SortConfig]] = None,
        end_filters: Optional[List[Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get cost data with filtering and optional grouping"""
        # Start with base query
        query = select(CostOptimize, ResourceOwner).outerjoin(
            ResourceOwner,
            CostOptimize.resource_id == ResourceOwner.resource_id,
        )

        # Apply filters if any
        if filters:
            query = self.apply_filters([CostOptimize, ResourceOwner], query, filters)

        # Apply grouping if specified
        if group_by:
            query = self.apply_grouping(CostOptimize, query, group_by)

        # Apply sorting if specified
        if sort:
            query = self.apply_sorting(CostOptimize, query, sort)

        if end_filters:
            query = query.where(*end_filters)

        print(query)
        # Execute query
        result = await session.execute(query)

        if group_by:
            # For grouped results, convert to dict with proper keys
            rows = result.mappings().all()
            return [dict(row) for row in rows]
        else:
            # For non-grouped results, return model instances
            return [CostOptimizeResponse(**row) for row in result.scalars().all()]

    async def get_all_cost_data(
        self,
        session: AsyncSession,
        filters: List[FilterOperator] | None = None,
        offset: int = 0,
        limit: int = 10,
        sort: List[SortConfig] | None = None,
        ids: Optional[List[int]] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get all cost optimization data with pagination and resource owner information"""
        # Create base query with join to ResourceOwner
        query = select(CostOptimize, ResourceOwner).outerjoin(
            ResourceOwner, CostOptimize.resource_id == ResourceOwner.resource_id
        )

        if filters:
            query = self.apply_filters([CostOptimize, ResourceOwner], query, filters)

        # Apply sorting if specified
        if sort:
            query = self.apply_sorting(CostOptimize, query, sort)

        if ids:
            query = query.where(and_(CostOptimize.id.in_(ids)))

        query = query.offset(offset).limit(limit)

        result = await session.execute(query)

        # Get total count of cost optimization data with filters
        total_query = (
            select(func.count())
            .select_from(CostOptimize, ResourceOwner)
            .outerjoin(
                ResourceOwner, CostOptimize.resource_id == ResourceOwner.resource_id
            )
        )
        if filters:
            total_query = self.apply_filters(
                [CostOptimize, ResourceOwner], total_query, filters
            )

        total = await session.execute(total_query)

        # Convert results to dictionary with both cost and owner information
        rows = result.all()
        combined_data = []
        for cost_row, owner_row in rows:
            data = {
                **cost_row.__dict__,
                "owner_name": owner_row.owner_name if owner_row else None,
                "owner_email": owner_row.owner_email if owner_row else None,
                "status": owner_row.status.value
                if owner_row and owner_row.status
                else None,
            }
            # Remove SQLAlchemy internal state
            data.pop("_sa_instance_state", None)
            combined_data.append(data)

        return combined_data, total.scalar_one()

    async def get_filter_facets(
        self,
        session: AsyncSession,
        filters: Optional[List[FilterOperator]] = None,
    ) -> Dict[str, List[Any]]:
        """Get unique values for specified columns with optional filtering"""
        columns_to_fetch = [
            "query_title",
            "resource_id",
            "payer_account_id",
            "usage_account_id",
            "payer_account_name",
            "usage_account_name",
            "product_code",
            "year",
            "month",
            "Source",
        ]

        result = {}
        for column_name in columns_to_fetch:
            # Create base query for distinct values with IDs
            column = getattr(CostOptimize, column_name)
            query = select(column).distinct()

            if filters:
                query = self.apply_filters([CostOptimize], query, filters)

            # Execute query and get results
            q_result = await session.execute(query)

            # Create tuples of (id, value) for each non-null value
            values = [row[0] for row in q_result if row[0] is not None]
            values = list(filter(lambda x: x, values))

            # Sort by the actual value
            result[column_name] = sorted(values)

        # Add resource owner fields to the query
        resource_owner_columns = [
            "owner_name",
            "owner_email",
        ]
        for column_name in resource_owner_columns:
            q = select(getattr(ResourceOwner, column_name)).distinct()
            q_result = await session.execute(q)

            values = [row[0] for row in q_result if row[0] is not None]
            values = list(filter(lambda x: x, values))
            result[column_name] = sorted(values)

        result["status"] = ["TODO", "WIP", "COMPLETED", "SUPRESSED"]

        return result
