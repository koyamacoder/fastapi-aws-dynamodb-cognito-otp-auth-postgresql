from typing import List, Tuple, Dict
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from trucost.core.models.common.filter import FilterOperator, SortConfig
from trucost.core.services.base import BaseService
from trucost.core.services.filter import Filter
from trucost.core.models.resource_tagging import (
    ResourceTagMapping,
    AssignAllResourceTagsRequest,
    ResourceTagMappingResponse,
    AssignResourceTagsRequest,
)
from trucost.core.models.resource_owner import ResourceOwner


class ResourceTagMappingRepository(BaseService, Filter):
    """Repository for resource tag mapping data retrieval"""

    async def get_by_resource_id(
        self,
        session: AsyncSession,
        resource_id: str,
    ) -> ResourceTagMapping:
        """Get resource tag mapping by resource id"""
        query = select(ResourceTagMapping).where(
            ResourceTagMapping.resource_id == resource_id,
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()

    async def list_paginated(
        self,
        session: AsyncSession,
        offset: int = 0,
        limit: int = 100,
        filters: List[FilterOperator] | None = None,
        sort: List[SortConfig] | None = None,
    ) -> Tuple[List[ResourceTagMapping], int]:
        """List resource tag mappings with pagination and optional resource_ids filter"""
        # Base query
        query = select(ResourceTagMapping)

        if filters:
            query = self.apply_filters([ResourceTagMapping], query, filters)

        if sort:
            query = self.apply_sorting(ResourceTagMapping, query, sort)

        # Add pagination
        query = query.offset(offset).limit(limit)

        # Execute queries
        result = await session.execute(query)

        # Get total count of resource tag mappings with filters
        total_query = select(func.count()).select_from(ResourceTagMapping)
        if filters:
            total_query = self.apply_filters([ResourceTagMapping], total_query, filters)
        total_result = await session.execute(total_query)
        total = total_result.scalar_one()

        # Get results
        mappings = result.scalars().all()

        return mappings, total

    async def get_by_tag_value(
        self,
        session: AsyncSession,
        tag_name: str,
        tag_value: str,
        use_current_tags: bool = True,
    ) -> List[ResourceTagMapping]:
        """
        Get resource tag mappings by tag name and value

        Args:
            session: The database session
            tag_name: The name of the tag (e.g., 'app', 'bu', 'env', 'owner', 'name')
            tag_value: The value to search for
            use_current_tags: If True, search in cur_trucost_rt_* columns, if False search in user_trucost_rt_* columns
        """
        prefix = "cur_trucost_rt_" if use_current_tags else "user_trucost_rt_"
        column_name = f"{prefix}{tag_name}"

        if not hasattr(ResourceTagMapping, column_name):
            raise ValueError(f"Invalid tag name: {tag_name}")

        query = select(ResourceTagMapping).where(
            getattr(ResourceTagMapping, column_name) == tag_value
        )

        result = await session.execute(query)
        return result.scalars().all()

    async def get_facets(
        self,
        session: AsyncSession,
        filters: List[FilterOperator] | None = None,
    ) -> Dict[str, List[str]]:
        """Get unique values for specified columns with optional filtering"""
        columns_to_fetch = [
            "resource_id",
            "cur_trucost_rt_app",
            "cur_trucost_rt_bu",
            "cur_trucost_rt_env",
            "cur_trucost_rt_owner",
            "cur_trucost_rt_name",
            "line_item_product_code",
            "product_region_code",
            "usage_account_id",
            "user_trucost_rt_app",
            "user_trucost_rt_bu",
            "user_trucost_rt_env",
            "user_trucost_rt_owner",
            "user_trucost_rt_name",
        ]

        result = {}
        for column_name in columns_to_fetch:
            # Create base query for distinct values with IDs
            column = getattr(ResourceTagMapping, column_name)
            query = select(column).distinct()

            if filters:
                query = self.apply_filters([ResourceTagMapping], query, filters)

            # Execute query and get results
            q_result = await session.execute(query)

            # Create tuples of (id, value) for each non-null value
            values = [row[0] for row in q_result if row[0] is not None]
            values = list(filter(lambda x: x, values))

            # Sort by the actual value
            result[column_name] = sorted(values)

        resource_owner_columns = ["owner_name", "owner_email"]
        for column_name in resource_owner_columns:
            q = select(getattr(ResourceOwner, column_name)).distinct()
            q_result = await session.execute(q)

            values = [row[0] for row in q_result if row[0] is not None]
            values = list(filter(lambda x: x, values))
            result[column_name] = sorted(values)

        return result

    async def update_resource_tag(
        self,
        session: AsyncSession,
        resource_id: str,
        update_data: dict,
    ) -> ResourceTagMapping:
        """Update a resource tag mapping"""
        # First check if the resource exists
        existing = await self.get_by_resource_id(session, resource_id)
        if not existing:
            return None

        # Update only user-editable fields
        update_data["last_user_update"] = func.now()

        # Update the resource tag mapping
        query = (
            update(ResourceTagMapping)
            .where(ResourceTagMapping.resource_id == resource_id)
            .values(**update_data)
        )
        await session.execute(query)
        await session.commit()

        # Fetch the updated record
        return await self.get_by_resource_id(session, resource_id)

    async def assign_all_resource_tags(
        self,
        session: AsyncSession,
        request: AssignAllResourceTagsRequest,
    ) -> List[ResourceTagMappingResponse]:
        """Assign all resource tags to the user"""
        print(f"request: {request.model_dump()=}")

        # Build base query with filters
        base_query = select(ResourceTagMapping)
        if request.filters:
            base_query = self.apply_filters(
                [ResourceTagMapping], base_query, request.filters
            )

        # Get the resource IDs that match our filters
        filtered_ids_query = base_query.with_only_columns(
            ResourceTagMapping.resource_id
        )
        filtered_ids_result = await session.execute(filtered_ids_query)
        resource_ids = [r[0] for r in filtered_ids_result]

        if not resource_ids:
            return []

        # Perform bulk update
        bulk_update = (
            update(ResourceTagMapping)
            .where(ResourceTagMapping.resource_id.in_(resource_ids))
            .values(
                user_trucost_rt_app=request.user_trucost_rt_app,
                user_trucost_rt_bu=request.user_trucost_rt_bu,
                user_trucost_rt_env=request.user_trucost_rt_env,
                user_trucost_rt_owner=request.user_trucost_rt_owner,
                user_trucost_rt_name=request.user_trucost_rt_name,
                last_user_update=func.now(),
            )
        )
        await session.execute(bulk_update)
        await session.commit()

        # Fetch updated records
        updated_records_query = select(ResourceTagMapping).where(
            ResourceTagMapping.resource_id.in_(resource_ids)
        )
        result = await session.execute(updated_records_query)
        return result.scalars().all()

    async def assign_resource_tags(
        self,
        session: AsyncSession,
        request: AssignResourceTagsRequest,
    ) -> List[ResourceTagMappingResponse]:
        """Assign resource tags to the user"""
        print(f"request: {request.model_dump()=}")

        query = (
            update(ResourceTagMapping)
            .where(ResourceTagMapping.resource_id.in_(request.resource_ids))
            .values(
                user_trucost_rt_app=request.user_trucost_rt_app,
                user_trucost_rt_bu=request.user_trucost_rt_bu,
                user_trucost_rt_env=request.user_trucost_rt_env,
                user_trucost_rt_owner=request.user_trucost_rt_owner,
                user_trucost_rt_name=request.user_trucost_rt_name,
                last_user_update=func.now(),
            )
        )

        await session.execute(query)
        await session.commit()

        # Fetch updated records
        updated_records_query = select(ResourceTagMapping).where(
            ResourceTagMapping.resource_id.in_(request.resource_ids)
        )
        result = await session.execute(updated_records_query)
        return result.scalars().all()
