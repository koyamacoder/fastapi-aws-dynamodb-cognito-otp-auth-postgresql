from typing import List, Tuple
from sqlalchemy import select, delete, update
from sqlalchemy.ext.asyncio import AsyncSession

from trucost.core.models.resource_owner import (
    ResourceOwner,
    ResourceOwnerStatus,
    ResourceOwnerCreateRequest,
)
from trucost.core.services.filter import Filter
from trucost.core.services.base import BaseService
from trucost.core.models.cost_optimization import CostOptimize
from trucost.core.models.common.filter import FilterOperator


class ResourceOwnerRepository(BaseService, Filter):
    """Repository for resource owner data retrieval"""

    async def get_resource_owner(
        self,
        session: AsyncSession,
        resource_id: str | None = None,
        account_id: str | None = None,
    ) -> ResourceOwner:
        """Get resource owner by resource id"""
        query = select(ResourceOwner).where(
            ResourceOwner.resource_id == resource_id,
        )

        if account_id:
            query = query.where(ResourceOwner.account_id == account_id)

        result = await session.execute(query)
        return result.scalar_one_or_none()

    async def create(
        self,
        session: AsyncSession,
        resource_id: str,
        account_id: str,
        owner_name: str,
        owner_email: str,
        status: ResourceOwnerStatus,
    ) -> ResourceOwner:
        """Create a new resource owner"""
        new_owner = ResourceOwner(
            resource_id=resource_id,
            account_id=account_id,
            owner_name=owner_name,
            owner_email=owner_email,
            status=status,
        )
        session.add(new_owner)
        await session.commit()
        await session.refresh(new_owner)
        return new_owner

    async def update(
        self,
        session: AsyncSession,
        resource_id: str,
        account_id: str,
        owner_name: str,
        owner_email: str,
        status: ResourceOwnerStatus,
    ) -> ResourceOwner:
        """Update a resource owner"""
        # First perform the update
        query = (
            update(ResourceOwner)
            .where(ResourceOwner.resource_id == resource_id)
            .values(
                account_id=account_id,
                owner_name=owner_name,
                owner_email=owner_email,
                status=status,
            )
        )
        await session.execute(query)
        await session.commit()

        # Then fetch the updated record
        query = select(ResourceOwner).where(ResourceOwner.resource_id == resource_id)
        result = await session.execute(query)
        return result.scalar_one()

    async def delete(self, session: AsyncSession, owner_id: int) -> None:
        """Delete a resource owner"""
        query = delete(ResourceOwner).where(ResourceOwner.id == owner_id)
        await session.execute(query)
        await session.commit()

    async def list_paginated(
        self,
        session: AsyncSession,
        offset: int = 0,
        limit: int = 100,
        status: ResourceOwnerStatus | None = None,
    ) -> Tuple[List[ResourceOwner], int]:
        """List resource owners with pagination and optional status filter"""
        # Base query
        query = select(ResourceOwner)
        count_query = select(ResourceOwner)

        # Add status filter if provided
        if status:
            query = query.where(ResourceOwner.status == status)
            count_query = count_query.where(ResourceOwner.status == status)

        # Add pagination
        query = query.offset(offset).limit(limit)

        # Execute queries
        result = await session.execute(query)
        count_result = await session.execute(count_query)

        # Get results
        owners = result.scalars().all()
        total = len(count_result.scalars().all())

        return owners, total

    async def get_filtered_resource_owner(
        self, session: AsyncSession, filters: List[FilterOperator]
    ) -> List[ResourceOwner]:
        """Get filtered resource owner"""
        query = select(ResourceOwner)
        for filter in filters:
            query = query.where(getattr(ResourceOwner, filter.field) == filter.value)
        result = await session.execute(query)
        return result.scalars().all()

    async def assign_all_resource_owners(
        self,
        session: AsyncSession,
        owner_name: str,
        owner_email: str,
        status: ResourceOwnerStatus,
        override_existing: bool = False,
        filters: List[FilterOperator] | None = None,
    ) -> List[ResourceOwner]:
        print(f"filters: {filters=}")
        """Assign all resource owners"""
        query = select(CostOptimize, ResourceOwner).outerjoin(
            ResourceOwner,
            ResourceOwner.resource_id == CostOptimize.resource_id,
        )

        if filters:
            query = self.apply_filters([CostOptimize, ResourceOwner], query, filters)

        print(f"query: {query}")

        result = await session.execute(query)
        rows = result.all()

        print(f"rows: {len(rows)=}")

        # raise Exception("stop here")
        combined_data = []
        for cost_optimization, resource_owner in rows:
            assert isinstance(cost_optimization, CostOptimize)

            if resource_owner:
                print(f"deleting resource_owner: {resource_owner.id}")
                await self.delete(session, resource_owner.id)

            resource_owner = ResourceOwner(
                resource_id=cost_optimization.resource_id,
                account_id=cost_optimization.payer_account_id,
                owner_name=owner_name,
                owner_email=owner_email,
                status=status,
            )
            combined_data.append(resource_owner)

            session.add(resource_owner)
        await session.commit()

        print(f"combined_data: {len(combined_data)=}")
        return combined_data
