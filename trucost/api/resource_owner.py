from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from trucost.api.auth import get_current_user
from trucost.core.models.resource_owner import (
    ResourceOwnerResponse,
    ResourceOwnerPagination,
    ResourceOwnerStatus,
    ResourceOwnerUpdateRequest,
    AssignAllResourceOwnersRequest,
    ResourceOwnerCreateListRequest,
)
from trucost.core.models.user import User
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.injector import get_services, get_settings
from trucost.core.settings import Metaservices, MetaSettings

router = APIRouter(prefix="/resource-owner", tags=["Resource Owner"])


@router.post("/assign-all", response_model=List[ResourceOwnerResponse])
async def assign_all_resource_owners(
    request: AssignAllResourceOwnersRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Create resource owner entries for all resources in the cost optimization table"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            # Get all resources from cost optimization table
            resource_owners = (
                await services.resource_owner_repo.assign_all_resource_owners(
                    session,
                    request.owner_name,
                    request.owner_email,
                    request.status,
                    request.override_existing,
                    request.filters,
                )
            )

            responses = []
            for resource_owner in resource_owners:
                responses.append(ResourceOwnerResponse.model_validate(resource_owner))
            return responses
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/", response_model=List[ResourceOwnerResponse])
async def assign_resource_owner(
    data: ResourceOwnerCreateListRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Create a new resource owner"""
    try:
        results = []
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            for ro_item in data.resource_owners:
                # First check if resource owner already exists
                existing_owner = await services.resource_owner_repo.get_resource_owner(
                    session,
                    ro_item.resource_id,
                    ro_item.account_id,
                )
                if existing_owner:
                    # update existing resource owner
                    result = await services.resource_owner_repo.update(
                        session,
                        ro_item.resource_id,
                        ro_item.account_id,
                        data.owner_name,
                        data.owner_email,
                        data.status,
                    )
                    results.append(ResourceOwnerResponse.model_validate(result))
                else:
                    # Create new resource owner
                    result = await services.resource_owner_repo.create(
                        session,
                        ro_item.resource_id,
                        ro_item.account_id,
                        data.owner_name,
                        data.owner_email,
                        data.status,
                    )
                    results.append(ResourceOwnerResponse.model_validate(result))
            return results
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/{resource_id}", response_model=ResourceOwnerResponse)
async def get_resource_owner(
    resource_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Get a resource owner by resource ID"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            resource_owner = await services.resource_owner_repo.get_resource_owner(
                session, resource_id
            )
            if not resource_owner:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Resource owner not found",
                )
            return resource_owner
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/", response_model=ResourceOwnerPagination)
async def list_resource_owners(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
    page: int = 1,
    page_size: int = 100,
    status: ResourceOwnerStatus | None = None,
):
    """List all resource owners with pagination and optional status filter"""
    try:
        offset = (page - 1) * page_size
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            owners, total = await services.resource_owner_repo.list_paginated(
                session,
                offset=offset,
                limit=page_size,
                status=status,
            )

            total_pages = (total + page_size - 1) // page_size
            next_page = page + 1 if page < total_pages else None
            previous_page = page - 1 if page > 1 else None

            return ResourceOwnerPagination(
                data=owners,
                pagination=PaginationMetadata(
                    total=total,
                    page=page,
                    page_size=page_size,
                    total_pages=total_pages,
                    next_page=next_page,
                    previous_page=previous_page,
                ),
            )
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.put("/{resource_id}", response_model=ResourceOwnerResponse)
async def update_resource_owner(
    resource_id: str,
    resource_owner: ResourceOwnerUpdateRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Update a resource owner"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            existing_owner = await services.resource_owner_repo.get_resource_owner(
                session, resource_id
            )
            if not existing_owner:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Resource owner not found",
                )

            return await services.resource_owner_repo.update(
                session, existing_owner.id, resource_owner
            )
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.delete("/{resource_id}", response_model=ResourceOwnerResponse)
async def delete_resource_owner(
    resource_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Delete a resource owner"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            existing_owner = await services.resource_owner_repo.get_resource_owner(
                session, resource_id
            )
            if not existing_owner:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Resource owner not found",
                )

            await services.resource_owner_repo.delete(session, existing_owner.id)
            return existing_owner
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Internal Server Error")
