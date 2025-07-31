from typing import Annotated, List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from trucost.api.auth import get_current_user
from trucost.core.models.resource_tagging import (
    ResourceTagMappingResponse,
    ResourceTagMappingPagination,
    ResourceTagMappingUpdate,
    AssignAllResourceTagsRequest,
    AssignResourceTagsRequest,
)
from trucost.core.models.common.filter import FilterOperator, SortConfig
from trucost.core.models.user import User
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.injector import get_services, get_settings
from trucost.core.settings import Metaservices, MetaSettings

router = APIRouter(prefix="/resource-tagging", tags=["Resource Tagging"])


@router.get("/{resource_id}")
async def get_resource_tag(
    resource_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Get resource tag mapping by resource ID"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.resource_tag_mapping_repo.get_by_resource_id(
                session, resource_id
            )
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Resource tag mapping not found for ID: {resource_id}",
                )
            return ResourceTagMappingResponse.model_validate(result)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def list_resource_tags(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
    filters: List[FilterOperator] | None = None,
    sort: List[SortConfig] | None = None,
    page: int = 1,
    page_size: int = 10,
):
    """List resource tag mappings with pagination and optional filters"""
    try:
        offset = (page - 1) * page_size

        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            results, total = await services.resource_tag_mapping_repo.list_paginated(
                session,
                offset=offset,
                limit=page_size,
                filters=filters,
                sort=sort,
            )

            total_pages = (total + page_size - 1) // page_size
            next_page = page + 1 if page < total_pages else None
            prev_page = page - 1 if page > 1 else None

            return ResourceTagMappingPagination(
                data=[ResourceTagMappingResponse.model_validate(r) for r in results],
                pagination=PaginationMetadata(
                    total=total,
                    page=page,
                    page_size=page_size,
                    total_pages=total_pages,
                    next_page=next_page,
                    prev_page=prev_page,
                ),
            )
    except SQLAlchemyError as e:
        print(f"SQLAlchemyError: {e}")
        import traceback

        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        print(f"Exception: {e}")
        import traceback

        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/facets")
async def get_facets(
    user: Annotated[User, Depends(get_current_user)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
    services: Annotated[Metaservices, Depends(get_services)],
    filters: List[FilterOperator] | None = None,
):
    """Get facets for resource tag mappings"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.resource_tag_mapping_repo.get_facets(
                session, filters
            )
            return result
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/")
async def update_resource_tag(
    update_data: ResourceTagMappingUpdate,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Update resource tag mapping by resource ID"""
    try:
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.resource_tag_mapping_repo.update_resource_tag(
                session,
                update_data.resource_id,
                update_data.to_dict(),
            )
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Resource tag mapping not found for ID: {update_data.resource_id}",
                )
            return ResourceTagMappingResponse.model_validate(result)
    except SQLAlchemyError as e:
        import traceback

        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assign-all")
async def assign_all_resource_tags(
    request: AssignAllResourceTagsRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Assign all resource tags to the user"""
    try:
        print(f"request-assign-all: {request.model_dump()=}")
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.resource_tag_mapping_repo.assign_all_resource_tags(
                session, request
            )
            return result
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assign-resource-tags")
async def assign_resource_tags(
    request: AssignResourceTagsRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Assign resource tags to the user"""
    try:
        print(f"request-assign-resource-tags: {request.model_dump()=}")
        async with services.summary_db_factory.get_session(
            user.account_id, settings, services
        ) as session:
            result = await services.resource_tag_mapping_repo.assign_resource_tags(
                session, request
            )
            return result
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))
