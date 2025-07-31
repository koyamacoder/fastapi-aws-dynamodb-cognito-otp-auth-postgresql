from typing import Annotated

from fastapi import APIRouter, HTTPException, Depends, status

from trucost.api.auth import get_current_user
from trucost.core.models.user import User
from trucost.core.models.user_settings import (
    PaginationMetadata,
    PaginatedUserSettingsResponse,
    UserSettingsCreateRequest,
    UserSettingsCreate,
    UserSettingsUpdate,
    UserSettingsResponse,
)
from trucost.core.injector import get_services
from trucost.core.settings import Metaservices

router = APIRouter(prefix="/settings", tags=["Settings"])


@router.post("/", response_model=UserSettingsResponse)
async def create_user_settings(
    settings: UserSettingsCreateRequest,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Create user settings for the authenticated user"""
    async with services.db.get_session() as session:
        # Check if settings already exist for the user
        existing_settings = await services.user_settings_repo.get_by_name(
            session, settings.name, user.id
        )
        if existing_settings:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User settings already exist",
            )

        # Create new settings
        return await services.user_settings_repo.create(
            session,
            UserSettingsCreate(user_id=user.id, **settings.model_dump()),
        )


@router.put("/", response_model=UserSettingsResponse)
async def update_user_settings(
    settings: UserSettingsUpdate,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """
    Update user settings for the authenticated user
    """

    async with services.db.get_session() as session:
        existing_settings = await services.user_settings_repo.get_by_id(
            session, settings.id, user.id
        )
        if not existing_settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User settings not found",
            )

        # Update settings
        updated_settings = await services.user_settings_repo.update(
            session, existing_settings.id, settings
        )
        return updated_settings


@router.get("/", response_model=PaginatedUserSettingsResponse)
async def list_user_settings(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 100,
):
    """List all user settings with detailed pagination metadata

    Args:
        page: Current page number (1-based indexing)
        page_size: Number of items per page

    Returns:
        PaginatedUserSettingsResponse: List of settings with detailed pagination metadata
    """
    # Calculate offset from page number
    offset = (page - 1) * page_size

    async with services.db.get_session() as session:
        items, total = await services.user_settings_repo.list_paginated(
            session, user.id, offset, page_size
        )

        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        prev_page = page - 1 if page > 1 else None

        return PaginatedUserSettingsResponse(
            data=items,
            pagination=PaginationMetadata(
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                next_page=next_page,
                prev_page=prev_page,
            ),
        )


@router.get("/{settings_id}", response_model=UserSettingsResponse)
async def get_user_settings(
    settings_id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Get user settings for the authenticated user"""
    async with services.db.get_session() as session:
        settings = await services.user_settings_repo.get_by_id(
            session, settings_id, user.id
        )
        if not settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User settings not found",
            )
        return settings


@router.delete("/{settings_id}", response_model=UserSettingsResponse)
async def delete_user_settings(
    settings_id: int,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Delete user settings for the authenticated user"""
    async with services.db.get_session() as session:
        settings = await services.user_settings_repo.get_by_id(
            session, settings_id, user.id
        )
        if not settings:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User settings not found",
            )

        await services.user_settings_repo.delete(session, settings.id)
        return settings
