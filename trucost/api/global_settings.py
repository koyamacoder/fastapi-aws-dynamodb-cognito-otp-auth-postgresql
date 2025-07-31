from typing import Annotated

from fastapi import APIRouter, Depends

from trucost.api.auth import get_admin_user
from trucost.core.models.user import User

from trucost.core.injector import get_services
from trucost.core.settings import Metaservices
from trucost.core.models.global_settings import (
    GlobalSettingsUpdate,
    GlobalSettingsResponse,
)

router = APIRouter(prefix="/global-settings", tags=["Global Settings"])


@router.get("/", response_model=GlobalSettingsResponse)
async def get_global_settings(
    user: Annotated[User, Depends(get_admin_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Get global settings"""

    async with services.db.get_session() as session:
        return await services.global_settings_repo.get_global_settings(session)


@router.put("/", response_model=GlobalSettingsResponse)
async def update_global_settings(
    settings: GlobalSettingsUpdate,
    user: Annotated[User, Depends(get_admin_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Update global settings"""
    async with services.db.get_session() as session:
        return await services.global_settings_repo.update_global_settings(
            session, settings
        )
