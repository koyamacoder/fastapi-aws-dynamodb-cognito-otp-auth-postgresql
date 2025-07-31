from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from trucost.core.settings import Metaservices, MetaSettings
from trucost.api.auth import get_current_user
from trucost.core.injector import get_services, get_settings
from trucost.core.models.query_report import (
    AthenaQueryExecutionCreateBatchDashboard,
    QueryExecutionBatchDashboard,
)
from trucost.api.query_report import create_query_execution_batch_dashboard
from trucost.core.models.user import User

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


class UserDashboardResponse(BaseModel):
    name: str
    dashboard_id: str


class DashboardEmbeddedUrlResponse(BaseModel):
    name: str
    embedded_url: str


@router.get("/")
async def get_dashboard(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        user = await services.user_repo.get_settings_and_templates(session, user_id=2)

        active_user_settings = None
        for user_setting in user.user_settings:
            if user_setting.active:
                active_user_settings = user_setting
                break

        if active_user_settings is None:
            raise HTTPException(status_code=400, detail="No active user settings found")

        executions = await create_query_execution_batch_dashboard(
            batch=AthenaQueryExecutionCreateBatchDashboard(
                user_settings_id=active_user_settings.id,
                queries=[
                    QueryExecutionBatchDashboard(
                        query_id=query.query_id,
                        query_template_assignment_id=query.id,
                        years=query.dashboard_config["dynamic_params"]["year"],
                        months=query.dashboard_config["dynamic_params"]["month"],
                    )
                    for template in user.assigned_templates
                    for query in template.queries_assigned
                ],
            ),
            user=user,
            services=services,
        )
        return executions


@router.get("/assigned", response_model=list[UserDashboardResponse])
async def get_assigned_dashboards(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        user_dashboard_mapping = (
            await services.global_settings_repo.get_global_settings(session)
        )

        dashboards = []
        for mapping in user_dashboard_mapping.user_role_dashboard_mapping:
            if mapping.get("user_id") == user.id:
                print(f"mapping: {mapping}")
                dashboards.append(
                    UserDashboardResponse(
                        name=mapping.get("dashboard_id")
                        .replace("-", " ")
                        .replace("_", " ")
                        .title(),
                        dashboard_id=mapping.get("dashboard_id"),
                    )
                )

        return dashboards


@router.get("/embedded-url/{dashboard_id}", response_model=DashboardEmbeddedUrlResponse)
async def get_dashboard_embed_url(
    dashboard_id: str,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """Get QuickSight dashboard embed URL for the specified dashboard ID"""
    async with services.db.get_session() as session:
        # Get user with active settings
        global_settings = await services.global_settings_repo.get_global_settings(
            session
        )

        if global_settings is None:
            raise HTTPException(status_code=400, detail="No global settings found")

        user_role_dashboard_mapping = global_settings.user_role_dashboard_mapping

        found_dashboard_id = None
        for mapping in user_role_dashboard_mapping:
            if mapping.get("user_id") == user.id:
                found_dashboard_id = mapping.get("dashboard_id")
                if found_dashboard_id == dashboard_id:
                    break

        if not found_dashboard_id:
            raise HTTPException(
                status_code=404, detail="No dashboard found for the user"
            )

        # Initialize QuickSight service with user settings
        embedded_url = await services.quicksight.get_embedded_url(
            dashboard_id=dashboard_id,
            region_name=settings.aws_region_name,
            quicksight_user_name=settings.aws_quicksight_user_name,
            allowed_domains=settings.quicksight_allowed_domains,
        )

        return DashboardEmbeddedUrlResponse(
            name=dashboard_id.replace("-", " ").replace("_", " ").title(),
            embedded_url=embedded_url,
        )
