from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException

from trucost.api.auth import get_admin_user, get_current_user
from trucost.core.models.template import (
    TemplateCreate,
    TemplateResponse,
    TemplateListResponse,
    TemplateAssignUsers,
    TemplateAssignQueries,
    QueryTemplateAssignmentResponse,
)
from trucost.core.repositories.template import UserAlreadyAssignedToTemplateError
from trucost.core.models.user import User
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.injector import get_services
from trucost.core.settings import Metaservices


router = APIRouter(prefix="/templates", tags=["Templates"])


@router.post("", response_model=TemplateResponse)
async def create_template(
    template: TemplateCreate,
    user: Annotated[User, Depends(get_admin_user)],
    services: Annotated[Metaservices, Depends(get_services)],
) -> TemplateResponse:
    """Create a new template."""

    async with services.db.get_session() as session:
        return await services.template_repo.create_template(
            session=session,
            name=template.name,
            description=template.description,
            created_by=user.id,
            user_ids=template.user_ids,
            query_template_assignments=template.query_template_assignments,
        )


@router.get("/queries", response_model=List[QueryTemplateAssignmentResponse])
async def get_user_templates_queries(
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    async with services.db.get_session() as session:
        queries = await services.template_repo.get_user_templates_queries(
            session, user.id
        )
        return queries


@router.get("", response_model=TemplateListResponse)
async def list_templates(
    user: Annotated[User, Depends(get_admin_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 100,
) -> TemplateListResponse:
    """List templates with pagination."""

    offset = (page - 1) * page_size

    async with services.db.get_session() as session:
        templates, total = await services.template_repo.list_templates(
            session=session,
            offset=offset,
            limit=page_size,
        )

        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        previous_page = page - 1 if page > 1 else None

        return TemplateListResponse(
            data=templates,
            pagination=PaginationMetadata(
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                next_page=next_page,
                previous_page=previous_page,
            ),
        )


@router.put("/users")
async def update_users_to_template_assignment(
    template_assign: TemplateAssignUsers,
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Assign a template to a user."""

    async with services.db.get_session() as session:
        if template_assign.include_user_ids:
            try:
                await services.template_repo.assign_users_to_template(
                    session=session,
                    template_id=template_assign.template_id,
                    user_ids=template_assign.include_user_ids,
                )
            except UserAlreadyAssignedToTemplateError as e:
                raise HTTPException(status_code=400, detail=str(e))

        if template_assign.exclude_user_ids:
            await services.template_repo.unassign_users_from_template(
                session=session,
                template_id=template_assign.template_id,
                user_ids=template_assign.exclude_user_ids,
            )


@router.put("/queries")
async def update_queries_to_template_assignment(
    template_assign: TemplateAssignQueries,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
):
    """Get a specific template by ID."""

    async with services.db.get_session() as session:
        if template_assign.include_query_template_assignments:
            await services.template_repo.assign_queries_to_template(
                session=session,
                template_id=template_assign.template_id,
                query_template_assignments=template_assign.include_query_template_assignments,
            )

        if template_assign.exclude_query_template_ids:
            await services.template_repo.unassign_queries_from_template(
                session=session,
                template_id=template_assign.template_id,
                query_template_ids=template_assign.exclude_query_template_ids,
            )
