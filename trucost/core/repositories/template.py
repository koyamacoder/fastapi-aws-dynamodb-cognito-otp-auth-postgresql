from typing import List, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from trucost.core.models.template import (
    Template,
    UserTemplateAssignment,
    QueryTemplateAssignment,
    QueryTemplateAssignmentUpdate,
    QueryAssignmentResponse,
)
from trucost.core.services.base import BaseService


class UserAlreadyAssignedToTemplateError(Exception):
    pass


class TemplateRepository(BaseService):
    async def create_template(
        self,
        session: AsyncSession,
        name: str,
        created_by: int,
        description: Optional[str] = None,
        user_ids: Optional[List[int]] = None,
        query_template_assignments: Optional[List[QueryAssignmentResponse]] = None,
    ) -> Template:
        """Create a new template with optional user and query assignments."""

        template = Template(
            name=name.lower(),
            description=description,
            created_by=created_by,
        )
        session.add(template)
        await session.flush()  # Flush to get the template ID

        if user_ids:
            await self.assign_users_to_template(
                session, template.id, user_ids, check_existing=False
            )

        if query_template_assignments:
            query_template_assignments = [
                QueryTemplateAssignmentUpdate(
                    query_id=qta.query_id,
                    dashboard_config=qta.dashboard_config,
                )
                for qta in query_template_assignments
            ]
            await self.assign_queries_to_template(
                session, template.id, query_template_assignments
            )

        await session.commit()
        return template

    async def get_template_by_id(
        self, session: AsyncSession, template_id: int
    ) -> Optional[Template]:
        """Get a template by its ID."""
        return await session.get(Template, template_id)

    async def get_templates_by_user(
        self, session: AsyncSession, user_id: int
    ) -> List[Template]:
        """Get all templates assigned to a user."""
        stmt = (
            select(Template)
            .join(UserTemplateAssignment)
            .where(UserTemplateAssignment.user_id == user_id)
        )
        return list((await session.execute(stmt)).scalars().all())

    async def get_template_by_name(
        self, session: AsyncSession, name: str
    ) -> Optional[Template]:
        """Get a template by its name."""
        return await session.scalar(select(Template).where(Template.name == name))

    async def get_templates_created_by(
        self, session: AsyncSession, user_id: int
    ) -> List[Template]:
        """Get all templates created by a user."""
        stmt = select(Template).where(Template.created_by == user_id)
        return list((await session.execute(stmt)).scalars().all())

    async def assign_users_to_template(
        self,
        session: AsyncSession,
        template_id: int,
        user_ids: List[int],
        check_existing: bool = True,  # if True, will raise an error if user is already assigned to the template
    ) -> List[UserTemplateAssignment]:
        """Assign multiple users to a template."""
        assignments = []
        for user_id in user_ids:
            stmt = select(UserTemplateAssignment).where(
                UserTemplateAssignment.user_id == user_id,
            )
            assignment = (await session.execute(stmt)).scalar_one_or_none()

            if assignment:
                if check_existing:
                    raise UserAlreadyAssignedToTemplateError(
                        f"User {user_id} is already assigned to template {template_id}"
                    )

            assignments.append(
                UserTemplateAssignment(template_id=template_id, user_id=user_id)
            )

        session.add_all(assignments)
        await session.commit()
        return assignments

    async def unassign_users_from_template(
        self, session: AsyncSession, template_id: int, user_ids: List[int]
    ) -> List[UserTemplateAssignment]:
        """Remove a user's assignment from a template."""
        stmt = select(UserTemplateAssignment).where(
            UserTemplateAssignment.template_id == template_id,
            UserTemplateAssignment.user_id.in_(user_ids),
        )
        assignments = (await session.execute(stmt)).scalars().all()
        for assignment in assignments:
            await session.delete(assignment)
        await session.commit()

        return assignments

    async def assign_queries_to_template(
        self,
        session: AsyncSession,
        template_id: int,
        query_template_assignments: List[QueryTemplateAssignmentUpdate],
    ):
        """Assign multiple queries to a template.

        Args:
            session: The database session
            template_id: The ID of the template to assign queries to
            query_ids: Either a list of query IDs or a list of QueryTemplateAssignmentUpdate objects
            dashboard_config: Optional dashboard config to use when query_ids is a list of integers
        """
        assignments = []
        for qta in query_template_assignments:
            assignments.append(
                QueryTemplateAssignment(
                    id=QueryTemplateAssignment.generate_id(),
                    template_id=template_id,
                    query_id=qta.query_id,
                    dashboard_config=qta.dashboard_config,
                )
            )

        session.add_all(assignments)
        await session.commit()

        return assignments

    async def unassign_queries_from_template(
        self, session: AsyncSession, template_id: int, query_template_ids: List[str]
    ):
        """Remove a query's assignment from a template."""
        stmt = select(QueryTemplateAssignment).where(
            QueryTemplateAssignment.template_id == template_id,
            QueryTemplateAssignment.id.in_(query_template_ids),
        )
        assignments = (await session.execute(stmt)).scalars().all()
        for assignment in assignments:
            await session.delete(assignment)
        await session.commit()

        return assignments

    async def update_template(
        self,
        session: AsyncSession,
        template_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[Template]:
        """Update a template's basic information."""
        template = self.get_template_by_id(session, template_id)
        if not template:
            return None

        if name is not None:
            template.name = name
        if description is not None:
            template.description = description

        await session.commit()
        return template

    async def delete_template(self, session: AsyncSession, template_id: int) -> bool:
        """Delete a template and all its assignments."""
        template = self.get_template_by_id(session, template_id)
        if not template:
            return False

        session.delete(template)
        await session.commit()
        return True

    async def list_templates(
        self,
        session: AsyncSession,
        offset: int = 0,
        limit: int = 10,
    ) -> tuple[List[Template], int]:
        """List all templates."""

        count_result = await session.execute(select(func.count()).select_from(Template))
        total = count_result.scalar_one()

        stmt = (
            select(Template)
            .options(
                selectinload(Template.users_assigned),
                selectinload(Template.queries_assigned).joinedload(
                    QueryTemplateAssignment.query
                ),
            )
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all()), total

    async def get_user_templates_queries(
        self, session: AsyncSession, user_id: int
    ) -> List[QueryTemplateAssignment]:
        """Get all templates with queries."""

        # Get all templates assigned to the user
        templates = await self.get_templates_by_user(session, user_id)
        template_ids = [template.id for template in templates]

        # Get all queries assigned to the templates
        stmt = (
            select(QueryTemplateAssignment)
            .where(QueryTemplateAssignment.template_id.in_(template_ids))
            .options(selectinload(QueryTemplateAssignment.query))
        )
        return list((await session.execute(stmt)).scalars().all())
