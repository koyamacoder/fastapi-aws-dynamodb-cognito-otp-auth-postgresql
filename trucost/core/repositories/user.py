from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from trucost.core.models.user import User, UserRole
from trucost.core.models.template import Template
from trucost.core.services.base import BaseService


class UserRepository(BaseService):
    """Repository for user-related database operations"""

    async def get_by_id(self, session: AsyncSession, user_id: int) -> User | None:
        """Get a user by ID"""
        result = await session.execute(select(User).filter(User.id == user_id))
        return result.scalar_one_or_none()

    async def get_by_email(self, session: AsyncSession, email: str) -> User | None:
        """Get a user by email"""
        result = await session.execute(select(User).filter(User.email == email))
        return result.scalar_one_or_none()

    async def get_by_user_name(
        self, session: AsyncSession, user_name: str
    ) -> User | None:
        """Get a user by user name"""
        result = await session.execute(select(User).filter(User.user_name == user_name))
        return result.scalar_one_or_none()

    async def upsert(
        self,
        session: AsyncSession,
        user: User,
    ) -> User:
        """Upsert a user"""
        existing_user = await self.get_by_user_name(session, user.user_name)
        if existing_user:
            existing_user.hashed_password = user.hashed_password
            existing_user.full_name = user.full_name
            existing_user.phone_number = user.phone_number
        else:
            session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

    async def create(
        self,
        session: AsyncSession,
        email: str,
        hashed_password: str,
        role: UserRole,
        account_id: str,
        full_name: str,
        user_name: str,
        phone_number: str,
    ) -> User:
        """Create a new user"""
        user = User(
            email=email,
            hashed_password=hashed_password,
            role=role,
            account_id=account_id,
            full_name=full_name,
            user_name=user_name,
            phone_number=phone_number,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

    async def list_paginated(
        self,
        session: AsyncSession,
        user_id: int | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[User], int]:
        """List users with pagination"""
        count_result = await session.execute(select(func.count()).select_from(User))
        total = count_result.scalar_one()

        query = (
            select(User)
            .options(
                selectinload(User.assigned_templates),
            )
            .filter(User.id != user_id)
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(query)
        return result.scalars().all(), total

    async def get_settings_and_templates(
        self, session: AsyncSession, user_id: int
    ) -> User:
        """Get dashboard data"""
        query = (
            select(User)
            .filter(User.id == user_id)
            .options(
                selectinload(User.user_settings),
                selectinload(User.assigned_templates).options(
                    selectinload(Template.queries_assigned)
                ),
            )
        )
        result = await session.execute(query)
        return result.scalar_one()

    async def update_user_account_id(
        self, session: AsyncSession, user_id: int, account_id: str
    ) -> User:
        """Update the account ID for a user"""
        user = await self.get_by_id(session, user_id)

        if not user:
            raise ValueError(f"User with ID {user_id} not found")

        user.account_id = account_id
        await session.commit()
        await session.refresh(user)
        return user
