from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from trucost.core.models.user_settings import (
    UserSettings,
    UserSettingsCreate,
    UserSettingsUpdate,
)

from trucost.core.services.base import BaseService


class UserSettingsRepository(BaseService):
    """Repository for user settings-related database operations"""

    async def get_by_name(
        self, session: AsyncSession, name: str, user_id: int
    ) -> UserSettings | None:
        """Get a user settings by name"""
        result = await session.execute(
            select(UserSettings).filter(
                UserSettings.name == name, UserSettings.user_id == user_id
            )
        )
        return result.scalar_one_or_none()

    async def get_by_id(
        self, session: AsyncSession, settings_id: int, user_id: int
    ) -> UserSettings | None:
        """Get a user settings by id"""
        result = await session.execute(
            select(UserSettings).filter(
                UserSettings.id == settings_id, UserSettings.user_id == user_id
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self, session: AsyncSession, user_settings: UserSettingsCreate
    ) -> UserSettings:
        """Create a new user settings"""
        user_settings = UserSettings(**user_settings.model_dump())
        session.add(user_settings)
        await session.commit()
        await session.refresh(user_settings)
        return user_settings

    async def update(
        self,
        session: AsyncSession,
        settings_id: int,
        settings_update: UserSettingsUpdate,
    ) -> UserSettings:
        """
        Update user settings
        """

        result = await session.execute(
            select(UserSettings).filter(UserSettings.id == settings_id)
        )
        settings = result.scalar_one_or_none()
        if not settings:
            return None

        # Update only the fields that are provided
        update_data = settings_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            # if already active, don't update
            if field == "active" and settings.active:
                continue

            setattr(settings, field, value)

        if settings_update.active:
            # Update all other settings to inactive
            await session.execute(
                update(UserSettings)
                .filter(
                    UserSettings.user_id == settings.user_id,
                    UserSettings.id != settings.id,
                )
                .values(active=False)
            )

        await session.commit()
        await session.refresh(settings)
        return settings

    async def list_paginated(
        self, session: AsyncSession, user_id: int, offset: int, limit: int
    ) -> tuple[list[UserSettings], int]:
        """List users with pagination and total count

        Returns:
            tuple[list[UserSettings], int]: A tuple containing the list of settings and total count
        """
        # Get total count efficiently using COUNT
        count_result = await session.execute(
            select(func.count())
            .select_from(UserSettings)
            .filter(UserSettings.user_id == user_id)
        )
        total = count_result.scalar_one()

        # Get paginated results
        result = await session.execute(
            select(UserSettings)
            .filter(UserSettings.user_id == user_id)
            .offset(offset)
            .limit(limit)
        )
        return result.scalars().all(), total

    async def delete(self, session: AsyncSession, settings_id: int) -> UserSettings:
        """Delete a user settings"""
        result = await session.execute(
            select(UserSettings).filter(UserSettings.id == settings_id)
        )
        settings = result.scalar_one_or_none()
        if not settings:
            return None

        await session.delete(settings)
        await session.commit()
        return settings
