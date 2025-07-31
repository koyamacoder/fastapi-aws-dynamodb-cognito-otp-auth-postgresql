from fastapi import HTTPException

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trucost.core.models.global_settings import (
    GlobalSettings,
    GlobalSettingsUpdate,
    QuickSightDashboardConfig,
    UserRoleDashboardMapping,
)
from trucost.core.services.base import BaseService


class GlobalSettingsRepository(BaseService):
    """Repository for global settings-related database operations"""

    async def get_global_settings(self, session: AsyncSession) -> GlobalSettings | None:
        """Get a global settings"""
        result = await session.execute(select(GlobalSettings).limit(1))
        return result.scalar_one_or_none()

    async def update_global_settings(
        self, session: AsyncSession, settings: GlobalSettingsUpdate
    ) -> GlobalSettings:
        """Update global settings"""
        # Check if global settings already exists
        existing_settings = await self.get_global_settings(session)
        if not existing_settings:
            raise HTTPException(status_code=404, detail="Global settings not found")

        existing_settings.quicksight_dashboard_config = [
            q.model_dump() for q in settings.quicksight_dashboard_config
        ]
        existing_settings.user_role_dashboard_mapping = [
            m.model_dump() for m in settings.user_role_dashboard_mapping
        ]
        session.add(existing_settings)
        await session.commit()
        await session.refresh(existing_settings)
        return existing_settings

    async def add_dashboard_to_existing(
        self, session: AsyncSession, user_id: int, dashboard_id: str
    ) -> GlobalSettings:
        try:
            """Add a dashboard to the existing global settings"""
            print(
                f"Adding dashboard to existing global settings: {user_id=} {dashboard_id=}"
            )

            existing_settings = await self.get_global_settings(session)
            if not existing_settings:
                raise HTTPException(status_code=404, detail="Global settings not found")

            dashboard_found = False
            for dashboard in existing_settings.quicksight_dashboard_config:
                if dashboard_id == dashboard["dashboard_id"]:
                    dashboard_found = True
                    break

            if not dashboard_found:
                print(
                    f"dashboard not found: {existing_settings.quicksight_dashboard_config}"
                )
                quicksight_dashboard_config = (
                    existing_settings.quicksight_dashboard_config
                )
                quicksight_dashboard_config = [
                    *quicksight_dashboard_config,
                    QuickSightDashboardConfig(
                        dashboard_id=dashboard_id,
                        quicksight_user_name="",
                    ).model_dump(),
                ]
                existing_settings.quicksight_dashboard_config = (
                    quicksight_dashboard_config
                )
                print(
                    f"dashboard added: {existing_settings.quicksight_dashboard_config}"
                )

            user_mapping_found = False
            for mapping in existing_settings.user_role_dashboard_mapping:
                if (
                    user_id == mapping["user_id"]
                    and dashboard_id == mapping["dashboard_id"]
                ):
                    user_mapping_found = True
                    break

            if not user_mapping_found:
                print(
                    f"user mapping not found: {existing_settings.user_role_dashboard_mapping}"
                )
                user_role_dashboard_mapping = (
                    existing_settings.user_role_dashboard_mapping
                )
                user_role_dashboard_mapping = [
                    *user_role_dashboard_mapping,
                    UserRoleDashboardMapping(
                        user_id=user_id,
                        dashboard_id=dashboard_id,
                    ).model_dump(),
                ]

                existing_settings.user_role_dashboard_mapping = (
                    user_role_dashboard_mapping
                )
                print(
                    f"user mapping added: {existing_settings.user_role_dashboard_mapping}"
                )

            print(f"Existing settings: {existing_settings=}")

            session.add(existing_settings)
            await session.commit()
            await session.refresh(existing_settings)
        except Exception as e:
            import traceback

            print(traceback.format_exc())
            print(f"Error adding dashboard to existing global settings: {e}")
