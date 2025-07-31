from functools import partial
from datetime import datetime, timezone

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    DateTime,
)
from sqlalchemy.dialects.postgresql import JSONB


from trucost.core.models.base import Base


class GlobalSettings(Base):
    __tablename__ = "global_settings"

    id = Column(Integer, primary_key=True, index=True)

    quicksight_dashboard_config = Column(JSONB, nullable=False, default=[])
    user_role_dashboard_mapping = Column(JSONB, nullable=False, default=[])

    created_at = Column(DateTime, default=partial(datetime.now, timezone.utc))
    updated_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )


class GlobalSettingsResponse(BaseModel):
    quicksight_dashboard_config: list[dict]
    user_role_dashboard_mapping: list[dict]

    class Config:
        from_attributes = True


class QuickSightDashboardConfig(BaseModel):
    dashboard_id: str
    quicksight_user_name: str


class UserRoleDashboardMapping(BaseModel):
    user_id: int
    dashboard_id: str


class GlobalSettingsUpdate(BaseModel):
    quicksight_dashboard_config: list[QuickSightDashboardConfig]
    user_role_dashboard_mapping: list[UserRoleDashboardMapping]
