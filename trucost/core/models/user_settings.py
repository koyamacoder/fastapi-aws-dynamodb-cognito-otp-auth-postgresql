from functools import partial
from datetime import datetime, timezone
from typing import List

from pydantic import BaseModel, field_validator, Field
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from trucost.core.models.base import Base
from trucost.core.models.common.pagination import PaginationMetadata


class UserSettings(Base):
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True, index=True)

    # Name of the settings
    name = Column(String, nullable=False)

    # AWS credentials
    access_key = Column(String, nullable=False)
    secret_key = Column(String, nullable=False)
    region = Column(String, nullable=False)
    session_token = Column(String, nullable=True)

    # Athena credentials
    athena_database = Column(String, nullable=False)
    athena_table = Column(String, nullable=False)
    athena_location = Column(String, nullable=False)

    active = Column(Boolean, default=False)

    created_at = Column(DateTime, default=partial(datetime.now, timezone.utc))
    updated_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    user = relationship("User", back_populates="user_settings")
    query_executions = relationship(
        "AthenaQueryExecution", back_populates="user_settings"
    )

    # Unique constraint on name and user_id
    __table_args__ = (
        UniqueConstraint("name", "user_id", name="uix_user_settings_name_user_id"),
    )

    def client_kwargs(self) -> dict:
        return dict(
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=self.session_token,
        )

    def __repr__(self) -> str:
        return f"<UserSettings(id={self.id}, name={self.name}, active={self.active})>"


class UserSettingsCreateRequest(BaseModel):
    name: str
    access_key: str
    secret_key: str
    region: str
    session_token: str | None = None
    athena_database: str
    athena_table: str
    athena_location: str


class UserSettingsCreate(UserSettingsCreateRequest):
    user_id: int


class UserSettingsUpdate(BaseModel):
    id: int
    name: str | None = Field(exclude=True)
    access_key: str | None = Field(exclude=True)
    secret_key: str | None = Field(exclude=True)
    region: str | None = Field(exclude=True)
    session_token: str | None = Field(exclude=True)
    athena_database: str | None = Field(exclude=True)
    athena_table: str | None = Field(exclude=True)
    athena_location: str | None = Field(exclude=True)
    active: bool | None = None


class UserSettingsResponse(BaseModel):
    id: int
    name: str
    access_key: str
    secret_key: str
    region: str
    session_token: str | None = None
    athena_database: str
    athena_table: str
    athena_location: str
    active: bool

    @field_validator(
        "access_key",
        "secret_key",
        "session_token",
        "athena_database",
        "athena_table",
        "athena_location",
        mode="after",
    )
    @classmethod
    def mask_sensitive_fields(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return f"****{v[-4:]}"

    class Config:
        from_attributes = True


class PaginatedUserSettingsResponse(BaseModel):
    data: List[UserSettingsResponse]
    pagination: PaginationMetadata
