from functools import partial
from datetime import datetime, timezone
from enum import Enum

from typing import TYPE_CHECKING, Optional
from pydantic import BaseModel, EmailStr
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship, Mapped
from sqlalchemy.types import Enum as SQLEnum

from trucost.core.models.base import Base
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.common.user import UserResponseBase, UserRole as UserRoleEnum
from trucost.core.models.common.template import TemplateResponseBase
from trucost.core.models.template import QueryTemplateAssignmentResponse

if TYPE_CHECKING:
    from trucost.core.models.query_report import AthenaQueryExecution
    from trucost.core.models.template import Template
    from trucost.core.models.user_settings import UserSettings


class UserRole(str, Enum):
    CFO = "cfo"
    CEO = "ceo"
    MANAGER = "manager"
    ENGINEER = "engineer"
    ADMIN = "admin"


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    full_name = Column(String, nullable=True)
    user_name = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)
    account_id = Column(String, index=True)
    hashed_password = Column(String)
    role = Column(SQLEnum(UserRole, name="userrole"), nullable=False)

    created_at = Column(DateTime, default=partial(datetime.now, timezone.utc))
    updated_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )

    # Add relationship to query executions
    query_executions: Mapped[list["AthenaQueryExecution"]] = relationship(
        "AthenaQueryExecution", back_populates="user", cascade="all, delete-orphan"
    )

    # Add relationship to user settings
    user_settings: Mapped[list["UserSettings"]] = relationship(
        "UserSettings",
        back_populates="user",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # Add relationship to templates
    created_templates: Mapped[list["Template"]] = relationship(
        "Template",
        back_populates="creator",
        cascade="all, delete-orphan",
        uselist=True,
    )

    # Add relationship to assigned templates
    assigned_templates: Mapped[list["Template"]] = relationship(
        "Template",
        secondary="user_template_assignments",
        back_populates="users_assigned",
        uselist=True,
    )


class UserCreate(BaseModel):
    email: EmailStr
    password: str
    role: UserRoleEnum
    full_name: str
    phone_number: str
    account_id: str


class UserLogin(BaseModel):
    email: EmailStr


class UserResponse(UserResponseBase):
    pass


class TemplateWithQueriesResponse(TemplateResponseBase):
    assigned_queries: list[QueryTemplateAssignmentResponse]


class UserDashboardResponse(UserResponseBase):
    assigned_templates: list[TemplateWithQueriesResponse]


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[str] = None


class UserWithTemplates(UserResponse):
    assigned_templates: list[TemplateResponseBase]


class UserListResponse(BaseModel):
    data: list[UserWithTemplates]
    pagination: PaginationMetadata


class UserAccountIdUpdate(BaseModel):
    account_id: str


class UserConfirmation(BaseModel):
    email: EmailStr
    confirmation_code: str


class MFAChallenge(BaseModel):
    email: EmailStr
    session: str
    code: str
