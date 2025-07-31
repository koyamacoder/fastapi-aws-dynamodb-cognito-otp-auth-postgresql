from datetime import datetime, timezone
from functools import partial
from typing import List, Optional, TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, Field
from sqlalchemy import (
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    JSON,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trucost.core.models.base import Base
from trucost.core.models.athena_query import AthenaQuery
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.common.template import TemplateResponseBase
from trucost.core.models.common.user import UserResponseBase
from trucost.core.models.common.athena_query import AthenaQueryResponse

if TYPE_CHECKING:
    from trucost.core.models.user import User
    from trucost.core.models.athena_query import AthenaQuery


class UserTemplateAssignment(Base):
    __tablename__ = "user_template_assignments"

    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("templates.id"), primary_key=True
    )
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), primary_key=True
    )


class QueryTemplateAssignment(Base):
    __tablename__ = "query_template_assignments"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("templates.id"), primary_key=True
    )
    query_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("athena_queries.id"), primary_key=True
    )
    dashboard_config: Mapped[dict] = mapped_column(JSON, nullable=True)

    template: Mapped["Template"] = relationship(
        back_populates="queries_assigned",
    )
    query: Mapped["AthenaQuery"] = relationship(
        back_populates="templates_assigned",
    )

    __table_args__ = (PrimaryKeyConstraint("id", "template_id", "query_id"),)

    @classmethod
    def generate_id(cls) -> str:
        return str(uuid4())


class Template(Base):
    __tablename__ = "templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=partial(datetime.now, timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
    )

    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False
    )
    creator: Mapped["User"] = relationship(back_populates="created_templates")

    # Users assigned to this template
    users_assigned: Mapped[list["User"]] = relationship(
        "User",
        secondary="user_template_assignments",
        back_populates="assigned_templates",
        uselist=True,
    )

    # Queries assigned to this template
    queries_assigned: Mapped[list["QueryTemplateAssignment"]] = relationship(
        back_populates="template",
        cascade="all, delete-orphan",
        uselist=True,
    )

    def __repr__(self) -> str:
        return f"<Template(id={self.id}, name={self.name}, description={self.description})>"


class TemplateBase(BaseModel):
    name: str = Field(..., description="Name of the template")
    description: Optional[str] = Field(None, description="Description of the template")


class QueryAssignmentResponse(BaseModel):
    query_id: int = Field(..., description="Query ID")
    dashboard_config: dict | list = Field(..., description="Dashboard config")
    # query: AthenaQueryResponse = Field(..., description="Query")

    class Config:
        from_attributes = True


class TemplateCreate(TemplateBase):
    user_ids: Optional[List[int]] = Field(
        default=None, description="List of user IDs to assign to the template"
    )
    query_template_assignments: Optional[List[QueryAssignmentResponse]] = Field(
        default=None,
        description="List of query template assignments to assign to the template",
    )


class TemplateUpdate(BaseModel):
    name: Optional[str] = Field(None, description="New name for the template")
    description: Optional[str] = Field(
        None, description="New description for the template"
    )


class TemplateResponse(TemplateResponseBase):
    pass


class QueryTemplateAssignmentResponse(BaseModel):
    id: str = Field(..., description="Query template assignment ID")
    template_id: int = Field(..., description="Template ID")
    query_id: int = Field(..., description="Query ID")
    dashboard_config: dict | list = Field(..., description="Dashboard config")
    query: AthenaQueryResponse = Field(..., description="Query")

    class Config:
        from_attributes = True


class TemplateWithUsersAndQueries(TemplateResponse):
    users_assigned: list[UserResponseBase]
    queries_assigned: list[QueryTemplateAssignmentResponse]


class TemplateListResponse(BaseModel):
    data: List[TemplateWithUsersAndQueries]
    pagination: PaginationMetadata


class TemplateAssignment(BaseModel):
    user_ids: List[int] = Field(..., description="List of user IDs to assign/unassign")


class QueryAssignment(BaseModel):
    query_ids: List[int] = Field(
        ..., description="List of query IDs to assign/unassign"
    )


class TemplateAssignUsers(BaseModel):
    template_id: int = Field(..., description="Template ID")
    include_user_ids: Optional[List[int]] = Field(
        default=None, description="List of user IDs to assign"
    )
    exclude_user_ids: Optional[List[int]] = Field(
        default=None, description="List of user IDs to unassign"
    )


class QueryTemplateAssignmentUpdate(BaseModel):
    query_id: int = Field(..., description="Query ID")
    dashboard_config: dict = Field(..., description="Dashboard config")


class TemplateAssignQueries(BaseModel):
    template_id: int = Field(..., description="Template ID")
    include_query_template_assignments: Optional[
        List[QueryTemplateAssignmentUpdate]
    ] = Field(default=None, description="List of query assignments to assign")
    exclude_query_template_ids: Optional[List[str]] = Field(
        default=None, description="List of query assignments to unassign"
    )


if __name__ == "__main__":
    include_query_template_assignments = [
        QueryTemplateAssignmentUpdate(
            query_id=1,
            dashboard_config={
                "graph_type": "line",
                "x_axis": "some_column_name_1",
                "y_axis": "some_column_name_2",
                "dynamic_params": {  # dynamic params always has just year and month values
                    "year": [2024, 2025],
                    "month": [1, 2, 3],
                },
            },
        )
    ]

    request = TemplateAssignQueries(
        template_id=1,
        include_query_template_assignments=include_query_template_assignments,
        exclude_query_ids=[2, 3],
    )

    print(request)
