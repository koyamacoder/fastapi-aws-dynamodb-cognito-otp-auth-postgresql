from enum import Enum
from typing import List
from datetime import datetime, timezone
from functools import partial

from pydantic import BaseModel
from sqlalchemy import Column, String, Integer, Enum as SQLEnum, DateTime

from trucost.core.models.base import SummaryBase
from trucost.core.models.common.filter import FilterOperator
from trucost.core.models.common.pagination import PaginationMetadata


class ResourceOwnerStatus(str, Enum):
    TODO = "TODO"
    WIP = "WIP"
    COMPLETED = "COMPLETED"
    SUPRESSED = "SUPRESSED"


class ResourceOwner(SummaryBase):
    __tablename__ = "resource_owner"

    id = Column(Integer, primary_key=True)
    resource_id = Column(String(200), nullable=True)
    account_id = Column(String(50), nullable=True)
    owner_name = Column(String(50), nullable=True)
    owner_email = Column(String(50), nullable=True)
    status = Column(SQLEnum(ResourceOwnerStatus), nullable=True)
    created_at = Column(
        DateTime, default=partial(datetime.now, timezone.utc), nullable=True
    )
    updated_at = Column(
        DateTime,
        default=partial(datetime.now, timezone.utc),
        onupdate=partial(datetime.now, timezone.utc),
        nullable=True,
    )


class ResourceOwnerCreateRequest(BaseModel):
    resource_id: str
    account_id: str | None = None


class ResourceOwnerCreateListRequest(BaseModel):
    resource_owners: List[ResourceOwnerCreateRequest]
    owner_name: str
    owner_email: str
    status: ResourceOwnerStatus


class ResourceOwnerUpdateRequest(BaseModel):
    resource_id: str | None = None
    account_id: str | None = None
    owner_name: str
    owner_email: str
    status: ResourceOwnerStatus


class ResourceOwnerResponse(BaseModel):
    id: int
    resource_id: str | None = None
    account_id: str | None = None
    owner_name: str
    owner_email: str
    status: ResourceOwnerStatus
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ResourceOwnerPagination(BaseModel):
    data: List[ResourceOwnerResponse]
    pagination: PaginationMetadata


class AssignAllResourceOwnersRequest(BaseModel):
    owner_name: str
    owner_email: str
    status: ResourceOwnerStatus = ResourceOwnerStatus.TODO
    override_existing: bool = False
    filters: List[FilterOperator] | None = None
