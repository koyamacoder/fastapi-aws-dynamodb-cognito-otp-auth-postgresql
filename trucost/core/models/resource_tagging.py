from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, DateTime

from trucost.core.models.base import SummaryBase
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.common.filter import FilterOperator


class ResourceTagMapping(SummaryBase):
    __tablename__ = "resource_tag_mappings"

    resource_id = Column(String(255), primary_key=True)
    usage_account_id = Column(String(255), nullable=True)
    product_region_code = Column(String(255), nullable=True)
    line_item_product_code = Column(String(255), nullable=True)

    # Actual values
    cur_trucost_rt_app = Column(String(255), nullable=True)
    cur_trucost_rt_bu = Column(String(255), nullable=True)
    cur_trucost_rt_env = Column(String(255), nullable=True)
    cur_trucost_rt_owner = Column(String(255), nullable=True)
    cur_trucost_rt_name = Column(String(255), nullable=True)

    # User values
    user_trucost_rt_app = Column(String(255), nullable=True)
    user_trucost_rt_bu = Column(String(255), nullable=True)
    user_trucost_rt_env = Column(String(255), nullable=True)
    user_trucost_rt_owner = Column(String(255), nullable=True)
    user_trucost_rt_name = Column(String(255), nullable=True)

    last_cur_sync = Column(DateTime, nullable=True, index=True)
    last_user_update = Column(DateTime, nullable=True, index=True)
    updated_by = Column(String(255), nullable=True)


class ResourceTagMappingResponse(BaseModel):
    resource_id: str
    usage_account_id: Optional[str] = None
    product_region_code: Optional[str] = None
    line_item_product_code: Optional[str] = None

    cur_trucost_rt_app: Optional[str] = None
    cur_trucost_rt_bu: Optional[str] = None
    cur_trucost_rt_env: Optional[str] = None
    cur_trucost_rt_owner: Optional[str] = None
    cur_trucost_rt_name: Optional[str] = None
    user_trucost_rt_app: Optional[str] = None
    user_trucost_rt_bu: Optional[str] = None
    user_trucost_rt_env: Optional[str] = None
    user_trucost_rt_owner: Optional[str] = None
    user_trucost_rt_name: Optional[str] = None
    last_cur_sync: Optional[datetime] = None
    last_user_update: Optional[datetime] = None
    updated_by: Optional[str] = None

    class Config:
        from_attributes = True


class ResourceTagMappingPagination(BaseModel):
    data: List[ResourceTagMappingResponse] = Field(default_factory=list)
    pagination: PaginationMetadata


class ResourceTagMappingUpdate(BaseModel):
    """Model for updating resource tag mapping"""

    resource_id: str
    user_trucost_rt_app: Optional[str] = None
    user_trucost_rt_bu: Optional[str] = None
    user_trucost_rt_env: Optional[str] = None
    user_trucost_rt_owner: Optional[str] = None
    user_trucost_rt_name: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding None values"""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class AssignAllResourceTagsRequest(BaseModel):
    """Model for assigning all resource tags to the user"""

    user_trucost_rt_app: Optional[str] = None
    user_trucost_rt_bu: Optional[str] = None
    user_trucost_rt_env: Optional[str] = None
    user_trucost_rt_owner: Optional[str] = None
    user_trucost_rt_name: Optional[str] = None
    override_existing: bool = False
    filters: List[FilterOperator] | None = None


class AssignResourceTagsRequest(BaseModel):
    """Model for assigning resource tags to the user"""

    resource_ids: List[str]
    user_trucost_rt_app: Optional[str] = None
    user_trucost_rt_bu: Optional[str] = None
    user_trucost_rt_env: Optional[str] = None
    user_trucost_rt_owner: Optional[str] = None
    user_trucost_rt_name: Optional[str] = None
