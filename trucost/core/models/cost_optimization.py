from typing import Optional, List, Dict
from datetime import datetime

from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.types import DateTime, JSON

from trucost.core.models.base import SummaryBase
from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.models.common.filter import FilterOperator, GroupByConfig, SortConfig


class CostOptimize(SummaryBase):
    __tablename__ = "cost_optimization_recommendations"

    id = Column(Integer, primary_key=True)
    query_title = Column(String(50), nullable=True)
    resource_id = Column(String(200), nullable=True)
    payer_account_id = Column(String(30), nullable=True)
    usage_account_id = Column(String(30), nullable=True)
    payer_account_name = Column(String(50), nullable=True)
    usage_account_name = Column(String(50), nullable=True)
    product_code = Column(String(50), nullable=True)
    year = Column(String(20), nullable=True)
    month = Column(String(20), nullable=True)
    potentials_saving_percentage = Column(Float, nullable=True)
    potential_savings_usd = Column(Float, nullable=True)
    unblended_cost = Column(Float, nullable=True)
    amortized_cost = Column(Float, nullable=True)
    query_date = Column(DateTime, nullable=True)
    achieved_savings_usd = Column(Float, nullable=True)
    current_config = Column(JSON, nullable=True)
    recommended_config = Column(JSON, nullable=True)
    implementation_details = Column(JSON, nullable=True)
    last_updated = Column(DateTime, nullable=True)
    Source = Column(String(20), nullable=True)


class CostOptimizeResponse(BaseModel):
    id: int
    query_title: str
    resource_id: str
    payer_account_id: str
    usage_account_id: str
    payer_account_name: str
    usage_account_name: str
    product_code: str
    year: str
    month: str
    potentials_saving_percentage: float
    potential_savings_usd: float
    unblended_cost: float
    amortized_cost: float
    query_date: datetime
    achieved_savings_usd: float
    current_config: dict
    recommended_config: dict
    implementation_details: dict
    last_updated: datetime
    Source: str

    class Config:
        from_attributes = True


class CostOptimizeWithResourceOwner(CostOptimizeResponse):
    owner_name: str | None = None
    owner_email: str | None = None
    status: str | None = None


class ErrorResponse(BaseModel):
    error_code: int
    error_message: str


class CostOptimizationPagination(BaseModel):
    error: ErrorResponse | None = None
    data: List[CostOptimizeWithResourceOwner] = Field(default_factory=list)
    cost_summary: Dict[str, float | None] = Field(default_factory=dict)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class CostOptimizationFilter(BaseModel):
    filters: Optional[List[FilterOperator]] = Field(
        None, description="List of filter conditions"
    )
    group_by: Optional[GroupByConfig] = Field(
        None, description="Grouping configuration"
    )
    sort: Optional[List[SortConfig]] = Field(None, description="Sorting configuration")


class CostOptimizationFilterWithIds(CostOptimizationFilter):
    ids: List[int] | None = None


class FilterFacetsFilter(BaseModel):
    filters: Optional[List[FilterOperator]] = Field(
        None, description="List of filter conditions"
    )


class CostOptimizationNotificationPayload(BaseModel):
    ids: List[int] | None = None
    email: str | None = None
    filters: CostOptimizationFilter
