from typing import List, Union, Dict, Literal

from pydantic import BaseModel, Field


class FilterOperator(BaseModel):
    field: str = Field(..., description="Field name to filter on")
    operator: str = Field(
        ..., description="Operator for comparison (eq, ne, gt, lt, gte, lte, in, like)"
    )
    value: Union[str, int, float, List[Union[str, int, float]], None] = Field(
        ..., description="Value to compare against"
    )


class GroupByConfig(BaseModel):
    fields: List[str] = Field(..., description="Fields to group by")
    aggregations: Dict[str, str] = Field(
        ...,
        description="Aggregation functions to apply on numeric fields (sum, avg, min, max, count)",
    )


class SortConfig(BaseModel):
    field: str = Field(..., description="Field name to sort by")
    order: Literal["asc", "desc"] = Field(
        default="asc", description="Sort order (asc or desc)"
    )
