from pydantic import BaseModel, Field
from typing import Optional


class PaginationMetadata(BaseModel):
    total: int = Field(default=0, description="Total number of items")
    page: int = Field(default=0, description="Current page number")
    total_pages: int = Field(default=0, description="Total number of pages")
    next_page: Optional[int] = Field(default=None, description="Next page number")
    prev_page: Optional[int] = Field(default=None, description="Previous page number")
