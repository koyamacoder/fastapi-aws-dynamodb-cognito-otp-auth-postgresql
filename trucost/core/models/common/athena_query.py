from datetime import datetime

from pydantic import BaseModel, Field


class AthenaQueryResponse(BaseModel):
    id: int = Field(..., description="Query ID")
    category: str = Field(..., description="Category of the query")
    category_type: str = Field(..., description="Type of the category")
    query_type: str = Field(..., description="Type of the query")
    query_subtype: str = Field(..., description="Subtype of the query")
    created_at: datetime = Field(..., description="When the query was created")
    updated_at: datetime = Field(..., description="When the query was last updated")
    query_metadata: list[str] = Field(..., description="Metadata of the query")

    class Config:
        from_attributes = True
