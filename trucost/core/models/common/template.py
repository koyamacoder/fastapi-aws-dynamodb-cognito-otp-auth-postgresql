from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TemplateResponseBase(BaseModel):
    id: int = Field(..., description="Template ID")
    name: str = Field(..., description="Name of the template")
    description: Optional[str] = Field(None, description="Description of the template")
    created_by: int = Field(..., description="ID of the user who created the template")
    created_at: datetime = Field(..., description="When the template was created")
    updated_at: datetime = Field(..., description="When the template was last updated")

    class Config:
        from_attributes = True
