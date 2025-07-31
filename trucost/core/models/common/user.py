from datetime import datetime
from enum import Enum

from pydantic import BaseModel, EmailStr


class UserRole(str, Enum):
    CFO = "cfo"
    CEO = "ceo"
    MANAGER = "manager"
    ENGINEER = "engineer"
    ADMIN = "admin"


class UserResponseBase(BaseModel):
    id: int
    email: EmailStr
    role: UserRole
    created_at: datetime
    updated_at: datetime
    account_id: str | None = None

    class Config:
        from_attributes = True
