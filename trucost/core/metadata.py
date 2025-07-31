from typing import Any

from pydantic import BaseModel


class Metadata(BaseModel):
    """
    Metadata for the application loaded from meta.yaml when the app is initialized
    """

    name: str
    version: str
    description: str
    author: str
    settings: dict[str, Any]
