from abc import ABC
from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices


class BaseService(ABC):
    is_connected: bool = False

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        self.is_connected = True

    async def disconnect(self, settings: "MetaSettings"):
        self.is_connected = False
