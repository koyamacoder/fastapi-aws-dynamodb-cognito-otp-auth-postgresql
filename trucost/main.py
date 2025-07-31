import os
from pathlib import Path

from .core.app import App
from .core.settings import MetaSettings

settings = MetaSettings.from_metadata(
    metadata_path=Path(os.getenv("METADATA_PATH", Path(__file__).parent / "meta.yaml")),
)


app = App(settings)
