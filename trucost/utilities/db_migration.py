import os
from pathlib import Path

from alembic.config import Config
from alembic import command


def get_alembic_config(db_name: str) -> Config:
    config = Config("alembic_summary.ini")
    script_location = Path(__file__).parent.parent.parent / "alembic_summary"
    config.set_main_option("script_location", str(script_location))

    DATABASE_URL = f"mysql+pymysql://{os.getenv('SUMMARY_DB_USER')}:{os.getenv('SUMMARY_DB_PASSWORD')}@{os.getenv('SUMMARY_DB_HOST')}:{os.getenv('SUMMARY_DB_PORT')}/{db_name}"
    config.set_main_option("sqlalchemy.url", DATABASE_URL)

    return config


def run_migrations(db_name: str):
    print(f"[+] Running migrations for `{db_name}`")
    alembic_cfg = get_alembic_config(db_name)
    command.upgrade(alembic_cfg, "head")  # applies all migrations up to head
    print(f"[+] Migrations applied for `{db_name}`")
