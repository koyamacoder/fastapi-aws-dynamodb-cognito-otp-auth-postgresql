import asyncio
from enum import Enum
from typing import AsyncGenerator, TYPE_CHECKING
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from trucost.core.services.base import BaseService
from trucost.utilities.db_migration import run_migrations

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices


def domain_from_email(email: str) -> str:
    domain = email.split("@")[1].strip().lower()
    if not domain:
        raise ValueError(f"Invalid domain: {domain=}")
    return domain


class AvailableDB(str, Enum):
    POSTGRES = "postgresql"
    MYSQL = "mysql"


class DBService(BaseService):
    """
    Service for database operations.
    """

    def __init__(self, db_type: AvailableDB, db_name: str | None = None):
        self.db_type = db_type
        self.db_name = db_name

        self._engine: AsyncEngine | None = None
        self._session: AsyncSession | None = None

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):  # type: ignore
        try:
            if self.db_type == AvailableDB.POSTGRES:
                print(f"[+] Connecting to {settings.db_dsn=}")
                self._engine = create_async_engine(settings.db_dsn)
            elif self.db_type == AvailableDB.MYSQL:
                print(f"[+] Connecting to {settings.summary_db_dsn(self.db_name)=}")
                self._engine = create_async_engine(
                    settings.summary_db_dsn(self.db_name)
                )
            else:
                raise ValueError(f"Invalid database type: {self.db_type}")

            self._session = sessionmaker(
                self._engine, class_=AsyncSession, expire_on_commit=False
            )

            # Test the connection
            async with self._engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                result = result.scalar()
                if not result == 1:
                    raise RuntimeError("Database connection test failed")
        except Exception as e:
            print(f"Error connecting to {self.db_name=}: {e}")
            raise e

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._session() as session:
            yield session

    async def close(self):
        await self._engine.dispose()


class SummaryDBFactory(BaseService):
    """
    Repository for MySQL database operations.
    """

    _db_services: dict[str, DBService] = {}

    @asynccontextmanager
    async def get_session(
        self,
        db_name: str,
        settings: "MetaSettings",
        services: "Metaservices",
    ) -> AsyncGenerator[AsyncSession, None]:
        if settings.use_central_db:
            db_name = settings.summary_db_name_prefix

        if db_name not in self._db_services:
            self._db_services[db_name] = DBService(AvailableDB.MYSQL, db_name)
            await self._db_services[db_name].connect(settings, services)

        async with self._db_services[db_name].get_session() as session:
            yield session

    async def close(self, db_name: str):
        await self._db_services[db_name].close()
        del self._db_services[db_name]

    async def create_db(
        self,
        settings: "MetaSettings",
        db_names: list[str],
    ):
        try:
            engine = create_async_engine(settings.summary_db_dsn(root_user=True))
            async with engine.connect() as conn:
                for db_name in db_names:
                    await conn.execute(
                        text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")
                    )
                    await conn.execute(
                        text(
                            f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO '{settings.summary_db_user}'@'%'"
                        )
                    )
                    await conn.execute(text("FLUSH PRIVILEGES"))

                    loop = asyncio.get_event_loop()
                    with ProcessPoolExecutor() as executor:
                        await loop.run_in_executor(executor, run_migrations, db_name)

            print(f"[+] Database created for {db_names=}")
            await engine.dispose()
        except Exception as e:
            print(f"[-] Error creating database: {e}")
            raise e

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        async with services.db.get_session() as session:
            email_list = await session.execute(text("SELECT email FROM users"))
            email_list = email_list.scalars().all()

        if not email_list:
            print("[-] No users found in the database")
            return

        # Fetch all account ids from dynamo db
        db_names = await services.dynamo_db.get_accounts_by_domains(
            settings.dynamo_db_table_name,
            [domain_from_email(email) for email in email_list],
        )

        await self.create_db(settings, db_names)

        self.is_connected = True

    async def disconnect(self, settings: "MetaSettings"):
        for db_service in self._db_services.values():
            await db_service.close()
