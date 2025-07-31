import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, Tuple

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from trucost.core.services.base import BaseService

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices


class AthenaSqlExecutorService:
    """
    Service for Athena operations.
    """

    def __init__(
        self,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
    ):
        self._client = boto3.client(
            "athena",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            endpoint_url=endpoint_url,
        )

    def sync_validate_connection(self):
        try:
            self._client.get_work_group(WorkGroup="primary")
        except ClientError as e:
            raise RuntimeError(f"Athena ClientError: {e}")
        except BotoCoreError as e:
            raise RuntimeError(f"Athena BotoCoreError: {e}")

    async def validate_connection(self):
        return await asyncio.to_thread(self.sync_validate_connection)

    def sync_execute_query(
        self,
        query: str,
        query_metadata: dict[str, list[int]],
        database: str,
        table: str,
        output_location: str,
    ) -> tuple[str, str]:
        """
        Execute a query and return the query execution id.
        # Step 1: Start Query Execution
        """
        try:
            query = (
                query.replace("${table_name}$", table)
                .replace(
                    "${year}$",
                    ",".join(f"'{year}'" for year in query_metadata["years"]),
                )
                .replace(
                    "${month}$",
                    ",".join(f"'{month}'" for month in query_metadata["months"]),
                )
            )
            print(
                f"Query: {query}, Database: {database}, Output Location: {output_location}"
            )

            query_excutor = self._client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": output_location},
            )
            return query_excutor["QueryExecutionId"], query
        except Exception as e:
            raise RuntimeError(f"Athena BotoCoreError: {e}")

    async def execute_query(
        self,
        query: str,
        query_metadata: dict[str, list[int]],
        database: str,
        table: str,
        output_location: str,
    ) -> tuple[str, str]:
        return await asyncio.to_thread(
            self.sync_execute_query,
            query,
            query_metadata,
            database,
            table,
            output_location,
        )

    def sync_get_query_execution_status(
        self,
        query_execution_id: str,
    ) -> str:
        """
        Get the status of a query execution.
        """
        try:
            response = self._client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            return {
                "status": response["QueryExecution"]["Status"]["State"],
                "failure_reason": response["QueryExecution"]["Status"].get(
                    "StateChangeReason", None
                ),
            }
        except Exception as e:
            raise RuntimeError(f"Athena BotoCoreError: {e}")

    async def get_query_execution_status(self, query_execution_id: str) -> str:
        return await asyncio.to_thread(
            self.sync_get_query_execution_status, query_execution_id
        )

    def sync_poll_query_execution(
        self,
        query_execution_id: str,
        interval: int = 1,
        max_attempts: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Poll the status of a query execution.
        # Step 2: Wait for Query to Complete
        """

        try:
            attempts = 0
            while True:
                status = self.sync_get_query_execution_status(query_execution_id)
                if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    return status
                else:
                    print("Waiting for query to complete...")
                    time.sleep(interval)
                    attempts += 1
                    if attempts >= max_attempts:
                        raise RuntimeError("Query execution timed out")
        except Exception as e:
            raise RuntimeError(f"Athena BotoCoreError: {e}")

    async def poll_query_execution(
        self, query_execution_id: str, interval: int = 1, max_attempts: int = 10
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self.sync_poll_query_execution, query_execution_id, interval, max_attempts
        )

    def sync_get_query_results(
        self,
        query_execution_id: str,
    ) -> list[dict[str, Any]]:
        """
        Get the results of a query execution.
        # Step 3: Get Results (if succeeded)
        """
        result_response = None
        try:
            result_response = self._client.get_query_results(
                QueryExecutionId=query_execution_id
            )

            result = []
            for row in result_response["ResultSet"]["Rows"]:
                result.append([col.get("VarCharValue", "") for col in row["Data"]])

            return result
        except Exception as e:
            raise RuntimeError(f"Athena BotoCoreError: {e}")

    async def get_query_results(self, query_execution_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.sync_get_query_results, query_execution_id)


class AthenaSqlExecutorServiceFactory(BaseService):
    """
    Factory for AthenaSqlExecutorService.
    """

    _clients: Dict[Tuple[str, str, str, str, str], Any] = {}

    @classmethod
    async def get_client(
        cls,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ) -> AthenaSqlExecutorService:
        key = (
            region_name,
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
        )

        if key not in cls._clients:
            await cls.validate_connection(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )

            cls._clients[key] = AthenaSqlExecutorService(
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                endpoint_url=None,
            )

        return cls._clients[key]

    @classmethod
    async def validate_connection(
        cls,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ):
        session = boto3.client(
            "sts",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
        try:
            await asyncio.to_thread(session.get_caller_identity)
        except Exception as e:
            raise Exception(f"Athena BotoCoreError: {e}")

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        self._clients = {}

    async def disconnect(self, settings: "MetaSettings"):
        self._clients = {}
