import asyncio

from cachetools import TTLCache

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from trucost.core.services.base import BaseService


class SecretManagerService(BaseService):
    """
    Service for Secret Manager operations.
    """

    _cache = TTLCache(maxsize=100, ttl=60 * 60)  # 1 hour

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ):
        self.secret_manager_client = boto3.client(
            "secretsmanager",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def sync_get_secret(self, secret_name: str):
        try:
            if secret_name in self._cache:
                return self._cache[secret_name]

            response = self.secret_manager_client.get_secret_value(SecretId=secret_name)
            self._cache[secret_name] = response
            return response
        except ClientError as e:
            raise RuntimeError(f"Secret Manager ClientError: {e}")
        except BotoCoreError as e:
            raise RuntimeError(f"Secret Manager BotoCoreError: {e}")

    async def get_secret(self, secret_name: str):
        return await asyncio.to_thread(self.sync_get_secret, secret_name)
