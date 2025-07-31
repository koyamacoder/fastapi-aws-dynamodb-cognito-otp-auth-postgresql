import asyncio

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from trucost.core.services.base import BaseService


class StsService(BaseService):
    """
    Service for STS operations.
    """

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ):
        self.sts_client = boto3.client(
            "sts",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def sync_get_caller_identity(self):
        try:
            return self.sts_client.get_caller_identity()
        except ClientError as e:
            raise RuntimeError(f"STS ClientError: {e}")
        except BotoCoreError as e:
            raise RuntimeError(f"STS BotoCoreError: {e}")

    async def get_caller_identity(self):
        return await asyncio.to_thread(self.sync_get_caller_identity)
