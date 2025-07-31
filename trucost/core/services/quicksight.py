import asyncio
import json
from typing import TYPE_CHECKING

import boto3

from trucost.core.services.base import BaseService
from trucost.core.services.sts import StsService

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices


class QuickSightService(BaseService):
    """
    Service for QuickSight operations.
    """

    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
        sts_service: StsService | None = None,
    ):
        self.client = boto3.client(
            "quicksight",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        # We can use STS service if needed
        self.sts_service = sts_service

    def sync_get_embedded_url(
        self,
        dashboard_id: str,
        region_name: str,
        quicksight_user_name: str,
        allowed_domains: list[str] = ["*"],
        session_lifetime_in_minutes: int = 600,
    ) -> str:
        assert self.sts_service is not None, "STS service is not initialized"

        caller_identity = self.sts_service.sync_get_caller_identity()
        if not caller_identity.get("Account"):
            raise RuntimeError("AWS Account ID not found")

        user_arn = f"arn:aws:quicksight:{region_name}:992382675036:user/default/{quicksight_user_name}"

        response = self.client.generate_embed_url_for_registered_user(
            AwsAccountId=caller_identity["Account"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": dashboard_id}},
            UserArn=user_arn,
            AllowedDomains=allowed_domains,
            SessionLifetimeInMinutes=session_lifetime_in_minutes,
        )
        if "EmbedUrl" not in response:
            raise RuntimeError("EmbedUrl not found in response")

        return response["EmbedUrl"]

    async def get_embedded_url(
        self,
        dashboard_id: str,
        region_name: str,
        quicksight_user_name: str,
        allowed_domains: list[str] = ["*"],
        session_lifetime_in_minutes: int = 600,
    ):
        return await asyncio.to_thread(
            self.sync_get_embedded_url,
            dashboard_id,
            region_name,
            quicksight_user_name,
            allowed_domains,
            session_lifetime_in_minutes,
        )

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        if self.sts_service is None:
            self.sts_service = services.sts

        # Test the connection
        await self.sts_service.get_caller_identity()

        self.is_connected = True

    async def disconnect(self, settings: "MetaSettings"):
        self.sts_service = None
        self.is_connected = False
