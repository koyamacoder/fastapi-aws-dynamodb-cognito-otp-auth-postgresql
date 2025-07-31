import asyncio
import base64
import hmac
import hashlib
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from trucost.core.services.base import BaseService

if TYPE_CHECKING:
    from trucost.core.settings import MetaSettings, Metaservices


def get_secret_hash(username, client_id, client_secret):
    message = bytes(username + client_id, "utf-8")
    key = bytes(client_secret, "utf-8")
    return base64.b64encode(
        hmac.new(key, message, digestmod=hashlib.sha256).digest()
    ).decode()


class CognitoService(BaseService):
    def __init__(
        self,
        region_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str | None = None,
    ):
        self.client = boto3.client(
            "cognito-idp",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

        self._client_id = None

    def sync_sign_up(
        self,
        email: str,
        password: str,
        full_name: str,
        user_name: str,
        phone_number: str,
    ):
        try:
            return self.client.sign_up(
                ClientId=self._client_id,
                Username=email,
                Password=password,
                UserAttributes=[
                    {"Name": "email", "Value": email},
                    {"Name": "name", "Value": full_name},
                    {"Name": "preferred_username", "Value": user_name},
                    {"Name": "phone_number", "Value": phone_number},
                ],
                SecretHash=get_secret_hash(email, self._client_id, self._client_secret),
            )
        except ClientError as e:
            print(f"Error signing up user {email}: {e}")
            raise e

    async def sign_up(
        self,
        email: str,
        password: str,
        full_name: str,
        user_name: str,
        phone_number: str,
    ):
        return await asyncio.to_thread(
            self.sync_sign_up, email, password, full_name, user_name, phone_number
        )

    def sync_initiate_auth(self, user_name: str, password: str):
        response = self.client.initiate_auth(
            ClientId=self._client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": user_name,
                "PASSWORD": password,
                "SECRET_HASH": get_secret_hash(
                    user_name, self._client_id, self._client_secret
                ),
            },
        )

        if "ChallengeName" in response:
            challenge = response["ChallengeName"]
            session = response.get("Session")
            if challenge == "MFA_SETUP":
                return {
                    "challenge": "MFA_SETUP",
                    "session": session,
                    "message": "MFA setup required. Please set up your authenticator app.",
                }
            elif challenge == "EMAIL_MFA":
                return {
                    "challenge": "EMAIL_MFA",
                    "session": session,
                    "message": "Email MFA required. Please enter the code sent to your email.",
                }
            elif challenge == "SOFTWARE_TOKEN_MFA":
                return {
                    "challenge": "SOFTWARE_TOKEN_MFA",
                    "session": session,
                    "message": "TOTP (authenticator app) MFA required. Please enter the code from your app.",
                }
            elif challenge == "EMAIL_OTP":
                return {
                    "challenge": "EMAIL_OTP",
                    "session": session,
                    "message": "Email OTP required. Please enter the code sent to your email.",
                }
            else:
                print(f"Challenge not found: {challenge}, {response}")
                raise Exception(f"Challenge not found: {challenge}")
        else:
            print(f"Invalid response from Cognito: {response}")
            raise Exception("Invalid response from Cognito")

    async def initiate_auth(self, user_name: str, password: str):
        return await asyncio.to_thread(self.sync_initiate_auth, user_name, password)

    def sync_confirm_sign_up(self, user_name: str, confirmation_code: str):
        return self.client.confirm_sign_up(
            ClientId=self._client_id,
            Username=user_name,
            ConfirmationCode=confirmation_code,
            SecretHash=get_secret_hash(user_name, self._client_id, self._client_secret),
        )

    async def confirm_sign_up(self, user_name: str, confirmation_code: str):
        return await asyncio.to_thread(
            self.sync_confirm_sign_up, user_name, confirmation_code
        )

    def sync_respond_to_email_mfa(
        self,
        challenge_name: str,
        user_name: str,
        session: str,
        code: str,
    ):
        return self.client.respond_to_auth_challenge(
            ClientId=self._client_id,
            ChallengeName=challenge_name,
            Session=session,
            ChallengeResponses={
                "USERNAME": user_name,
                "EMAIL_OTP_CODE": code,
                "ANSWER": code,
                "SECRET_HASH": get_secret_hash(
                    user_name, self._client_id, self._client_secret
                ),
            },
        )

    async def respond_to_email_mfa(
        self,
        challenge_name: str,
        user_name: str,
        session: str,
        code: str,
    ):
        return await asyncio.to_thread(
            self.sync_respond_to_email_mfa, challenge_name, user_name, session, code
        )

    def sync_respond_to_totp_mfa(
        self,
        user_name: str,
        session: str,
        code: str,
    ):
        return self.client.respond_to_auth_challenge(
            ClientId=self._client_id,
            ChallengeName="SOFTWARE_TOKEN_MFA",
            Session=session,
            ChallengeResponses={
                "USERNAME": user_name,
                "SOFTWARE_TOKEN_MFA_CODE": code,
                "SECRET_HASH": get_secret_hash(
                    user_name, self._client_id, self._client_secret
                ),
            },
        )

    async def respond_to_totp_mfa(
        self,
        user_name: str,
        session: str,
        code: str,
    ):
        return await asyncio.to_thread(
            self.sync_respond_to_totp_mfa, user_name, session, code
        )

    def sync_associate_software_token(self, session: str):
        return self.client.associate_software_token(
            ClientId=self._client_id,
            Session=session,
        )

    async def associate_software_token(self, session: str):
        return await asyncio.to_thread(self.sync_associate_software_token, session)

    def sync_verify_software_token(self, session: str, code: str):
        return self.client.verify_software_token(
            ClientId=self._client_id,
            Session=session,
            UserCode=code,
        )

    async def verify_software_token(self, session: str, code: str):
        return await asyncio.to_thread(self.sync_verify_software_token, session, code)

    def sync_admin_get_user(self, user_name: str):
        try:
            self.client.admin_get_user(
                UserPoolId=self._user_pool_id,
                Username=user_name,
            )
            print(f"User {user_name} exists")
            return True
        except self.client.exceptions.UserNotFoundException:
            print(f"User {user_name} does not exist")
            return False
        except Exception as e:
            print(f"Error getting user {user_name}: {e}")
            raise e

    async def admin_get_user(self, user_name: str):
        return await asyncio.to_thread(self.sync_admin_get_user, user_name)

    async def connect(self, settings: "MetaSettings", services: "Metaservices"):
        # super().connect(settings, services)
        self._client_id = settings.cognito_client_id
        self._client_secret = settings.cognito_client_secret
        self._user_pool_id = settings.cognito_user_pool_id

    async def disconnect(self, settings: "MetaSettings"):
        super().disconnect(settings)
