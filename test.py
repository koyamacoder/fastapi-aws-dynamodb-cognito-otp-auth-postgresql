from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import boto3
import os
import base64
import hmac
import hashlib
from botocore.exceptions import ClientError

app = FastAPI(title="Cognito Auth API with Email & TOTP MFA")

# --- AWS Cognito Configuration ---
COGNITO_CLIENT_ID = os.getenv("COGNITO_CLIENT_ID")
COGNITO_CLIENT_SECRET = os.getenv("COGNITO_CLIENT_SECRET")
AWS_REGION = os.getenv("AWS_REGION_NAME")

cognito_client = boto3.client(
    "cognito-idp",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


def get_secret_hash(username, client_id, client_secret):
    message = bytes(username + client_id, "utf-8")
    key = bytes(client_secret, "utf-8")
    return base64.b64encode(
        hmac.new(key, message, digestmod=hashlib.sha256).digest()
    ).decode()


# --- Pydantic Models ---
class UserRegistration(BaseModel):
    email: EmailStr
    password: str
    full_name: str


class UserConfirmation(BaseModel):
    email: EmailStr
    confirmation_code: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class EmailMFARequest(BaseModel):
    email: EmailStr
    session: str
    code: str


class TOTPChallengeRequest(BaseModel):
    email: EmailStr
    session: str
    code: str


class TOTPSetupSessionRequest(BaseModel):
    session: str


class TOTPVerifyRequest(BaseModel):
    session: str
    code: str


# --- Endpoints ---


@app.post("/register")
async def register_user(user_data: UserRegistration):
    """Register user in Cognito"""
    try:
        cognito_client.sign_up(
            ClientId=COGNITO_CLIENT_ID,
            Username=user_data.email,
            Password=user_data.password,
            UserAttributes=[
                {"Name": "email", "Value": user_data.email},
                {"Name": "name", "Value": user_data.full_name},
            ],
            SecretHash=get_secret_hash(
                user_data.email, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET
            ),
        )
        return {
            "message": "User registered. Check your email for the verification code."
        }
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "UsernameExistsException":
            raise HTTPException(status_code=400, detail="User already exists")
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Registration failed: {e.response['Error']['Message']}",
            )


@app.post("/confirm-registration")
async def confirm_registration(confirmation_data: UserConfirmation):
    """Confirm user registration with verification code"""
    try:
        cognito_client.confirm_sign_up(
            ClientId=COGNITO_CLIENT_ID,
            Username=confirmation_data.email,
            ConfirmationCode=confirmation_data.confirmation_code,
            SecretHash=get_secret_hash(
                confirmation_data.email, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET
            ),
        )
        return {"message": "Email confirmed. You can now login."}
    except ClientError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Confirmation failed: {e.response['Error']['Message']}",
        )


@app.post("/login")
async def login(login_data: LoginRequest):
    """
    Login with email and password.
    Handles MFA challenges: MFA_SETUP, EMAIL_MFA, SOFTWARE_TOKEN_MFA.
    """
    try:
        response = cognito_client.initiate_auth(
            ClientId=COGNITO_CLIENT_ID,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": login_data.email,
                "PASSWORD": login_data.password,
                "SECRET_HASH": get_secret_hash(
                    login_data.email, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET
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
        # Successful login (no MFA required)
        return {
            "message": "Login successful",
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "id_token": response["AuthenticationResult"]["IdToken"],
            "refresh_token": response["AuthenticationResult"]["RefreshToken"],
            "expires_in": response["AuthenticationResult"]["ExpiresIn"],
            "token_type": response["AuthenticationResult"]["TokenType"],
        }
    except ClientError as e:
        raise HTTPException(
            status_code=400, detail=f"Login failed: {e.response['Error']['Message']}"
        )


@app.post("/respond-to-email-mfa")
async def respond_to_email_mfa(mfa: EmailMFARequest):
    """Respond to Email MFA challenge during login."""
    try:
        response = cognito_client.respond_to_auth_challenge(
            ClientId=COGNITO_CLIENT_ID,
            ChallengeName="EMAIL_MFA",
            Session=mfa.session,
            ChallengeResponses={
                "USERNAME": mfa.email,
                "ANSWER": mfa.code,
                "SECRET_HASH": get_secret_hash(
                    mfa.email, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET
                ),
            },
        )
        return {
            "message": "Login successful",
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "id_token": response["AuthenticationResult"]["IdToken"],
            "refresh_token": response["AuthenticationResult"]["RefreshToken"],
            "expires_in": response["AuthenticationResult"]["ExpiresIn"],
            "token_type": response["AuthenticationResult"]["TokenType"],
        }
    except ClientError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Email MFA failed: {e.response['Error']['Message']}",
        )


@app.post("/respond-to-totp-mfa")
async def respond_to_totp_mfa(mfa: TOTPChallengeRequest):
    """Respond to TOTP (authenticator app) MFA challenge during login."""
    try:
        response = cognito_client.respond_to_auth_challenge(
            ClientId=COGNITO_CLIENT_ID,
            ChallengeName="SOFTWARE_TOKEN_MFA",
            Session=mfa.session,
            ChallengeResponses={
                "USERNAME": mfa.email,
                "SOFTWARE_TOKEN_MFA_CODE": mfa.code,
                "SECRET_HASH": get_secret_hash(
                    mfa.email, COGNITO_CLIENT_ID, COGNITO_CLIENT_SECRET
                ),
            },
        )
        return {
            "message": "Login successful",
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "id_token": response["AuthenticationResult"]["IdToken"],
            "refresh_token": response["AuthenticationResult"]["RefreshToken"],
            "expires_in": response["AuthenticationResult"]["ExpiresIn"],
            "token_type": response["AuthenticationResult"]["TokenType"],
        }
    except ClientError as e:
        raise HTTPException(
            status_code=400, detail=f"TOTP MFA failed: {e.response['Error']['Message']}"
        )


@app.post("/associate-software-token")
async def associate_software_token(request: TOTPSetupSessionRequest):
    """
    Initiate TOTP setup (after MFA_SETUP challenge): returns a secret code to generate a QR code.
    """
    try:
        response = cognito_client.associate_software_token(Session=request.session)
        return {
            "secret_code": response["SecretCode"],
            "session": response.get("Session"),
            "message": "Scan this code with your authenticator app.",
        }
    except ClientError as e:
        raise HTTPException(
            status_code=400,
            detail=f"TOTP association failed: {e.response['Error']['Message']}",
        )


@app.post("/verify-software-token")
async def verify_software_token(request: TOTPVerifyRequest):
    """
    Verify TOTP setup: user enters the code from their authenticator app.
    """
    try:
        response = cognito_client.verify_software_token(
            Session=request.session,
            UserCode=request.code,
            FriendlyDeviceName="My Authenticator App",
        )
        # After this, Cognito will allow login with TOTP MFA.
        return {
            "message": "TOTP setup complete. You can now log in with MFA.",
            "status": response["Status"],
        }
    except ClientError as e:
        raise HTTPException(
            status_code=400,
            detail=f"TOTP verification failed: {e.response['Error']['Message']}",
        )


# --- Optional: Run with Uvicorn ---
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
