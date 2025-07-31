from typing import Annotated
import jwt
import uuid


from fastapi import APIRouter, Depends, HTTPException, status

from trucost.core.models.user import (
    User,
    UserCreate,
    UserResponse,
    UserRole,
    UserListResponse,
    UserAccountIdUpdate,
    UserConfirmation,
    UserLogin,
    MFAChallenge,
)

from trucost.core.models.common.pagination import PaginationMetadata
from trucost.core.injector import get_services, get_oauth_scheme, get_settings
from trucost.core.settings import Metaservices, MetaSettings

auth_router = APIRouter(prefix="/auth", tags=["Auth"])


def domain_from_email(email: str) -> str:
    domain = email.split("@")[1].strip().lower()
    if not domain:
        raise ValueError(f"Invalid domain: {domain=}")
    return domain


async def get_current_user(
    token: Annotated[str, Depends(get_oauth_scheme)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
    fetch_account_id: bool = True,
):
    try:
        jwks_url = f"https://cognito-idp.{settings.aws_region_name}.amazonaws.com/{settings.cognito_user_pool_id}/.well-known/jwks.json"

        jwks_client = jwt.PyJWKClient(jwks_url)

        signing_key = jwks_client.get_signing_key_from_jwt(token)
        if not signing_key:
            raise HTTPException(status_code=401, detail="Invalid token")

        payload: dict = jwt.decode(
            token,
            signing_key.key,  # This is a PEM-formatted key
            algorithms=["RS256"],
            audience=settings.cognito_client_id,
            issuer=f"https://cognito-idp.{settings.aws_region_name}.amazonaws.com/{settings.cognito_user_pool_id}",
        )
        email = payload.get("email")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        async with services.db.get_session() as db_session:
            user = await services.user_repo.get_by_email(db_session, email)
            if not user:
                raise HTTPException(status_code=401, detail="Invalid token")

        if fetch_account_id:
            user.account_id = await services.dynamo_db.get_account_id_by_domain(
                settings.dynamo_db_table_name, domain_from_email(email)
            )
        return user

    except Exception:
        import traceback

        print(f"Error: {traceback.format_exc()}")

        raise HTTPException(status_code=401, detail="Invalid token")


def get_admin_user(
    user: Annotated[User, Depends(get_current_user)],
) -> User:
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin user required",
        )
    return user


@auth_router.post("/register", response_model=UserResponse)
async def register(
    user: UserCreate,
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    if user.role == UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid user role",
        )

    async with services.db.get_session() as db_session:
        cognito_user = await services.cognito.admin_get_user(user.email)
        if cognito_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Please contact support. User already exists.",
            )

        user_name = str(uuid.uuid4())
        hashed_password = services.jwt_auth.get_password_hash(str(uuid.uuid4()))

        try:
            await services.cognito.sign_up(
                user.email,
                hashed_password,
                user.full_name,
                user_name,
                user.phone_number,
            )
        except Exception as e:
            print(f"Error signing up user {user.email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error signing up user: {e}",
            )

        db_user = await services.user_repo.upsert(
            db_session,
            User(
                email=user.email,
                hashed_password=hashed_password,
                role=user.role,
                account_id=user.account_id,
                full_name=user.full_name,
                user_name=user_name,
                phone_number=user.phone_number,
            ),
        )

    return db_user


@auth_router.post("/confirm-register", response_model=UserResponse)
async def confirm_register(
    user_confirmation: UserConfirmation,
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    async with services.db.get_session() as db_session:
        user = await services.user_repo.get_by_email(
            db_session, user_confirmation.email
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Please contact support. User not found.",
            )

        cognito_user = await services.cognito.admin_get_user(user.email)
        if not cognito_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Please contact support. Cognito user not found.",
            )

        try:
            await services.cognito.confirm_sign_up(
                user.email,
                user_confirmation.confirmation_code,
            )
        except Exception as e:
            print(f"Error confirming user {user.email}: {e}")
            import traceback

            print(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to confirm user",
            )

        user.account_id = await services.dynamo_db.get_account_id_by_domain(
            settings.dynamo_db_table_name, domain_from_email(user.email)
        )

        if user.account_id:
            await services.summary_db_factory.create_db(settings, [user.account_id])

            # Assign default dashboard to user
        await services.global_settings_repo.add_dashboard_to_existing(
            db_session, user.id, f"{user.account_id}_dashboard"
        )

        # try:
        #     await services.template_repo.create_template(
        #         db_session,
        #         name=f"{user.email} Default Template",
        #         created_by=user.id,
        #         user_ids=[user.id],
        #     )
        # except Exception as e:
        #     print(f"Error creating default template for user {user.email}: {e}")
        #     pass

        # try:
        #     template = await services.template_repo.get_template_by_name(
        #         db_session,
        #         name=f"{user.role.value}",
        #     )
        #     if template:
        #         await services.template_repo.assign_users_to_template(
        #             db_session,
        #             template_id=template.id,
        #             user_ids=[user.id],
        #             check_existing=False,
        #         )
        # except Exception as e:
        #     print(f"Error assigning template to user with role {user.role}: {e}")
        #     pass

    return user


@auth_router.post("/login")
async def login(
    data: UserLogin,
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
):
    """
    Login with email and password.
    Handles MFA challenges: MFA_SETUP, EMAIL_MFA, SOFTWARE_TOKEN_MFA.
    """
    try:
        async with services.db.get_session() as db_session:
            user = await services.user_repo.get_by_email(db_session, data.email)
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="User not found",
                )

        response = await services.cognito.initiate_auth(
            user.email,
            user.hashed_password,
        )

        print(f"Response: {response}")

        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Login failed: {e}")


@auth_router.post("/login-mfa")
async def login_mfa(
    mfa: MFAChallenge,
    services: Annotated[Metaservices, Depends(get_services)],
):
    try:
        async with services.db.get_session() as db_session:
            user = await services.user_repo.get_by_email(db_session, mfa.email)
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="User not found",
                )

        return await services.cognito.respond_to_email_mfa(
            "EMAIL_OTP", user.email, mfa.session, mfa.code
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MFA failed: {e}")


@auth_router.get("/me", response_model=UserResponse)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_user)],
):
    return current_user


@auth_router.get("/users", response_model=UserListResponse)
async def get_users(
    user: Annotated[User, Depends(get_admin_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    page: int = 1,
    page_size: int = 100,
):
    offset = (page - 1) * page_size
    async with services.db.get_session() as db_session:
        users, total = await services.user_repo.list_paginated(
            db_session,
            offset=offset,
            limit=page_size,
        )

        total_pages = (total + page_size - 1) // page_size
        next_page = page + 1 if page < total_pages else None
        previous_page = page - 1 if page > 1 else None

        return UserListResponse(
            data=users,
            pagination=PaginationMetadata(
                total=total,
                page=page,
                page_size=page_size,
                total_pages=total_pages,
                next_page=next_page,
                previous_page=previous_page,
            ),
        )


@auth_router.patch("/account-id", response_model=UserResponse)
async def update_account_id(
    data: UserAccountIdUpdate,
    user: Annotated[User, Depends(get_current_user)],
    services: Annotated[Metaservices, Depends(get_services)],
    settings: Annotated[MetaSettings, Depends(get_settings)],
) -> UserResponse:
    async with services.db.get_session() as db_session:
        user = await services.user_repo.update_user_account_id(
            db_session, user.id, data.account_id
        )

        if user.account_id:
            await services.summary_db_factory.create_db(settings, [user.account_id])

    return user
