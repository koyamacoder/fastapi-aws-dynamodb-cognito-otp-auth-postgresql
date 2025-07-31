from fastapi import Request
from fastapi.security import OAuth2PasswordBearer
from trucost.core.models.user import User
from trucost.core.settings import Metaservices, MetaSettings


def get_user(request: Request) -> User:
    return request.state.user


def get_services(request: Request) -> Metaservices:
    return request.app.state.services


def get_settings(request: Request) -> MetaSettings:
    return request.app.state.settings


async def get_oauth_scheme(request: Request) -> OAuth2PasswordBearer:
    return await request.app.state.oauth_scheme(request)
