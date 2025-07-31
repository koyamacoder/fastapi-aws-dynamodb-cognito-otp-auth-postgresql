from typing import Literal, AsyncGenerator
from dataclasses import dataclass
from contextlib import asynccontextmanager
from functools import partial
from typing import cast


from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from fastapi.security import OAuth2PasswordBearer
from starlette.datastructures import State as StarletteState

from trucost.core.settings import MetaSettings
from trucost.core.services.db import DBService, AvailableDB
from trucost.core.services.auth import JWTAuthService
from trucost.core.services.athena import AthenaSqlExecutorServiceFactory
from trucost.core.services.sts import StsService
from trucost.core.services.quicksight import QuickSightService
from trucost.core.repositories.user import UserRepository
from trucost.core.repositories.athena_query import AthenaQueryRepository
from trucost.core.repositories.query_report import QueryReportRepository
from trucost.core.repositories.user_settings import UserSettingsRepository
from trucost.core.repositories.template import TemplateRepository
from trucost.core.repositories.global_settings import GlobalSettingsRepository
from trucost.core.repositories.cost_optimization import CostOptimizationRepository
from trucost.core.repositories.resource_owner import ResourceOwnerRepository
from trucost.core.repositories.resource_tagging import ResourceTagMappingRepository
from trucost.core.services.db import SummaryDBFactory
from trucost.core.settings import Metaservices
from trucost.core.services.email import EmailService
from trucost.core.services.cognito import CognitoService
from trucost.core.services.dynamo_db import DynamoDBService
from trucost.core.router import root_router


@dataclass
class State:
    """
    Adds type checking to the `state` of `FastAPI.App`.
    """

    settings: "MetaSettings"
    services: "Metaservices"
    oauth_scheme: OAuth2PasswordBearer


@asynccontextmanager
async def lifespan(app: "App", settings: MetaSettings) -> AsyncGenerator[None, None]:
    services: "Metaservices" = app.state.services

    async with services.lifespan(settings, services):
        app.add_api_route(
            "/health",
            App.health_check,
        )

        yield


class App(FastAPI):
    """
    The main application class.
    """

    run_state: Literal["starting", "ok"] = "starting"

    def __init__(
        self,
        settings: MetaSettings,
        *args,
        **kwargs,
    ):
        _kwargs = {
            **kwargs,
            # FastAPI configuration
            "title": settings.metadata.name,
            "version": settings.metadata.version,
            "description": settings.metadata.description,
            "author": settings.metadata.author,
            "lifespan": partial(lifespan, settings=settings),
        }
        super().__init__(*args, **_kwargs)

        services = self._get_services(settings)
        oauth_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token")

        self.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # State configuration
        self.state = cast(
            StarletteState,
            State(settings=settings, services=services, oauth_scheme=oauth_scheme),
        )

        # Router configuration
        self.include_router(router=root_router, prefix="/api")

        print(f"App state: {self.state}")

    def _get_services(self, settings: MetaSettings) -> Metaservices:
        db = DBService(AvailableDB.POSTGRES)
        jwt_auth = JWTAuthService(
            secret_key=settings.auth_secret_key,
            algorithm=settings.auth_algorithm,
            access_token_expire_minutes=settings.auth_access_token_expire_minutes,
        )
        athena_factory = AthenaSqlExecutorServiceFactory()
        sts = StsService(
            region_name=settings.aws_region_name,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        quicksight = QuickSightService(
            region_name=settings.aws_region_name,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        user_repo = UserRepository()
        athena_query_repo = AthenaQueryRepository()
        query_report_repo = QueryReportRepository()
        user_settings_repo = UserSettingsRepository()
        template_repo = TemplateRepository()
        global_settings_repo = GlobalSettingsRepository()
        cost_optimization_repo = CostOptimizationRepository()
        resource_owner_repo = ResourceOwnerRepository()
        resource_tag_mapping_repo = ResourceTagMappingRepository()
        email = EmailService()
        cognito = CognitoService(
            region_name=settings.aws_region_name,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        dynamo_db = DynamoDBService(
            region_name=settings.aws_region_name,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        return Metaservices(
            db=db,
            summary_db_factory=SummaryDBFactory(),
            jwt_auth=jwt_auth,
            athena_factory=athena_factory,
            sts=sts,
            quicksight=quicksight,
            user_repo=user_repo,
            athena_query_repo=athena_query_repo,
            query_report_repo=query_report_repo,
            user_settings_repo=user_settings_repo,
            template_repo=template_repo,
            global_settings_repo=global_settings_repo,
            cost_optimization_repo=cost_optimization_repo,
            resource_owner_repo=resource_owner_repo,
            resource_tag_mapping_repo=resource_tag_mapping_repo,
            email=email,
            cognito=cognito,
            dynamo_db=dynamo_db,
        )

    @staticmethod
    async def health_check(req: Request) -> Response:
        return JSONResponse(status_code=200, content={"status": "ok"})
