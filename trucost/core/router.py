from fastapi import APIRouter

from trucost.api import routers


root_router = APIRouter()


def include_routers(routers: list[APIRouter]):
    for router in routers:
        root_router.include_router(router)


include_routers(routers)
