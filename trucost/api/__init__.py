from .auth import auth_router
from .athena_query import router as athena_query_router
from .query_report import router as query_report_router
from .user_settings import router as user_settings_router
from .template import router as template_router
from .dashboard import router as dashboard_router
from .global_settings import router as global_settings_router
from .cost_optimization import router as cost_optimization_router
from .resource_owner import router as resource_owner_router
from .resource_tagging import router as resource_tagging_router

routers = [
    auth_router,
    athena_query_router,
    query_report_router,
    user_settings_router,
    template_router,
    dashboard_router,
    global_settings_router,
    cost_optimization_router,
    resource_owner_router,
    resource_tagging_router,
]
