"""Top-level API router registration."""

from fastapi import APIRouter

from data_refiner_api.api.routes import (
    base_datasets,
    health,
    operators,
    pipelines,
    resources,
    schemas,
    user_workspaces,
    workspace_operations,
)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(base_datasets.router, prefix="/base-datasets")
api_router.include_router(schemas.router, prefix="/schemas")
api_router.include_router(operators.router, prefix="/operators")
api_router.include_router(pipelines.router, prefix="/pipelines")
api_router.include_router(resources.router, prefix="/resources")
api_router.include_router(user_workspaces.router, prefix="/user-containers")
api_router.include_router(workspace_operations.router, prefix="/workspace")
