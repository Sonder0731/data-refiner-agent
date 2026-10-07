"""FastAPI application assembly."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from db import init_database
from data_refiner_api.api.router import api_router
from sync_base_dataset import sync_base_datasets
from sync_pipeline_case import sync_pipeline_cases


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_database()
    sync_pipeline_cases()
    sync_base_datasets()
    yield


def create_app() -> FastAPI:
    """Create and configure a Data Refiner API application."""
    application = FastAPI(
        title="Data Refiner API",
        description=(
            "Inspect data schemas and operator documentation, then validate "
            "and run Data Refiner pipelines."
        ),
        version="0.0.1",
        lifespan=lifespan,
    )
    application.include_router(api_router)
    return application


app = create_app()
