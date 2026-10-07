import os

DEFAULT_SERVICE_HOST = "127.0.0.1"
DEFAULT_DATA_REFINER_API_URL = f"http://{DEFAULT_SERVICE_HOST}:7778"
DEFAULT_DOCKER_CONTAINER_API_URL = f"http://{DEFAULT_SERVICE_HOST}:19999"
DEFAULT_EMBEDDING_API_URL = f"http://{DEFAULT_SERVICE_HOST}:18000"
DEFAULT_SPARK_MONITOR_API_URL = f"http://{DEFAULT_SERVICE_HOST}:8010"


def resolve_database_dsn(dsn: str | None = None) -> str:
    resolved = dsn or os.getenv("DATABASE_URL")
    if not resolved:
        raise ValueError("DATABASE_URL is required")
    return resolved


def resolve_base_url(
    base_url: str | None,
    environment_variable: str,
    default: str,
) -> str:
    return (base_url or os.getenv(environment_variable, default)).rstrip("/")
