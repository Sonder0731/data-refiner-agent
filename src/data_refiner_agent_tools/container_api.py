import os
from collections.abc import Mapping
from typing import Any

from data_refiner_agent_tools._config import (
    DEFAULT_DOCKER_CONTAINER_API_URL,
    resolve_base_url,
)
from data_refiner_agent_tools._http import post_json


def create_container(
    payload: Mapping[str, Any],
    *,
    api_key: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    url = resolve_base_url(
        base_url,
        "DOCKER_CONTAINER_API_URL",
        DEFAULT_DOCKER_CONTAINER_API_URL,
    )
    key = api_key or os.getenv("DOCKER_CONTAINER_API_KEY")
    if not key:
        raise ValueError("DOCKER_CONTAINER_API_KEY is required")
    return post_json(
        f"{url}/containers",
        dict(payload),
        headers={"X-API-Key": key},
    )
