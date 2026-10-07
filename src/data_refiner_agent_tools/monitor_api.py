import re
from typing import Any

from data_refiner_agent_tools._config import (
    DEFAULT_SPARK_MONITOR_API_URL,
    resolve_base_url,
)
from data_refiner_agent_tools._http import get_json, post_json

APPLICATION_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def _base_url(base_url: str | None) -> str:
    return resolve_base_url(
        base_url,
        "SPARK_MONITOR_URL",
        DEFAULT_SPARK_MONITOR_API_URL,
    )


def create_monitor_task(
    application_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    if not APPLICATION_ID_PATTERN.fullmatch(application_id):
        raise ValueError(f"invalid application_id: {application_id}")
    return post_json(
        f"{_base_url(base_url)}/monitor/tasks",
        {"application_id": application_id},
        timeout=10,
    )


def get_monitor_task(
    application_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    if not APPLICATION_ID_PATTERN.fullmatch(application_id):
        raise ValueError(f"invalid application_id: {application_id}")
    return get_json(
        f"{_base_url(base_url)}/monitor/tasks/{application_id}",
        timeout=10,
    )
