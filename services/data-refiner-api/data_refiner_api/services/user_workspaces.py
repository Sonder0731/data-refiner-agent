"""Health-check workspace APIs and persist user mappings."""

import os
import re
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import psycopg

from db import DEFAULT_DATABASE_DSN

CONTAINER_API_PORT = 8000
HEALTH_PATH = "/health"
_WORKSPACE_USER_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def normalize_user_id(user_id: str) -> str:
    """Convert a Matrix user ID to the workspace user name."""
    user_name = user_id.removeprefix("@").split(":", 1)[0]
    if not _WORKSPACE_USER_NAME.fullmatch(user_name):
        raise ValueError("user_id does not contain a valid workspace user name")
    return user_name


def _container_api_url(container_name: str) -> str:
    api_url = f"http://{container_name}:{CONTAINER_API_PORT}"
    try:
        with urlopen(f"{api_url}{HEALTH_PATH}", timeout=5) as response:
            if response.status != 200:
                raise ValueError(
                    f"Container {container_name!r} health check returned "
                    f"HTTP {response.status}"
                )
    except URLError as exc:
        raise ValueError(
            f"Container {container_name!r} health check failed"
        ) from exc
    return api_url


def save_mapping(user_id: str, container_name: str) -> dict[str, Any]:
    workspace_user_name = normalize_user_id(user_id)
    api_url = _container_api_url(container_name)
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)

    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO workspace_mapping (
                    user_id, user_name, container_name, api_url
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_name) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    container_name = EXCLUDED.container_name,
                    api_url = EXCLUDED.api_url,
                    updated_at = NOW()
                RETURNING user_id, container_name, api_url, updated_at
                """,
                (user_id, workspace_user_name, container_name, api_url),
            )
            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL did not return the saved mapping")
    return {
        "user_id": row[0],
        "container_name": row[1],
        "api_url": row[2],
        "updated_at": row[3],
    }


def get_api_url(user_id: str) -> str:
    """Return the workspace API address mapped to a user."""
    workspace_user_name = normalize_user_id(user_id)
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT api_url FROM workspace_mapping WHERE user_name = %s",
                (workspace_user_name,),
            )
            row = cursor.fetchone()

    if row is None:
        raise ValueError(f"No workspace mapping found for user {user_id!r}")
    return row[0]
