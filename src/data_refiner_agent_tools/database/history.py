from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from data_refiner_agent_tools.database.connection import (
    fetch_one,
    request_uuid,
    require_row,
)


class History:
    @staticmethod
    def create(
        request_id: str | UUID,
        content: list[dict[str, Any]],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO history (request_id, content)
            VALUES (%s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), Jsonb(content)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created history")

    @staticmethod
    def get(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM history WHERE request_id = %s",
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_content(
        request_id: str | UUID,
        content: list[dict[str, Any]],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE history SET content = %s
            WHERE request_id = %s
            RETURNING *
            """,
            (Jsonb(content), request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "history")

    @staticmethod
    def append(
        request_id: str | UUID,
        item: dict[str, Any],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE history SET content = content || %s
            WHERE request_id = %s
            RETURNING *
            """,
            (Jsonb([item]), request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "history")

    @staticmethod
    def delete(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            "DELETE FROM history WHERE request_id = %s RETURNING *",
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "history")
