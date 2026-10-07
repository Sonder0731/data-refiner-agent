from typing import Any
from uuid import UUID

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    request_uuid,
    require_row,
    validate_choice,
)


class Pipeline:
    CATEGORIES = {"test", "production"}

    @staticmethod
    def create(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        row = fetch_one(
            """
            INSERT INTO pipeline (request_id, category, content)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), category, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created pipeline")

    @staticmethod
    def upsert(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        row = fetch_one(
            """
            INSERT INTO pipeline (request_id, category, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, category) DO UPDATE
            SET content = EXCLUDED.content
            RETURNING *
            """,
            (request_uuid(request_id), category, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "saved pipeline")

    @staticmethod
    def get(
        request_id: str | UUID,
        category: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        return fetch_one(
            """
            SELECT * FROM pipeline
            WHERE request_id = %s AND category = %s
            """,
            (request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def list_by_request_id(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        return fetch_all(
            """
            SELECT * FROM pipeline
            WHERE request_id = %s
            ORDER BY id
            """,
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_category(
        request_id: str | UUID,
        category: str,
        new_category: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        validate_choice(new_category, Pipeline.CATEGORIES, "new_category")
        row = fetch_one(
            """
            UPDATE pipeline SET category = %s
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (new_category, request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline")

    @staticmethod
    def update_content(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        row = fetch_one(
            """
            UPDATE pipeline SET content = %s
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (content, request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline")

    @staticmethod
    def delete(
        request_id: str | UUID,
        category: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, Pipeline.CATEGORIES, "category")
        row = fetch_one(
            """
            DELETE FROM pipeline
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline")
