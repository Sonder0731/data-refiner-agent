from typing import Any
from uuid import UUID

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    request_uuid,
    require_row,
    validate_choice,
)


class DataMetadata:
    CATEGORIES = {"input", "test_output", "output"}

    @staticmethod
    def create(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        row = fetch_one(
            """
            INSERT INTO data_metadata (request_id, category, content)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), category, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created data_metadata")

    @staticmethod
    def upsert(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        row = fetch_one(
            """
            INSERT INTO data_metadata (request_id, category, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, category) DO UPDATE
            SET content = EXCLUDED.content
            RETURNING *
            """,
            (request_uuid(request_id), category, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "saved data_metadata")

    @staticmethod
    def get(
        request_id: str | UUID,
        category: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        return fetch_one(
            """
            SELECT * FROM data_metadata
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
            SELECT * FROM data_metadata
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
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        validate_choice(new_category, DataMetadata.CATEGORIES, "new_category")
        row = fetch_one(
            """
            UPDATE data_metadata SET category = %s
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (new_category, request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "data_metadata")

    @staticmethod
    def update_content(
        request_id: str | UUID,
        category: str,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        row = fetch_one(
            """
            UPDATE data_metadata SET content = %s
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (content, request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "data_metadata")

    @staticmethod
    def delete(
        request_id: str | UUID,
        category: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(category, DataMetadata.CATEGORIES, "category")
        row = fetch_one(
            """
            DELETE FROM data_metadata
            WHERE request_id = %s AND category = %s
            RETURNING *
            """,
            (request_uuid(request_id), category),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "data_metadata")
