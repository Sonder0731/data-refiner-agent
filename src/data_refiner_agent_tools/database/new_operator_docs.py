from typing import Any
from uuid import UUID

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    request_uuid,
    require_row,
)


class NewOperatorDocs:
    @staticmethod
    def create(
        request_id: str | UUID,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO new_operator_docs (request_id, content)
            VALUES (%s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created new_operator_docs")

    @staticmethod
    def get(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM new_operator_docs WHERE id = %s",
            (id,),
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
            SELECT * FROM new_operator_docs
            WHERE request_id = %s
            ORDER BY id
            """,
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_content(
        id: int,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE new_operator_docs SET content = %s
            WHERE id = %s
            RETURNING *
            """,
            (content, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "new_operator_docs")

    @staticmethod
    def delete(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            "DELETE FROM new_operator_docs WHERE id = %s RETURNING *",
            (id,),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "new_operator_docs")
