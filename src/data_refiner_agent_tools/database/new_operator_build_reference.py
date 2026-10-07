from typing import Any
from uuid import UUID

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    request_uuid,
    require_row,
    validate_rounds,
)


class NewOperatorBuildReference:
    @staticmethod
    def create(
        request_id: str | UUID,
        rounds: int,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_rounds(rounds)
        row = fetch_one(
            """
            INSERT INTO new_operator_build_reference (
                request_id, rounds, content
            ) VALUES (%s, %s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), rounds, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created new_operator_build_reference")

    @staticmethod
    def upsert(
        request_id: str | UUID,
        rounds: int,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_rounds(rounds)
        row = fetch_one(
            """
            INSERT INTO new_operator_build_reference (
                request_id, rounds, content
            ) VALUES (%s, %s, %s)
            ON CONFLICT (request_id, rounds) DO UPDATE
            SET content = EXCLUDED.content
            RETURNING *
            """,
            (request_uuid(request_id), rounds, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "saved new_operator_build_reference")

    @staticmethod
    def get(
        request_id: str | UUID,
        rounds: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        validate_rounds(rounds)
        return fetch_one(
            """
            SELECT * FROM new_operator_build_reference
            WHERE request_id = %s AND rounds = %s
            """,
            (request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def get_latest(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            """
            SELECT * FROM new_operator_build_reference
            WHERE request_id = %s
            ORDER BY rounds DESC
            LIMIT 1
            """,
            (request_uuid(request_id),),
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
            SELECT * FROM new_operator_build_reference
            WHERE request_id = %s
            ORDER BY rounds
            """,
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_rounds(
        request_id: str | UUID,
        rounds: int,
        new_rounds: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_rounds(rounds)
        validate_rounds(new_rounds)
        row = fetch_one(
            """
            UPDATE new_operator_build_reference SET rounds = %s
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (new_rounds, request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "new_operator_build_reference")

    @staticmethod
    def update_content(
        request_id: str | UUID,
        rounds: int,
        content: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_rounds(rounds)
        row = fetch_one(
            """
            UPDATE new_operator_build_reference SET content = %s
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (content, request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "new_operator_build_reference")

    @staticmethod
    def delete(
        request_id: str | UUID,
        rounds: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_rounds(rounds)
        row = fetch_one(
            """
            DELETE FROM new_operator_build_reference
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "new_operator_build_reference")
