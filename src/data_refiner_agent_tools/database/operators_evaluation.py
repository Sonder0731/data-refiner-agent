from typing import Any
from uuid import UUID

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    request_uuid,
    require_row,
    validate_rounds,
)


class OperatorsEvaluation:
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
            INSERT INTO operators_evaluation (request_id, rounds, content)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            (request_uuid(request_id), rounds, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created operators_evaluation")

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
            INSERT INTO operators_evaluation (request_id, rounds, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, rounds) DO UPDATE
            SET content = EXCLUDED.content
            RETURNING *
            """,
            (request_uuid(request_id), rounds, content),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "saved operators_evaluation")

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
            SELECT * FROM operators_evaluation
            WHERE request_id = %s AND rounds = %s
            """,
            (request_uuid(request_id), rounds),
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
            SELECT * FROM operators_evaluation
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
            UPDATE operators_evaluation SET rounds = %s
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (new_rounds, request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "operators_evaluation")

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
            UPDATE operators_evaluation SET content = %s
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (content, request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "operators_evaluation")

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
            DELETE FROM operators_evaluation
            WHERE request_id = %s AND rounds = %s
            RETURNING *
            """,
            (request_uuid(request_id), rounds),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "operators_evaluation")
