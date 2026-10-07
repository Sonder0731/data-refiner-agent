from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from data_refiner_agent_tools.database.connection import (
    fetch_one,
    request_uuid,
    require_row,
)


class RequestState:
    @staticmethod
    def create(
        request_id: str | UUID,
        user_id: str,
        user_name: str,
        room_id: str,
        current_stage: str,
        user_request: str,
        *,
        input_data_metadata_id: int | None = None,
        operators_evaluation_id: int | None = None,
        new_operator_build_reference_id: int | None = None,
        new_operator_docs: list[int] | None = None,
        test_pipeline_id: int | None = None,
        pipeline_id: int | None = None,
        test_output_data_metadata_id: int | None = None,
        output_data_metadata_id: int | None = None,
        history_id: int | None = None,
        active_handoff: dict[str, Any] | None = None,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO request_state (
                request_id,
                user_id,
                user_name,
                room_id,
                current_stage,
                user_request,
                input_data_metadata_id,
                operators_evaluation_id,
                new_operator_build_reference_id,
                new_operator_docs,
                test_pipeline_id,
                pipeline_id,
                test_output_data_metadata_id,
                output_data_metadata_id,
                history_id,
                active_handoff
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
            """,
            (
                request_uuid(request_id),
                user_id,
                user_name,
                room_id,
                current_stage,
                user_request,
                input_data_metadata_id,
                operators_evaluation_id,
                new_operator_build_reference_id,
                new_operator_docs or [],
                test_pipeline_id,
                pipeline_id,
                test_output_data_metadata_id,
                output_data_metadata_id,
                history_id,
                Jsonb(active_handoff) if active_handoff is not None else None,
            ),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created request_state")

    @staticmethod
    def get(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM request_state WHERE request_id = %s",
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def get_user_id(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> str | None:
        row = fetch_one(
            "SELECT user_id FROM request_state WHERE request_id = %s",
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )
        return row["user_id"] if row else None

    @staticmethod
    def update_user_id(
        request_id: str | UUID,
        user_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET user_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (user_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_user_name(
        request_id: str | UUID,
        user_name: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET user_name = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (user_name, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_room_id(
        request_id: str | UUID,
        room_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET room_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (room_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_current_stage(
        request_id: str | UUID,
        current_stage: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET current_stage = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (current_stage, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def compare_and_set_current_stage(
        request_id: str | UUID,
        expected_stage: str,
        next_stage: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET current_stage = %s, updated_at = NOW()
            WHERE request_id = %s AND current_stage = %s
            RETURNING *
            """,
            (next_stage, request_uuid(request_id), expected_stage),
            dsn=dsn,
            connection=connection,
        )
        if row is None:
            raise LookupError(
                f"request_state did not match expected stage {expected_stage}"
            )
        return row

    @staticmethod
    def update_user_request(
        request_id: str | UUID,
        user_request: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET user_request = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (user_request, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_input_data_metadata_id(
        request_id: str | UUID,
        input_data_metadata_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET input_data_metadata_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (input_data_metadata_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_operators_evaluation_id(
        request_id: str | UUID,
        operators_evaluation_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET operators_evaluation_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (operators_evaluation_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_new_operator_build_reference_id(
        request_id: str | UUID,
        new_operator_build_reference_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET new_operator_build_reference_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (new_operator_build_reference_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_new_operator_docs(
        request_id: str | UUID,
        new_operator_docs: list[int],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET new_operator_docs = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (new_operator_docs, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_test_pipeline_id(
        request_id: str | UUID,
        test_pipeline_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET test_pipeline_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (test_pipeline_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_pipeline_id(
        request_id: str | UUID,
        pipeline_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET pipeline_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (pipeline_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_test_output_data_metadata_id(
        request_id: str | UUID,
        test_output_data_metadata_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET test_output_data_metadata_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (test_output_data_metadata_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_output_data_metadata_id(
        request_id: str | UUID,
        output_data_metadata_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state
            SET output_data_metadata_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (output_data_metadata_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_history_id(
        request_id: str | UUID,
        history_id: int | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE request_state SET history_id = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (history_id, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def update_active_handoff(
        request_id: str | UUID,
        active_handoff: dict[str, Any] | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        value = Jsonb(active_handoff) if active_handoff is not None else None
        row = fetch_one(
            """
            UPDATE request_state
            SET active_handoff = %s, updated_at = NOW()
            WHERE request_id = %s RETURNING *
            """,
            (value, request_uuid(request_id)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")

    @staticmethod
    def delete(
        request_id: str | UUID,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            "DELETE FROM request_state WHERE request_id = %s RETURNING *",
            (request_uuid(request_id),),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "request_state")
