from typing import Any

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    require_row,
    validate_choice,
)


class PipelineCase:
    BELONG_VALUES = {"data-refiner", "user"}

    @staticmethod
    def _validate_owner(belong: str, user_id: str | None) -> None:
        validate_choice(belong, PipelineCase.BELONG_VALUES, "belong")
        if (belong == "user") != (user_id is not None):
            raise ValueError("user_id is required only when belong is user")

    @staticmethod
    def create(
        original_user_query: str,
        belong: str,
        user_id: str | None,
        input_data_desc: str,
        pipeline: str,
        task_summary: str,
        processing_steps: str,
        output_data_description: str,
        spark_runtime_config: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        PipelineCase._validate_owner(belong, user_id)
        row = fetch_one(
            """
            INSERT INTO pipeline_case (
                original_user_query,
                belong,
                user_id,
                input_data_desc,
                pipeline,
                task_summary,
                processing_steps,
                output_data_description,
                spark_runtime_config
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                original_user_query,
                belong,
                user_id,
                input_data_desc,
                pipeline,
                task_summary,
                processing_steps,
                output_data_description,
                spark_runtime_config,
            ),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created pipeline_case")

    @staticmethod
    def get(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM pipeline_case WHERE id = %s",
            (id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def list_by_belong(
        belong: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        validate_choice(belong, PipelineCase.BELONG_VALUES, "belong")
        return fetch_all(
            """
            SELECT * FROM pipeline_case
            WHERE belong = %s
            ORDER BY id
            """,
            (belong,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def list_by_user_id(
        user_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        return fetch_all(
            """
            SELECT * FROM pipeline_case
            WHERE user_id = %s
            ORDER BY id
            """,
            (user_id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_original_user_query(
        id: int,
        original_user_query: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET original_user_query = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (original_user_query, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_belong(
        id: int,
        belong: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        validate_choice(belong, PipelineCase.BELONG_VALUES, "belong")
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET belong = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (belong, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_user_id(
        id: int,
        user_id: str | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET user_id = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (user_id, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_owner(
        id: int,
        belong: str,
        user_id: str | None,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        PipelineCase._validate_owner(belong, user_id)
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET belong = %s, user_id = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (belong, user_id, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_input_data_desc(
        id: int,
        input_data_desc: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET input_data_desc = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (input_data_desc, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_pipeline(
        id: int,
        pipeline: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET pipeline = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (pipeline, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_task_summary(
        id: int,
        task_summary: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET task_summary = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (task_summary, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_processing_steps(
        id: int,
        processing_steps: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET processing_steps = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (processing_steps, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_output_data_description(
        id: int,
        output_data_description: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET output_data_description = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (output_data_description, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def update_spark_runtime_config(
        id: int,
        spark_runtime_config: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_case
            SET spark_runtime_config = %s, update_at = NOW()
            WHERE id = %s RETURNING *
            """,
            (spark_runtime_config, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")

    @staticmethod
    def delete(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            "DELETE FROM pipeline_case WHERE id = %s RETURNING *",
            (id,),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_case")
