import json
from typing import Any

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    require_row,
)


class PipelineRetrievalIndex:
    @staticmethod
    def _validate_embedding(embedding: list[float]) -> None:
        if len(embedding) != 384:
            raise ValueError("embedding must contain 384 values")

    @staticmethod
    def create(
        pipeline_case_id: int,
        retrieval_text: str,
        embedding: list[float],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        PipelineRetrievalIndex._validate_embedding(embedding)
        row = fetch_one(
            """
            INSERT INTO pipeline_retrieval_index (
                pipeline_case_id, retrieval_text, embedding
            ) VALUES (%s, %s, %s::vector)
            RETURNING *
            """,
            (pipeline_case_id, retrieval_text, json.dumps(embedding)),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created pipeline_retrieval_index")

    @staticmethod
    def get(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM pipeline_retrieval_index WHERE id = %s",
            (id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def list_by_pipeline_case_id(
        pipeline_case_id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        return fetch_all(
            """
            SELECT * FROM pipeline_retrieval_index
            WHERE pipeline_case_id = %s
            ORDER BY id
            """,
            (pipeline_case_id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def search_similar(
        embedding: list[float],
        limit: int = 5,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        PipelineRetrievalIndex._validate_embedding(embedding)
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        return fetch_all(
            """
            SELECT
                retrieval.id AS retrieval_index_id,
                retrieval.pipeline_case_id,
                retrieval.retrieval_text,
                1 - (retrieval.embedding <=> %s::vector) AS similarity,
                pipeline_case.original_user_query,
                pipeline_case.belong,
                pipeline_case.user_id,
                pipeline_case.input_data_desc,
                pipeline_case.pipeline,
                pipeline_case.task_summary,
                pipeline_case.processing_steps,
                pipeline_case.output_data_description,
                pipeline_case.spark_runtime_config,
                pipeline_case.create_at,
                pipeline_case.update_at
            FROM pipeline_retrieval_index AS retrieval
            JOIN pipeline_case
                ON pipeline_case.id = retrieval.pipeline_case_id
            ORDER BY similarity DESC
            LIMIT %s
            """,
            (json.dumps(embedding), limit),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_pipeline_case_id(
        id: int,
        pipeline_case_id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_retrieval_index SET pipeline_case_id = %s
            WHERE id = %s RETURNING *
            """,
            (pipeline_case_id, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_retrieval_index")

    @staticmethod
    def update_retrieval_text(
        id: int,
        retrieval_text: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE pipeline_retrieval_index SET retrieval_text = %s
            WHERE id = %s RETURNING *
            """,
            (retrieval_text, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_retrieval_index")

    @staticmethod
    def update_embedding(
        id: int,
        embedding: list[float],
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        PipelineRetrievalIndex._validate_embedding(embedding)
        row = fetch_one(
            """
            UPDATE pipeline_retrieval_index SET embedding = %s::vector
            WHERE id = %s RETURNING *
            """,
            (json.dumps(embedding), id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_retrieval_index")

    @staticmethod
    def delete(
        id: int,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            DELETE FROM pipeline_retrieval_index
            WHERE id = %s RETURNING *
            """,
            (id,),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "pipeline_retrieval_index")
