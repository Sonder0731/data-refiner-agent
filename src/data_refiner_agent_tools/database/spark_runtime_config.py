from typing import Any

from data_refiner_agent_tools.database.connection import (
    fetch_all,
    fetch_one,
    require_row,
)


class SparkRuntimeConfig:
    @staticmethod
    def create(
        id: str,
        request_id: str,
        config: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO spark_runtime_conifg (id, request_id, config)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            (id, request_id, config),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created spark_runtime_conifg")

    @staticmethod
    def get(
        id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM spark_runtime_conifg WHERE id = %s",
            (id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def list_by_request_id(
        request_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> list[dict[str, Any]]:
        return fetch_all(
            """
            SELECT * FROM spark_runtime_conifg
            WHERE request_id = %s
            ORDER BY id
            """,
            (request_id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_request_id(
        id: str,
        request_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE spark_runtime_conifg SET request_id = %s
            WHERE id = %s RETURNING *
            """,
            (request_id, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "spark_runtime_conifg")

    @staticmethod
    def update_config(
        id: str,
        config: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE spark_runtime_conifg SET config = %s
            WHERE id = %s RETURNING *
            """,
            (config, id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "spark_runtime_conifg")

    @staticmethod
    def delete(
        id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            DELETE FROM spark_runtime_conifg
            WHERE id = %s RETURNING *
            """,
            (id,),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "spark_runtime_conifg")
