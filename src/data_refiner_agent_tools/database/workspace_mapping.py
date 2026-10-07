from typing import Any

from data_refiner_agent_tools.database.connection import (
    fetch_one,
    require_row,
)


class WorkspaceMapping:
    @staticmethod
    def create(
        user_id: str,
        user_name: str,
        container_name: str,
        api_url: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO workspace_mapping (
                user_id, user_name, container_name, api_url
            ) VALUES (%s, %s, %s, %s)
            RETURNING *
            """,
            (user_id, user_name, container_name, api_url),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "created workspace_mapping")

    @staticmethod
    def upsert(
        user_id: str,
        user_name: str,
        container_name: str,
        api_url: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            INSERT INTO workspace_mapping (
                user_id, user_name, container_name, api_url
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                user_name = EXCLUDED.user_name,
                container_name = EXCLUDED.container_name,
                api_url = EXCLUDED.api_url,
                updated_at = NOW()
            RETURNING *
            """,
            (user_id, user_name, container_name, api_url),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "saved workspace_mapping")

    @staticmethod
    def get(
        user_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM workspace_mapping WHERE user_id = %s",
            (user_id,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def get_by_user_name(
        user_name: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any] | None:
        return fetch_one(
            "SELECT * FROM workspace_mapping WHERE user_name = %s",
            (user_name,),
            dsn=dsn,
            connection=connection,
        )

    @staticmethod
    def update_user_name(
        user_id: str,
        user_name: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE workspace_mapping
            SET user_name = %s, updated_at = NOW()
            WHERE user_id = %s RETURNING *
            """,
            (user_name, user_id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "workspace_mapping")

    @staticmethod
    def update_container_name(
        user_id: str,
        container_name: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE workspace_mapping
            SET container_name = %s, updated_at = NOW()
            WHERE user_id = %s RETURNING *
            """,
            (container_name, user_id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "workspace_mapping")

    @staticmethod
    def update_api_url(
        user_id: str,
        api_url: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            UPDATE workspace_mapping
            SET api_url = %s, updated_at = NOW()
            WHERE user_id = %s RETURNING *
            """,
            (api_url, user_id),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "workspace_mapping")

    @staticmethod
    def delete(
        user_id: str,
        *,
        dsn: str | None = None,
        connection=None,
    ) -> dict[str, Any]:
        row = fetch_one(
            """
            DELETE FROM workspace_mapping
            WHERE user_id = %s RETURNING *
            """,
            (user_id,),
            dsn=dsn,
            connection=connection,
        )
        return require_row(row, "workspace_mapping")
