import asyncio

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from db import TABLE_DDL
from data_refiner_api.api.routes import schemas, user_workspaces
from data_refiner_api.api.routes.health import health
from data_refiner_api.api.routes.user_workspaces import UserContainerRequest
from data_refiner_api.main import create_app, lifespan
from data_refiner_api.models.schemas import FileSchemaRequest, TableSchemaRequest
from data_refiner_api.services import user_workspaces as user_container_service
from data_refiner_api.services.schema_inference import _file_suffix
from main import main
from main import app as root_app


def test_health() -> None:
    assert health() == {"status": "ok"}


def test_file_schema_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        schemas.schema_inference,
        "infer_file_schema",
        lambda path: f"schema for {path}",
    )

    response = schemas.read_file_schema(
        FileSchemaRequest(file_path="hdfs:///warehouse/events")
    )

    assert response.model_dump() == {
        "result": "schema for hdfs:///warehouse/events"
    }


def test_table_schema_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        schemas.schema_inference,
        "infer_table_schema",
        lambda table: f"schema for {table}",
    )

    response = schemas.read_table_schema(
        TableSchemaRequest(table_name="db.events")
    )

    assert response.model_dump() == {"result": "schema for db.events"}


def test_table_schema_includes_bounded_examples(monkeypatch) -> None:
    class DataType:
        @staticmethod
        def simpleString() -> str:
            return "string"

    class Field:
        name = "status"
        dataType = DataType()
        nullable = True

    class Frame:
        schema = type("Schema", (), {"fields": [Field()]})()

        @staticmethod
        def _show_string(*, n: int, truncate: bool) -> str:
            assert (n, truncate) == (5, True)
            return "|valid|"

    class Spark:
        def table(self, table_name: str) -> Frame:
            assert table_name == "db.events"
            return Frame()

        def stop(self) -> None:
            pass

    class Builder:
        def appName(self, _: str) -> "Builder":
            return self

        def enableHiveSupport(self) -> "Builder":
            return self

        @staticmethod
        def getOrCreate() -> Spark:
            return Spark()

    monkeypatch.setattr(
        schemas.schema_inference,
        "SparkSession",
        type("SparkSession", (), {"builder": Builder()}),
    )

    result = schemas.schema_inference.infer_table_schema("db.events")

    assert "name=status, type=string, nullable=True" in result
    assert "Data example:\n|valid|" in result


def test_data_metadata_schema_accepts_test_output() -> None:
    data_metadata_ddl = next(
        statement
        for statement in TABLE_DDL
        if "CREATE TABLE IF NOT EXISTS data_metadata" in statement
    )

    assert "'test_output'" in data_metadata_ddl


def test_empty_file_path_is_rejected() -> None:
    with pytest.raises(ValidationError):
        FileSchemaRequest(file_path="")


def test_spark_error_is_returned_as_http_error(monkeypatch) -> None:
    def fail(_: str) -> str:
        raise RuntimeError("HDFS is unavailable")

    monkeypatch.setattr(schemas.schema_inference, "infer_file_schema", fail)

    with pytest.raises(HTTPException) as exc_info:
        schemas.read_file_schema(FileSchemaRequest(file_path="hdfs:///data"))

    assert exc_info.value.status_code == 500
    assert (
        exc_info.value.detail
        == "Failed to infer file schema: HDFS is unavailable"
    )


def test_application_registers_public_routes() -> None:
    paths = create_app().openapi()["paths"]

    assert "get" in paths["/health"]
    assert "post" in paths["/base-datasets"]
    assert "post" in paths["/base-datasets/search"]
    assert "post" in paths["/schemas/file"]
    assert "post" in paths["/schemas/table"]
    assert "post" in paths["/operators/market"]
    assert "post" in paths["/operators/installed-packages"]
    assert "get" in paths[
        "/operators/meta-operator/{document_name}/docs"
    ]
    assert "post" in paths[
        "/operators/{operator_type}/{operator_name}/docs"
    ]
    assert "post" in paths[
        "/operators/{operator_type}/{operator_name}/code"
    ]
    assert "post" in paths[
        "/operators/{operator_type}/{operator_name}/test-code"
    ]
    assert "post" in paths[
        "/operators/processing_operator/{operator_name}/docs"
    ]
    assert "post" in paths[
        "/operators/processing_operator/{operator_name}/code"
    ]
    assert "post" in paths[
        "/operators/processing_operator/{operator_name}/test-code"
    ]
    assert "get" in paths["/pipelines/example"]
    assert "post" in paths["/pipelines/validate"]
    assert "post" in paths["/pipelines/run"]
    assert "post" in paths["/resources/hdfs"]
    assert "post" in paths["/resources/hive"]
    assert "get" in paths["/resources/cluster"]
    assert "post" in paths["/user-containers"]
    assert "post" in paths["/workspace/write-operator-code"]
    assert "post" in paths["/workspace/write-operator-test-code"]
    assert "post" in paths["/workspace/write-pipeline"]
    assert "post" in paths["/workspace/run-pytest"]
    assert "post" in paths["/workspace/run-doc-checker"]
    assert "post" in paths["/workspace/add-package"]
    assert "post" in paths["/workspace/sync-workspace"]
    assert "post" in paths["/workspace/sync-conda-env"]


def test_application_initializes_database_on_startup(monkeypatch) -> None:
    executed: list[str] = []
    synced: list[str] = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, statement: str) -> None:
            executed.append(statement)

    monkeypatch.setattr(
        "db.psycopg.connect",
        lambda dsn, connect_timeout: Connection(),
    )
    monkeypatch.setattr(
        "data_refiner_api.main.sync_pipeline_cases",
        lambda: synced.append("pipeline-cases"),
    )
    monkeypatch.setattr(
        "data_refiner_api.main.sync_base_datasets",
        lambda: synced.append("base-datasets"),
    )

    async def start_application() -> None:
        async with lifespan(create_app()):
            pass

    asyncio.run(start_application())

    assert executed == list(TABLE_DDL)
    assert synced == ["pipeline-cases", "base-datasets"]


def test_user_container_mapping(monkeypatch) -> None:
    monkeypatch.setattr(
        user_workspaces.user_workspaces,
        "save_mapping",
        lambda user_id, container_name: {
            "user_id": user_id,
            "container_name": container_name,
            "api_url": "http://localhost:19611",
            "updated_at": "2026-08-02T12:00:00+08:00",
        },
    )

    response = user_workspaces.save_user_container(
        UserContainerRequest(
            user_id="user-1",
            container_name="hiclaw-worker-pipeline-agent",
        )
    )

    assert response.api_url == "http://localhost:19611"


def test_container_api_url_uses_container_name_and_health_check(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

    def fake_urlopen(url: str, timeout: int) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(user_container_service, "urlopen", fake_urlopen)

    assert user_container_service._container_api_url("worker") == (
        "http://worker:8000"
    )
    assert calls == [("http://worker:8000/health", 5)]


def test_matrix_user_id_maps_to_workspace_user_name() -> None:
    assert user_container_service.normalize_user_id(
        "@admin:matrix-local.agentteams.io:18081"
    ) == "admin"
    assert user_container_service.normalize_user_id("admin") == "admin"

    with pytest.raises(ValueError):
        user_container_service.normalize_user_id("../admin")


def test_save_mapping_stores_user_id_and_user_name(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, query: str, params: tuple[str, ...]) -> None:
            calls.append((query, params))

        def fetchone(self):
            return (
                "@admin:matrix-local.agentteams.io:18081",
                "worker",
                "http://workspace",
                "2026-08-11T00:00:00Z",
            )

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def cursor(self) -> Cursor:
            return Cursor()

    monkeypatch.setattr(
        user_container_service,
        "_container_api_url",
        lambda container_name: "http://workspace",
    )
    monkeypatch.setattr(
        user_container_service.psycopg,
        "connect",
        lambda dsn, connect_timeout: Connection(),
    )

    mapping = user_container_service.save_mapping(
        "@admin:matrix-local.agentteams.io:18081",
        "worker",
    )

    assert mapping["user_id"] == "@admin:matrix-local.agentteams.io:18081"
    assert calls[0][1] == (
        "@admin:matrix-local.agentteams.io:18081",
        "admin",
        "worker",
        "http://workspace",
    )
    assert "ON CONFLICT (user_name)" in calls[0][0]


def test_get_api_url_queries_normalized_user_id(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...]]] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, query: str, params: tuple[str, ...]) -> None:
            calls.append((query, params))

        def fetchone(self):
            return ("http://workspace",)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def cursor(self) -> Cursor:
            return Cursor()

    monkeypatch.setattr(
        user_container_service.psycopg,
        "connect",
        lambda dsn, connect_timeout: Connection(),
    )

    assert user_container_service.get_api_url(
        "@admin:matrix-local.agentteams.io:18081"
    ) == "http://workspace"
    assert calls == [
        (
            "SELECT api_url FROM workspace_mapping WHERE user_name = %s",
            ("admin",),
        )
    ]


def test_invalid_container_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        UserContainerRequest(
            user_id="user-1",
            container_name="worker;docker ps",
        )


def test_root_asgi_entrypoint_remains_available() -> None:
    assert root_app.title == "Data Refiner API"


def test_root_script_entrypoint_starts_uvicorn(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(application: str, **kwargs: object) -> None:
        calls.append({"application": application, **kwargs})

    monkeypatch.setattr("main.uvicorn.run", fake_run)

    main()

    assert calls == [
        {
            "application": "data_refiner_api.main:app",
            "host": "0.0.0.0",
            "port": 8000,
        }
    ]


def test_hdfs_file_suffix() -> None:
    assert _file_suffix("part-00000.snappy.parquet") == ".parquet"
