import pytest
from fastapi import HTTPException
from pydantic import TypeAdapter, ValidationError

from data_refiner_api.api.routes import operators, pipelines, resources
from data_refiner_api.models.operations import (
    InstalledPackagesRequest,
    InstalledPackagesResponse,
    OperatorDocsRequest,
    OperatorMarketRequest,
    OperatorType,
    PipelineRunRequest,
    PipelineValidationRequest,
    PipelineValidationResponse,
)
from data_refiner_api.models.resources import (
    HdfsResourceRequest,
    HiveResourceRequest,
)


def test_post_operator_market(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        operators.operator_catalog,
        "get_ops_market",
        lambda: "main market",
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_ops_market",
        lambda api_url: calls.append(("market", api_url)) or "sub market",
    )

    response = operators.get_ops_market(
        OperatorMarketRequest(user_id="user-1")
    )

    assert response.model_dump() == {
        "result": (
            "-----------Main Operator Market-----------\n\n"
            "main market\n"
            "-----------Sub Operator Market-----------\n\n"
            "sub market"
        )
    }
    assert calls == [("mapping", "user-1"), ("market", "http://workspace")]


def test_post_installed_packages(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_installed_packages",
        lambda api_url: calls.append(("installed-packages", api_url))
        or InstalledPackagesResponse(
            python="/usr/bin/python",
            packages=[{"name": "alpha", "version": "1.0"}],
        ),
    )

    response = operators.get_installed_packages(
        InstalledPackagesRequest(user_id="user-1")
    )

    assert response.model_dump() == {
        "python": "/usr/bin/python",
        "packages": [{"name": "alpha", "version": "1.0"}],
    }
    assert calls == [
        ("mapping", "user-1"),
        ("installed-packages", "http://workspace"),
    ]


def test_get_meta_operator_document() -> None:
    response = operators.get_meta_operator_docs("filter")

    assert "Filter Operator" in response.result


def test_missing_meta_operator_document_returns_404() -> None:
    with pytest.raises(HTTPException) as exc_info:
        operators.get_meta_operator_docs("does_not_exist")

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == (
        "Meta operator document not found: does_not_exist"
    )


def test_get_sub_operator_market(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def read(self) -> bytes:
            return b'{"result":"sub market"}'

    def fake_urlopen(url: str, timeout: int) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(operators.operator_catalog, "urlopen", fake_urlopen)

    assert operators.operator_catalog.get_sub_ops_market("http://workspace/") == (
        "sub market"
    )
    assert calls == [("http://workspace/ops_market", 5)]


def test_get_sub_installed_packages(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def read(self) -> bytes:
            return b'{"python":"/usr/bin/python","packages":[]}'

    def fake_urlopen(url: str, timeout: int) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(operators.operator_catalog, "urlopen", fake_urlopen)

    assert operators.operator_catalog.get_sub_installed_packages(
        "http://workspace/"
    ).model_dump() == {
        "python": "/usr/bin/python",
        "packages": [],
    }
    assert calls == [("http://workspace/installed-packages", 5)]


def test_post_operator_document_uses_main_document(monkeypatch) -> None:
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_docs",
        lambda operator_type, operator_name: f"{operator_type}/{operator_name}",
    )

    response = operators.get_processing_operator_docs(
        "deduplicator",
        "exact_deduplicator",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {
        "result": "deduplicator/exact_deduplicator"
    }


def test_post_operator_document_by_name_falls_back_to_workspace(
    monkeypatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_docs_by_name",
        lambda operator_name: operators.operator_catalog.DOCUMENT_NOT_FOUND,
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_processing_operator_docs_by_name",
        lambda api_url, operator_name: calls.append(
            ("docs", api_url, operator_name)
        )
        or "workspace docs",
    )

    response = operators.get_processing_operator_docs_by_name(
        "custom_mapper",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {"result": "workspace docs"}
    assert calls == [
        ("mapping", "user-1"),
        ("docs", "http://workspace", "custom_mapper"),
    ]


def test_operator_lookup_preserves_workspace_not_found(monkeypatch) -> None:
    def missing_workspace_operator(api_url, operator_name):
        raise operators.workspace_proxy.WorkspaceProxyError(404, "not found")

    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_docs_by_name",
        lambda operator_name: operators.operator_catalog.DOCUMENT_NOT_FOUND,
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_processing_operator_docs_by_name",
        missing_workspace_operator,
    )

    with pytest.raises(HTTPException) as exc_info:
        operators.get_processing_operator_docs_by_name(
            "missing_operator",
            OperatorDocsRequest(user_id="user-1"),
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "not found"


def test_post_operator_document_falls_back_to_workspace(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def missing_document(operator_type, operator_name):
        return operators.operator_catalog.DOCUMENT_NOT_FOUND

    def get_api_url(user_id: str) -> str:
        calls.append(("mapping", user_id))
        return "http://workspace"

    def get_sub_document(api_url, operator_type, operator_name):
        calls.append(("docs", api_url, operator_type, operator_name))
        return "sub docs"

    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_docs",
        missing_document,
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        get_api_url,
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_processing_operator_docs",
        get_sub_document,
    )

    response = operators.get_processing_operator_docs(
        "mapper",
        "custom_mapper",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {"result": "sub docs"}
    assert calls == [
        ("mapping", "user-1"),
        ("docs", "http://workspace", "mapper", "custom_mapper"),
    ]


def test_post_operator_code_uses_main_source(monkeypatch) -> None:
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_code",
        lambda operator_type, operator_name: (
            f"{operator_type}/{operator_name}.py"
        ),
    )

    response = operators.get_processing_operator_code(
        "filter",
        "length_filter",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {
        "result": "filter/length_filter.py",
    }


def test_post_operator_code_falls_back_to_workspace(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_code",
        lambda operator_type, operator_name: operators.operator_catalog.SOURCE_NOT_FOUND,
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_processing_operator_code",
        lambda api_url, operator_type, operator_name: calls.append(
            ("code", api_url, operator_type, operator_name)
        )
        or "sub code",
    )

    response = operators.get_processing_operator_code(
        "mapper",
        "custom_mapper",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {"result": "sub code"}
    assert calls == [
        ("mapping", "user-1"),
        ("code", "http://workspace", "mapper", "custom_mapper"),
    ]


def test_post_operator_test_code_uses_main_source(monkeypatch) -> None:
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_test_code",
        lambda operator_type, operator_name: (
            f"tests/{operator_type}/test_{operator_name}.py"
        ),
    )

    response = operators.get_processing_operator_test_code(
        "filter",
        "length_filter",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {
        "result": "tests/filter/test_length_filter.py",
    }


def test_post_operator_test_code_falls_back_to_workspace(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        operators.operator_catalog,
        "get_processing_operator_test_code",
        lambda operator_type, operator_name: operators.operator_catalog.SOURCE_NOT_FOUND,
    )
    monkeypatch.setattr(
        operators.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        operators.operator_catalog,
        "get_sub_processing_operator_test_code",
        lambda api_url, operator_type, operator_name: calls.append(
            ("test-code", api_url, operator_type, operator_name)
        )
        or "sub test code",
    )

    response = operators.get_processing_operator_test_code(
        "mapper",
        "custom_mapper",
        OperatorDocsRequest(user_id="user-1"),
    )

    assert response.model_dump() == {"result": "sub test code"}
    assert calls == [
        ("mapping", "user-1"),
        ("test-code", "http://workspace", "mapper", "custom_mapper"),
    ]


def test_get_sub_operator_document(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def read(self) -> bytes:
            return b'{"result":"sub docs"}'

    def fake_urlopen(url: str, timeout: int) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(operators.operator_catalog, "urlopen", fake_urlopen)

    assert operators.operator_catalog.get_sub_processing_operator_docs(
        "http://workspace/",
        "mapper",
        "custom_mapper",
    ) == "sub docs"
    assert calls == [
        ("http://workspace/mapper/custom_mapper/docs", 5)
    ]


def test_get_sub_operator_sources(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class Response:
        def __init__(self, value: str) -> None:
            self.value = value

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def read(self) -> bytes:
            return f'{{"result":"{self.value}"}}'.encode()

    def fake_urlopen(url: str, timeout: int) -> Response:
        calls.append((url, timeout))
        return Response("source")

    monkeypatch.setattr(operators.operator_catalog, "urlopen", fake_urlopen)

    assert operators.operator_catalog.get_sub_processing_operator_code(
        "http://workspace/",
        "mapper",
        "custom_mapper",
    ) == "source"
    assert operators.operator_catalog.get_sub_processing_operator_test_code(
        "http://workspace/",
        "mapper",
        "custom_mapper",
    ) == "source"
    assert calls == [
        ("http://workspace/mapper/custom_mapper/code", 5),
        ("http://workspace/mapper/custom_mapper/test-code", 5),
    ]


def test_invalid_operator_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(OperatorType).validate_python("unknown")


def test_get_pipeline_example(monkeypatch) -> None:
    monkeypatch.setattr(
        pipelines.pipeline_operations,
        "get_pipeline_example",
        lambda: "example: true",
    )

    response = pipelines.get_pipeline_example()

    assert response.model_dump() == {"result": "example: true"}


def test_validate_pipeline(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        pipelines.user_workspaces,
        "get_api_url",
        lambda user_id: calls.append(("mapping", user_id))
        or "http://workspace",
    )
    monkeypatch.setattr(
        pipelines.pipeline_operations,
        "validate_workspace_pipeline",
        lambda api_url, value: calls.append(("validation", api_url, value))
        or PipelineValidationResponse(
            success=True,
            message=f"validated {value}",
        ),
    )

    response = pipelines.validate_pipeline(
        PipelineValidationRequest(
            user_id="user-1",
            pipeline_config_yaml_string="node: {}",
        )
    )

    assert response.model_dump(exclude_none=True) == {
        "success": True,
        "message": "validated node: {}",
    }
    assert calls == [
        ("mapping", "user-1"),
        ("validation", "http://workspace", "node: {}"),
    ]


def test_pipeline_validation_failure_remains_a_business_result(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        pipelines.user_workspaces,
        "get_api_url",
        lambda _: "http://workspace",
    )
    monkeypatch.setattr(
        pipelines.pipeline_operations,
        "validate_workspace_pipeline",
        lambda *_: PipelineValidationResponse(
            success=False,
            error="unknown operator",
        ),
    )

    response = pipelines.validate_pipeline(
        PipelineValidationRequest(
            user_id="user-1",
            pipeline_config_yaml_string="node: {}",
        )
    )

    assert response.model_dump(exclude_none=True) == {
        "success": False,
        "error": "unknown operator",
    }


def test_run_pipeline(monkeypatch) -> None:
    called_with: list[tuple[str, str, list[str]]] = []

    def fake_run(
        user_id: str,
        pipeline_name: str,
        spark_runtime_config: list[str],
    ) -> str:
        called_with.append((user_id, pipeline_name, spark_runtime_config))
        return "application_1740000000000_0042"

    monkeypatch.setattr(
        pipelines.pipeline_operations,
        "run_pipeline",
        fake_run,
    )

    response = pipelines.run_pipeline(
        PipelineRunRequest(
            user_id="@admin:matrix-local.agentteams.io",
            pipeline_name="xxx.yaml",
        )
    )

    assert response.model_dump() == {
        "application_id": "application_1740000000000_0042",
    }
    assert called_with == [
        ("@admin:matrix-local.agentteams.io", "xxx.yaml", []),
    ]


def test_invalid_pipeline_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PipelineRunRequest(
            user_id="admin",
            pipeline_name="../xxx.yaml",
        )


def test_hdfs_data_size(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        resources.resource_planning,
        "get_hdfs_size",
        lambda path: calls.append(path) or 1024,
    )

    response = resources.read_hdfs_size(
        HdfsResourceRequest(hdfs_path="hdfs:///data/events")
    )

    assert response.model_dump() == {"size_bytes": 1024}
    assert calls == ["hdfs:///data/events"]


def test_hive_data_size(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        resources.resource_planning,
        "get_hive_size",
        lambda table: calls.append(table) or 2048,
    )

    response = resources.read_hive_size(
        HiveResourceRequest(table_name="analytics.events")
    )

    assert response.model_dump() == {"size_bytes": 2048}
    assert calls == ["analytics.events"]


def test_cluster_resources(monkeypatch) -> None:
    monkeypatch.setattr(
        resources.resource_planning,
        "get_cluster_resources",
        lambda: {
            "total_memory_mb": 131072,
            "available_memory_mb": 65536,
            "allocated_memory_mb": 65536,
            "total_vcores": 64,
            "available_vcores": 32,
            "allocated_vcores": 32,
        },
    )

    assert resources.read_cluster_resources().model_dump() == {
        "total_memory_mb": 131072,
        "available_memory_mb": 65536,
        "allocated_memory_mb": 65536,
        "total_vcores": 64,
        "available_vcores": 32,
        "allocated_vcores": 32,
    }


def test_pipeline_service_error_is_returned_as_http_error(
    monkeypatch,
) -> None:
    def fail() -> str:
        raise OSError("example is unavailable")

    monkeypatch.setattr(
        pipelines.pipeline_operations,
        "get_pipeline_example",
        fail,
    )

    with pytest.raises(HTTPException) as exc_info:
        pipelines.get_pipeline_example()

    assert exc_info.value.status_code == 500
    assert (
        exc_info.value.detail
        == "Failed to read pipeline example: example is unavailable"
    )
