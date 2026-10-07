import json

import pytest
from fastapi import HTTPException

from data_refiner_api.api.routes import workspace_operations
from data_refiner_api.models.workspace import (
    AddPackageRequest,
    RunPytestRequest,
    WorkspaceRequest,
    WritePipelineRequest,
    WriteOperatorRequest,
)
from data_refiner_api.services import workspace_proxy

MATRIX_USER_ID = "@admin:matrix-local.agentteams.io:18081"


def test_workspace_proxy_posts_json_to_api_url(monkeypatch) -> None:
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def read(self) -> bytes:
            return b'{"success":true}'

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return Response()

    monkeypatch.setattr(workspace_proxy, "urlopen", fake_urlopen)

    result = workspace_proxy.post_url(
        "http://workspace/",
        "/pipeline-validation",
        {"pipeline_config_yaml_string": "node: {}"},
    )

    request, timeout = calls[0]
    assert result == {"success": True}
    assert request.full_url == "http://workspace/pipeline-validation"
    assert json.loads(request.data) == {
        "pipeline_config_yaml_string": "node: {}"
    }
    assert timeout == workspace_proxy.DEFAULT_TIMEOUT


def test_workspace_operations_forward_to_mapped_container(monkeypatch) -> None:
    calls: list[tuple[str, str, object, float]] = []

    def fake_post(user_id, path, payload, *, timeout):
        calls.append((user_id, path, payload, timeout))
        return {"result": "ok"}

    monkeypatch.setattr(workspace_operations.workspace_proxy, "post", fake_post)

    assert workspace_operations.write_operator_code(
        WriteOperatorRequest(
            user_id=MATRIX_USER_ID,
            operator_type="filter",
            operator_name="length_filter",
            content="code",
        )
    ) == {"result": "ok"}
    assert workspace_operations.write_operator_test_code(
        WriteOperatorRequest(
            user_id=MATRIX_USER_ID,
            operator_type="filter",
            operator_name="length_filter",
            content="test",
        )
    ) == {"result": "ok"}
    assert workspace_operations.write_pipeline(
        WritePipelineRequest(
            user_id=MATRIX_USER_ID,
            pipeline_config_yaml_string="node:\n  op_name: sample\n",
        )
    ) == {"result": "ok"}
    assert workspace_operations.run_pytest(
        RunPytestRequest(
            user_id=MATRIX_USER_ID,
            path="workspace/tests/filter/test_length_filter.py",
        )
    ) == {"result": "ok"}
    assert workspace_operations.run_doc_checker(
        WorkspaceRequest(user_id=MATRIX_USER_ID)
    ) == {"result": "ok"}
    assert workspace_operations.add_package(
        AddPackageRequest(user_id=MATRIX_USER_ID, package_name="requests")
    ) == {"result": "ok"}
    assert workspace_operations.sync_workspace(
        WorkspaceRequest(user_id=MATRIX_USER_ID)
    ) == {"result": "ok"}
    assert workspace_operations.sync_conda_env(
        WorkspaceRequest(user_id=MATRIX_USER_ID)
    ) == {"result": "ok"}

    assert calls == [
        (
            MATRIX_USER_ID,
            "/write-operator-code",
            {
                "operator_type": "filter",
                "operator_name": "length_filter",
                "content": "code",
            },
            300,
        ),
        (
            MATRIX_USER_ID,
            "/write-operator-test-code",
            {
                "operator_type": "filter",
                "operator_name": "length_filter",
                "content": "test",
            },
            300,
        ),
        (
            MATRIX_USER_ID,
            "/write-pipeline",
            {"pipeline_config_yaml_string": "node:\n  op_name: sample\n"},
            300,
        ),
        (
            MATRIX_USER_ID,
            "/run-pytest",
            {"path": "workspace/tests/filter/test_length_filter.py"},
            1000,
        ),
        (MATRIX_USER_ID, "/run-doc-checker", None, 300),
        (
            MATRIX_USER_ID,
            "/add-package",
            {"package_name": "requests"},
            610,
        ),
        (
            MATRIX_USER_ID,
            "/sync-workspace",
            {"user_name": "admin"},
            300,
        ),
        (
            MATRIX_USER_ID,
            "/sync-conda-env",
            {"user_name": "admin"},
            1900,
        ),
    ]


def test_add_package_forwards_exact_version(monkeypatch) -> None:
    calls = []

    def fake_post(user_id, path, payload, *, timeout):
        calls.append((user_id, path, payload, timeout))
        return {"result": "ok"}

    monkeypatch.setattr(workspace_operations.workspace_proxy, "post", fake_post)

    result = workspace_operations.add_package(
        AddPackageRequest(
            user_id=MATRIX_USER_ID,
            package_name="pypinyin",
            version="0.55.0",
        )
    )

    assert result == {"result": "ok"}
    assert calls == [
        (
            MATRIX_USER_ID,
            "/add-package",
            {"package_name": "pypinyin", "version": "0.55.0"},
            610,
        )
    ]


def test_add_package_rejects_invalid_name_and_version() -> None:
    with pytest.raises(ValueError):
        AddPackageRequest(user_id=MATRIX_USER_ID, package_name="--dev")
    with pytest.raises(ValueError):
        AddPackageRequest(
            user_id=MATRIX_USER_ID,
            package_name="pypinyin",
            version="latest stable",
        )


def test_workspace_proxy_error_is_forwarded(monkeypatch) -> None:
    def fail(*args, **kwargs):
        raise workspace_operations.workspace_proxy.WorkspaceProxyError(
            409,
            {"detail": "workspace busy"},
        )

    monkeypatch.setattr(workspace_operations.workspace_proxy, "post", fail)

    with pytest.raises(HTTPException) as exc_info:
        workspace_operations.run_doc_checker(
            WorkspaceRequest(user_id="admin")
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {"detail": "workspace busy"}
