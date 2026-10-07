"""Proxy user workspace development operations."""

from typing import Any

from fastapi import APIRouter, HTTPException

from data_refiner_api.models.workspace import (
    AddPackageRequest,
    RunPytestRequest,
    WorkspaceRequest,
    WritePipelineRequest,
    WriteOperatorRequest,
)
from data_refiner_api.services import user_workspaces, workspace_proxy

router = APIRouter(tags=["workspace"])


def _post(
    user_id: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = workspace_proxy.DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    try:
        return workspace_proxy.post(
            user_id,
            path,
            payload,
            timeout=timeout,
        )
    except workspace_proxy.WorkspaceProxyError as exc:
        raise HTTPException(exc.status_code, detail=exc.detail) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail="Failed to call workspace API",
        ) from exc


def _workspace_user_name(user_id: str) -> str:
    try:
        return user_workspaces.normalize_user_id(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/write-operator-code", response_model=dict[str, Any])
def write_operator_code(request: WriteOperatorRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/write-operator-code",
        request.model_dump(exclude={"user_id"}),
    )


@router.post("/write-operator-test-code", response_model=dict[str, Any])
def write_operator_test_code(request: WriteOperatorRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/write-operator-test-code",
        request.model_dump(exclude={"user_id"}),
    )


@router.post("/write-pipeline", response_model=dict[str, Any])
def write_pipeline(request: WritePipelineRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/write-pipeline",
        request.model_dump(exclude={"user_id"}),
    )


@router.post("/run-pytest", response_model=dict[str, Any])
def run_pytest(request: RunPytestRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/run-pytest",
        request.model_dump(exclude={"user_id"}),
        timeout=workspace_proxy.PYTEST_TIMEOUT,
    )


@router.post("/run-doc-checker", response_model=dict[str, Any])
def run_doc_checker(request: WorkspaceRequest) -> dict[str, Any]:
    return _post(request.user_id, "/run-doc-checker")


@router.post("/add-package", response_model=dict[str, Any])
def add_package(request: AddPackageRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/add-package",
        request.model_dump(exclude={"user_id"}, exclude_none=True),
        timeout=workspace_proxy.ADD_PACKAGE_TIMEOUT,
    )


@router.post("/sync-workspace", response_model=dict[str, Any])
def sync_workspace(request: WorkspaceRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/sync-workspace",
        {"user_name": _workspace_user_name(request.user_id)},
    )


@router.post("/sync-conda-env", response_model=dict[str, Any])
def sync_conda_env(request: WorkspaceRequest) -> dict[str, Any]:
    return _post(
        request.user_id,
        "/sync-conda-env",
        {"user_name": _workspace_user_name(request.user_id)},
        timeout=workspace_proxy.CONDA_TIMEOUT,
    )
