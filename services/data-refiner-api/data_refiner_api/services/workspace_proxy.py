"""Proxy requests to a user's workspace API."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from data_refiner_api.services import user_workspaces

DEFAULT_TIMEOUT = 300
ADD_PACKAGE_TIMEOUT = 610
PYTEST_TIMEOUT = 1000
CONDA_TIMEOUT = 1900


class WorkspaceProxyError(RuntimeError):
    """An error returned while calling a user workspace API."""

    def __init__(self, status_code: int, detail: Any) -> None:
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


def _json_response(raw: bytes, error: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WorkspaceProxyError(502, error) from exc
    if not isinstance(payload, dict):
        raise WorkspaceProxyError(502, error)
    return payload


def _http_error_detail(error: HTTPError) -> Any:
    raw = error.read()
    if not raw:
        return str(error.reason)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw.decode(errors="replace")


def post_url(
    api_url: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """POST JSON to a workspace API URL."""
    body = None if payload is None else json.dumps(payload).encode()
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(
        f"{api_url.rstrip('/')}{path}",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            return _json_response(
                response.read(),
                "Workspace API returned invalid JSON",
            )
    except HTTPError as exc:
        raise WorkspaceProxyError(
            exc.code,
            _http_error_detail(exc),
        ) from exc
    except (TimeoutError, URLError) as exc:
        raise WorkspaceProxyError(
            504,
            f"Workspace API request failed: {exc}",
        ) from exc


def post(
    user_id: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """POST JSON to the workspace mapped to ``user_id``."""
    return post_url(
        user_workspaces.get_api_url(user_id),
        path,
        payload,
        timeout=timeout,
    )
