from collections.abc import Mapping
from typing import Any

import httpx


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 300,
) -> Any:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)

    request_arguments = {
        "headers": request_headers,
        "timeout": timeout,
    }
    if payload is not None:
        request_arguments["json"] = payload

    response = httpx.request(
        method,
        url,
        **request_arguments,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        try:
            body = response.json()
            detail = body.get("detail") if isinstance(body, dict) else body
        except ValueError:
            detail = response.text
        if detail:
            raise httpx.HTTPStatusError(
                f"{exc}\nService detail: {str(detail)[:2000]}",
                request=exc.request,
                response=exc.response,
            ) from exc
        raise
    return response.json()


def get_json(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 300,
) -> Any:
    return request_json(url, headers=headers, timeout=timeout)


def post_json(
    url: str,
    payload: Any,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float = 300,
) -> Any:
    return request_json(
        url,
        method="POST",
        payload=payload,
        headers=headers,
        timeout=timeout,
    )
