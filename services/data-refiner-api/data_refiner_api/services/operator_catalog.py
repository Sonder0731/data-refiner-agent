"""Read built-in operator sources and documentation."""

import json
import logging
from importlib import resources
from typing import get_args
from urllib.error import HTTPError
from urllib.request import urlopen

from data_refiner_api.models.operations import (
    InstalledPackagesResponse,
    OperatorType,
)
from data_refiner_api.services.workspace_proxy import WorkspaceProxyError

logger = logging.getLogger(__name__)

DOCUMENT_NOT_FOUND = (
    "Document not found, please check operator type and operator name"
)
SOURCE_NOT_FOUND = (
    "Operator source not found, please check operator type and operator name"
)


class AmbiguousOperatorNameError(ValueError):
    """A processing operator name maps to more than one physical category."""


def get_ops_market() -> str:
    """Return the built-in operator market document."""
    logger.info("Reading operator market document")
    return (
        resources.files("data_refiner")
        .joinpath("docs/operator/ops_market.md")
        .read_text(encoding="utf-8")
    )


def _get_workspace_result(api_url: str, path: str, error: str) -> str:
    try:
        with urlopen(f"{api_url.rstrip('/')}{path}", timeout=5) as response:
            payload = json.load(response)
    except HTTPError as exc:
        try:
            payload = json.load(exc)
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = str(exc.reason)
        detail = (
            payload.get("detail", payload)
            if isinstance(payload, dict)
            else payload
        )
        raise WorkspaceProxyError(exc.code, detail) from exc
    if not isinstance(payload, dict) or not isinstance(
        payload.get("result"), str
    ):
        raise ValueError(error)
    return payload["result"]


def _read_package_text(path: str, not_found: str) -> str:
    try:
        return (
            resources.files("data_refiner")
            .joinpath(path)
            .read_text(encoding="utf-8")
        )
    except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
        return not_found


def resolve_processing_operator_type(operator_name: str) -> OperatorType | None:
    """Resolve a built-in processing operator's physical category by name."""
    package = resources.files("data_refiner")
    matches = [
        operator_type
        for operator_type in get_args(OperatorType)
        if package.joinpath(
            "ops", operator_type, f"{operator_name}.py"
        ).is_file()
    ]
    if len(matches) > 1:
        raise AmbiguousOperatorNameError(
            f"Ambiguous processing operator name: {operator_name}"
        )
    return matches[0] if matches else None


def get_sub_ops_market(api_url: str) -> str:
    """Return the operator market exposed by a user's workspace API."""
    return _get_workspace_result(
        api_url,
        "/ops_market",
        "Workspace operator market response is invalid",
    )


def get_sub_installed_packages(api_url: str) -> InstalledPackagesResponse:
    """Return installed packages exposed by a user's workspace API."""
    with urlopen(
        f"{api_url.rstrip('/')}/installed-packages",
        timeout=5,
    ) as response:
        return InstalledPackagesResponse.model_validate(json.load(response))


def get_sub_processing_operator_docs(
    api_url: str,
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return processing operator docs exposed by a user's workspace API."""
    return _get_workspace_result(
        api_url,
        f"/{operator_type}/{operator_name}/docs",
        "Workspace operator document response is invalid",
    )


def get_sub_processing_operator_docs_by_name(
    api_url: str,
    operator_name: str,
) -> str:
    """Return workspace operator docs without requiring its category."""
    return _get_workspace_result(
        api_url,
        f"/processing_operator/{operator_name}/docs",
        "Workspace operator document response is invalid",
    )


def get_sub_processing_operator_code(
    api_url: str,
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return processing operator code exposed by a user's workspace API."""
    return _get_workspace_result(
        api_url,
        f"/{operator_type}/{operator_name}/code",
        "Workspace operator code response is invalid",
    )


def get_sub_processing_operator_code_by_name(
    api_url: str,
    operator_name: str,
) -> str:
    """Return workspace operator code without requiring its category."""
    return _get_workspace_result(
        api_url,
        f"/processing_operator/{operator_name}/code",
        "Workspace operator code response is invalid",
    )


def get_sub_processing_operator_test_code(
    api_url: str,
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return processing operator test code exposed by a workspace API."""
    return _get_workspace_result(
        api_url,
        f"/{operator_type}/{operator_name}/test-code",
        "Workspace operator test code response is invalid",
    )


def get_sub_processing_operator_test_code_by_name(
    api_url: str,
    operator_name: str,
) -> str:
    """Return workspace operator test code without requiring its category."""
    return _get_workspace_result(
        api_url,
        f"/processing_operator/{operator_name}/test-code",
        "Workspace operator test code response is invalid",
    )


def get_processing_operator_docs(
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return the Markdown document for a processing operator."""
    logger.info("Reading %s operator document", operator_name)
    return _read_package_text(
        f"docs/operator/{operator_type}/{operator_name}.md",
        DOCUMENT_NOT_FOUND,
    )


def get_processing_operator_docs_by_name(operator_name: str) -> str:
    """Return built-in operator docs after resolving its category."""
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        return DOCUMENT_NOT_FOUND
    return get_processing_operator_docs(operator_type, operator_name)


def get_meta_operator_docs(document_name: str) -> str:
    """Return a meta operator Markdown document."""
    logger.info("Reading %s meta operator document", document_name)
    return _read_package_text(
        f"docs/operator/meta_operator/{document_name}.md",
        DOCUMENT_NOT_FOUND,
    )


def get_processing_operator_code(
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return the Python source for a processing operator."""
    logger.info("Reading %s operator code", operator_name)
    return _read_package_text(
        f"ops/{operator_type}/{operator_name}.py",
        SOURCE_NOT_FOUND,
    )


def get_processing_operator_code_by_name(operator_name: str) -> str:
    """Return built-in operator code after resolving its category."""
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        return SOURCE_NOT_FOUND
    return get_processing_operator_code(operator_type, operator_name)


def get_processing_operator_test_code(
    operator_type: OperatorType,
    operator_name: str,
) -> str:
    """Return the test source for a processing operator."""
    logger.info("Reading %s operator test code", operator_name)
    return _read_package_text(
        f"tests/ops/{operator_type}/test_{operator_name}.py",
        SOURCE_NOT_FOUND,
    )


def get_processing_operator_test_code_by_name(operator_name: str) -> str:
    """Return built-in operator test code after resolving its category."""
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        return SOURCE_NOT_FOUND
    return get_processing_operator_test_code(operator_type, operator_name)
