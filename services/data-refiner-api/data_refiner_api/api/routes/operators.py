"""Operator market and documentation endpoints."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path

from data_refiner_api.models.operations import (
    InstalledPackagesRequest,
    InstalledPackagesResponse,
    OperatorDocsRequest,
    OperatorMarketRequest,
    OperatorType,
    TextResponse,
)
from data_refiner_api.services import (
    operator_catalog,
    user_workspaces,
    workspace_proxy,
)

router = APIRouter(tags=["operators"])

ProcessingOperatorName = Annotated[
    str,
    Path(
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Processing operator module name without an extension.",
    ),
]


@router.post("/market", response_model=TextResponse)
def get_ops_market(
    request: OperatorMarketRequest,
) -> TextResponse:
    try:
        main_market = operator_catalog.get_ops_market()
        api_url = user_workspaces.get_api_url(request.user_id)
        sub_market = operator_catalog.get_sub_ops_market(api_url)
        result = (
            "-----------Main Operator Market-----------\n\n"
            f"{main_market}\n"
            "-----------Sub Operator Market-----------\n\n"
            f"{sub_market}"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator market: {exc}",
        ) from exc
    return TextResponse(result=result)


@router.post(
    "/installed-packages",
    response_model=InstalledPackagesResponse,
)
def get_installed_packages(
    request: InstalledPackagesRequest,
) -> InstalledPackagesResponse:
    try:
        api_url = user_workspaces.get_api_url(request.user_id)
        return operator_catalog.get_sub_installed_packages(api_url)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read installed packages: {exc}",
        ) from exc


@router.get(
    "/meta-operator/{document_name}/docs",
    response_model=TextResponse,
)
def get_meta_operator_docs(
    document_name: Annotated[
        str,
        Path(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Document name without the .md extension.",
        ),
    ],
) -> TextResponse:
    try:
        result = operator_catalog.get_meta_operator_docs(document_name)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read meta operator document: {exc}",
        ) from exc
    if result == operator_catalog.DOCUMENT_NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail=f"Meta operator document not found: {document_name}",
        )
    return TextResponse(result=result)


@router.post(
    "/processing_operator/{operator_name}/docs",
    response_model=TextResponse,
)
def get_processing_operator_docs_by_name(
    operator_name: ProcessingOperatorName,
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_docs_by_name(
            operator_name
        )
        if result == operator_catalog.DOCUMENT_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = operator_catalog.get_sub_processing_operator_docs_by_name(
                api_url,
                operator_name,
            )
    except operator_catalog.AmbiguousOperatorNameError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except workspace_proxy.WorkspaceProxyError as exc:
        raise HTTPException(exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator document: {exc}",
        ) from exc
    if result == operator_catalog.DOCUMENT_NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail=f"Processing operator document not found: {operator_name}",
        )
    return TextResponse(result=result)


@router.post(
    "/processing_operator/{operator_name}/code",
    response_model=TextResponse,
)
def get_processing_operator_code_by_name(
    operator_name: ProcessingOperatorName,
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_code_by_name(
            operator_name
        )
        if result == operator_catalog.SOURCE_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = operator_catalog.get_sub_processing_operator_code_by_name(
                api_url,
                operator_name,
            )
    except operator_catalog.AmbiguousOperatorNameError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except workspace_proxy.WorkspaceProxyError as exc:
        raise HTTPException(exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator code: {exc}",
        ) from exc
    if result == operator_catalog.SOURCE_NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail=f"Processing operator source not found: {operator_name}",
        )
    return TextResponse(result=result)


@router.post(
    "/processing_operator/{operator_name}/test-code",
    response_model=TextResponse,
)
def get_processing_operator_test_code_by_name(
    operator_name: ProcessingOperatorName,
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_test_code_by_name(
            operator_name
        )
        if result == operator_catalog.SOURCE_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = (
                operator_catalog.get_sub_processing_operator_test_code_by_name(
                    api_url,
                    operator_name,
                )
            )
    except operator_catalog.AmbiguousOperatorNameError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except workspace_proxy.WorkspaceProxyError as exc:
        raise HTTPException(exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator test code: {exc}",
        ) from exc
    if result == operator_catalog.SOURCE_NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail=f"Processing operator test source not found: {operator_name}",
        )
    return TextResponse(result=result)


@router.post(
    "/{operator_type}/{operator_name}/docs",
    response_model=TextResponse,
)
def get_processing_operator_docs(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        Path(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the .md extension.",
        ),
    ],
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_docs(
            operator_type,
            operator_name,
        )
        if result == operator_catalog.DOCUMENT_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = operator_catalog.get_sub_processing_operator_docs(
                api_url,
                operator_type,
                operator_name,
            )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator document: {exc}",
        ) from exc
    return TextResponse(result=result)


@router.post(
    "/{operator_type}/{operator_name}/code",
    response_model=TextResponse,
)
def get_processing_operator_code(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        Path(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the .py extension.",
        ),
    ],
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_code(
            operator_type,
            operator_name,
        )
        if result == operator_catalog.SOURCE_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = operator_catalog.get_sub_processing_operator_code(
                api_url,
                operator_type,
                operator_name,
            )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator code: {exc}",
        ) from exc
    return TextResponse(result=result)


@router.post(
    "/{operator_type}/{operator_name}/test-code",
    response_model=TextResponse,
)
def get_processing_operator_test_code(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        Path(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the test_ prefix.",
        ),
    ],
    request: OperatorDocsRequest,
) -> TextResponse:
    try:
        result = operator_catalog.get_processing_operator_test_code(
            operator_type,
            operator_name,
        )
        if result == operator_catalog.SOURCE_NOT_FOUND:
            api_url = user_workspaces.get_api_url(request.user_id)
            result = operator_catalog.get_sub_processing_operator_test_code(
                api_url,
                operator_type,
                operator_name,
            )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator test code: {exc}",
        ) from exc
    return TextResponse(result=result)
