from collections.abc import Callable
from typing import Annotated, Any, TypeVar

import httpx
from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, ToolException, tool
from packaging.version import InvalidVersion, Version
from pydantic import AfterValidator, Field

from data_refiner_agent import tools as operations
from data_refiner_agent.workflow_types import (
    OperatorBuilderContext,
    PipelineAgentContext,
    PipelineTask,
)

T = TypeVar("T")

PackageName = Annotated[
    str,
    Field(
        pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$",
        description="Python distribution name without a version specifier.",
    ),
]


def _normalize_package_version(value: str) -> str:
    try:
        return str(Version(value))
    except InvalidVersion as exc:
        raise ValueError(
            "version must be a valid exact Python package version"
        ) from exc


PackageVersion = Annotated[
    str,
    Field(description="Exact Python package version, for example 0.55.0."),
    AfterValidator(_normalize_package_version),
]


def _builder_context(
    runtime: ToolRuntime[OperatorBuilderContext],
) -> OperatorBuilderContext:
    context = runtime.context
    if isinstance(context, OperatorBuilderContext):
        return context
    if isinstance(context, dict):
        return OperatorBuilderContext(**context)
    raise ValueError("OperatorBuilderContext is required for this tool")


def _pipeline_context(
    runtime: ToolRuntime[PipelineAgentContext],
) -> PipelineAgentContext:
    context = runtime.context
    if isinstance(context, PipelineAgentContext):
        return context
    if isinstance(context, dict):
        return PipelineAgentContext(**context)
    raise ValueError("PipelineAgentContext is required for this tool")


def _require_pipeline_task(
    context: PipelineAgentContext, *allowed_tasks: PipelineTask
) -> None:
    if context.task not in allowed_tasks:
        allowed = ", ".join(task.value for task in allowed_tasks)
        raise ToolException(
            f"This tool is only available for {allowed}; "
            f"the current task is {context.task.value}."
        )


def _pipeline_call(operation: Callable[[], T]) -> T:
    try:
        return operation()
    except ValueError as exc:
        raise ToolException(str(exc)) from exc


@tool("get_operator_build_reference_for_request")
def builder_get_operator_build_reference(
    runtime: ToolRuntime[OperatorBuilderContext],
) -> str:
    """Read this workflow's build guide for the exact current operator round."""
    context = _builder_context(runtime)
    row = operations.get_operator_build_reference_for_round(
        context.require_request_id(), context.operator_round
    )
    return str(row["content"])


@tool("get_operator_market_for_request")
def builder_get_operator_market(
    runtime: ToolRuntime[OperatorBuilderContext],
) -> str:
    """Read the current workflow user's built-in and workspace operator market."""
    return operations.get_operator_market_for_request(
        _builder_context(runtime).require_request_id()
    )


@tool("get_installed_packages_for_request")
def get_installed_packages_for_request(
    runtime: ToolRuntime[OperatorBuilderContext],
) -> dict[str, Any]:
    """Read packages installed in the current workflow user's workspace."""
    return operations.get_installed_packages_for_request(
        _builder_context(runtime).require_request_id()
    )


@tool("get_operator_implementation")
def get_operator_implementation(
    operator_name: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> str:
    """Read one operator implementation from the current user's catalog."""
    return operations.get_operator_implementation(
        _builder_context(runtime).require_request_id(), operator_name
    )


@tool("get_operator_document")
def builder_get_operator_document(
    operator_name: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> str:
    """Read one operator document from the current workflow user's catalog."""
    return operations.get_operator_document(
        _builder_context(runtime).require_request_id(), operator_name
    )


@tool("get_operator_test_implementation")
def get_operator_test_implementation(
    operator_name: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> str:
    """Read one operator test implementation from the current user's catalog."""
    return operations.get_operator_test_implementation(
        _builder_context(runtime).require_request_id(), operator_name
    )


@tool("install_workspace_package")
def install_workspace_package(
    package_name: PackageName,
    runtime: ToolRuntime[OperatorBuilderContext],
    version: PackageVersion | None = None,
) -> dict[str, Any]:
    """Install one package, optionally pinned to an exact version."""
    try:
        result = operations.install_workspace_package(
            _builder_context(runtime).require_request_id(), package_name, version
        )
    except httpx.HTTPError as exc:
        raise ToolException(str(exc)) from exc
    if result.get("status") == "failed" or result.get("exit_code") not in {None, 0}:
        error = (
            result.get("error") or result.get("stderr") or "package installation failed"
        )
        raise ToolException(str(error))
    return {**result, "status": result.get("status", "succeeded")}


@tool("save_operator_implementation")
def save_operator_implementation(
    operator_type: operations.OperatorType,
    operator_name: str,
    code: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> dict[str, Any]:
    """Save generated operator source in the current user's workspace."""
    return operations.save_operator_implementation(
        _builder_context(runtime).require_request_id(),
        operator_type,
        operator_name,
        code,
    )


@tool("save_operator_test_implementation")
def save_operator_test_implementation(
    operator_type: operations.OperatorType,
    operator_name: str,
    test_code: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> dict[str, Any]:
    """Save generated operator tests in the current user's workspace."""
    return operations.save_operator_test_implementation(
        _builder_context(runtime).require_request_id(),
        operator_type,
        operator_name,
        test_code,
    )


@tool("test_operator_implementation")
def test_operator_implementation(
    test_path: str,
    runtime: ToolRuntime[OperatorBuilderContext],
) -> dict[str, Any]:
    """Run one generated operator test file in the current user's workspace."""
    return operations.test_operator_implementation(
        _builder_context(runtime).require_request_id(), test_path
    )


@tool("sync_operator_documentation")
def sync_operator_documentation(
    runtime: ToolRuntime[OperatorBuilderContext],
) -> dict[str, Any]:
    """Synchronize operator documentation in the current user's workspace."""
    return operations.sync_operator_documentation(
        _builder_context(runtime).require_request_id()
    )


@tool("get_input_metadata_for_request")
def pipeline_get_input_metadata(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read the persisted input data description for this workflow."""
    context = _pipeline_context(runtime)
    return _pipeline_call(
        lambda: operations.get_input_metadata_for_request(context.require_request_id())
    )


@tool("get_test_output_metadata_for_request")
def pipeline_get_test_output_metadata(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read test-output metadata while validating the completed test run."""
    context = _pipeline_context(runtime)
    _require_pipeline_task(context, PipelineTask.VERIFY_TEST_OUTPUT)
    return _pipeline_call(
        lambda: operations.get_test_output_metadata_for_request(
            context.require_request_id()
        )
    )


@tool("get_operator_market_for_request")
def pipeline_get_operator_market(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read this workflow user's built-in and workspace operator market."""
    context = _pipeline_context(runtime)
    return _pipeline_call(
        lambda: operations.get_operator_market_for_request(context.require_request_id())
    )


@tool("get_operator_document")
def pipeline_get_operator_document(
    operator_name: str,
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read one operator document from this workflow user's catalog."""
    context = _pipeline_context(runtime)
    return _pipeline_call(
        lambda: operations.get_operator_document(
            context.require_request_id(), operator_name
        )
    )


@tool("get_pipeline_example")
def pipeline_get_example(runtime: ToolRuntime[PipelineAgentContext]) -> str:
    """Read the canonical Data Refiner pipeline example."""
    return _pipeline_call(operations.get_pipeline_example_for_agent)


@tool("get_latest_operator_evaluation")
def pipeline_get_latest_evaluation(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read the latest operator evaluation while planning a pipeline."""
    context = _pipeline_context(runtime)
    _require_pipeline_task(
        context, PipelineTask.PLAN_TEST, PipelineTask.PLAN_PRODUCTION
    )
    return _pipeline_call(
        lambda: operations.get_latest_operator_evaluation_for_request(
            context.require_request_id()
        )
    )


@tool("get_test_pipeline")
def pipeline_get_test_pipeline(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read the persisted test pipeline after it has been planned."""
    context = _pipeline_context(runtime)
    _require_pipeline_task(
        context,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_PRODUCTION,
        PipelineTask.ANALYZE_TEST_FAILURE,
    )
    return _pipeline_call(
        lambda: operations.get_test_pipeline_for_request(context.require_request_id())
    )


@tool("get_production_pipeline")
def pipeline_get_production_pipeline(
    runtime: ToolRuntime[PipelineAgentContext],
) -> str:
    """Read the persisted production pipeline while analyzing its failure."""
    context = _pipeline_context(runtime)
    _require_pipeline_task(context, PipelineTask.ANALYZE_PRODUCTION_FAILURE)
    return _pipeline_call(
        lambda: operations.get_production_pipeline_for_request(
            context.require_request_id()
        )
    )


@tool("search_similar_pipeline_cases")
def pipeline_search_cases(
    runtime: ToolRuntime[PipelineAgentContext],
) -> dict[str, Any]:
    """Read similar pipeline cases while planning the test pipeline."""
    context = _pipeline_context(runtime)
    _require_pipeline_task(context, PipelineTask.PLAN_TEST)
    return _pipeline_call(
        lambda: operations.search_similar_pipeline_cases(context.require_request_id())
    )


OPERATOR_BUILDER_TOOLS: list[BaseTool] = [
    builder_get_operator_build_reference,
    builder_get_operator_market,
    get_installed_packages_for_request,
    get_operator_implementation,
    builder_get_operator_document,
    get_operator_test_implementation,
    install_workspace_package,
    save_operator_implementation,
    save_operator_test_implementation,
    test_operator_implementation,
    sync_operator_documentation,
]

PIPELINE_TOOLS_BY_TASK: dict[PipelineTask, list[BaseTool]] = {
    PipelineTask.EVALUATE_OPERATORS: [
        pipeline_get_input_metadata,
        pipeline_get_operator_market,
        pipeline_get_operator_document,
    ],
    PipelineTask.PLAN_TEST: [
        pipeline_get_input_metadata,
        pipeline_get_operator_market,
        pipeline_get_operator_document,
        pipeline_get_example,
        pipeline_get_latest_evaluation,
        pipeline_search_cases,
    ],
    PipelineTask.VERIFY_TEST_OUTPUT: [
        pipeline_get_input_metadata,
        pipeline_get_test_output_metadata,
        pipeline_get_operator_document,
        pipeline_get_test_pipeline,
    ],
    PipelineTask.PLAN_PRODUCTION: [
        pipeline_get_input_metadata,
        pipeline_get_operator_market,
        pipeline_get_operator_document,
        pipeline_get_example,
        pipeline_get_latest_evaluation,
        pipeline_get_test_pipeline,
    ],
    PipelineTask.ANALYZE_TEST_FAILURE: [
        pipeline_get_input_metadata,
        pipeline_get_operator_market,
        pipeline_get_operator_document,
        pipeline_get_test_pipeline,
    ],
    PipelineTask.ANALYZE_PRODUCTION_FAILURE: [
        pipeline_get_input_metadata,
        pipeline_get_operator_document,
        pipeline_get_production_pipeline,
    ],
}

PIPELINE_TOOLS: list[BaseTool] = list(
    {
        pipeline_tool.name: pipeline_tool
        for task_tools in PIPELINE_TOOLS_BY_TASK.values()
        for pipeline_tool in task_tools
    }.values()
)

for pipeline_tool in PIPELINE_TOOLS:
    pipeline_tool.handle_tool_error = True

__all__ = ["OPERATOR_BUILDER_TOOLS", "PIPELINE_TOOLS", "PIPELINE_TOOLS_BY_TASK"]
