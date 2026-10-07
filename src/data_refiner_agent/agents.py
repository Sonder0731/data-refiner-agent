import os
from pathlib import Path
from typing import Any

from deepagents import (
    FilesystemPermission,
    GeneralPurposeSubagentProfile,
    HarnessProfile,
    create_deep_agent,
    register_harness_profile,
)
from deepagents.backends import FilesystemBackend
from langchain.agents.middleware import (
    ModelRequest,
    ModelResponse,
    ToolCallRequest,
    ToolErrorMiddleware,
    before_model,
    wrap_model_call,
)
from langchain.agents.structured_output import ToolStrategy
from langchain_core.messages import ToolMessage

from data_refiner_agent.agent_tools import (
    OPERATOR_BUILDER_TOOLS,
    PIPELINE_TOOLS,
    PIPELINE_TOOLS_BY_TASK,
)
from data_refiner_agent.prompts import load_prompt
from data_refiner_agent.workflow_types import (
    OperatorBuilderContext,
    OperatorBuildResult,
    PipelineAgentContext,
    PipelineAgentResult,
)

OPERATOR_BUILDER_AGENT = "operator-builder-agent"
PIPELINE_AGENT = "pipeline-agent"
MAX_CONSECUTIVE_TOOL_FAILURES = 3

_PIPELINE_READ_ONLY_TOOLS = {"read_file"}
_PIPELINE_RESULT_TOOL = PipelineAgentResult.__name__


class ToolRetryExhaustedError(RuntimeError):
    def __init__(self, tool_name: str, failures: int) -> None:
        self.tool_name = tool_name
        self.failures = failures
        super().__init__(
            f"Tool '{tool_name}' failed {failures} consecutive times; "
            "retry limit exhausted"
        )


def _consecutive_tool_failures(messages: list[Any]) -> dict[str, int]:
    failures: dict[str, int] = {}
    completed: set[str] = set()
    for message in reversed(messages):
        if not isinstance(message, ToolMessage) or not message.name:
            continue
        if message.name in completed:
            continue
        if message.status == "error":
            failures[message.name] = failures.get(message.name, 0) + 1
        else:
            completed.add(message.name)
    return failures


def _format_tool_error(exc: Exception, request: ToolCallRequest) -> str:
    tool_name = request.tool_call["name"]
    previous_failures = _consecutive_tool_failures(
        request.state.get("messages", [])
    ).get(tool_name, 0)
    attempt = previous_failures + 1
    detail = str(exc).strip() or repr(exc)
    return (
        f"Tool `{tool_name}` failed "
        f"({attempt}/{MAX_CONSECUTIVE_TOOL_FAILURES} consecutive failures): "
        f"{type(exc).__name__}: {detail[:2000]}. Correct the error and retry."
    )


_tool_error_middleware = ToolErrorMiddleware(on_error=_format_tool_error)


@before_model
def _enforce_tool_failure_limit(state: dict[str, Any], _runtime: Any) -> None:
    failures = _consecutive_tool_failures(state.get("messages", []))
    for tool_name, count in sorted(failures.items()):
        if count >= MAX_CONSECUTIVE_TOOL_FAILURES:
            raise ToolRetryExhaustedError(tool_name, count)


def _tool_name(tool: Any) -> str | None:
    if isinstance(tool, dict):
        name = tool.get("name")
        return name if isinstance(name, str) else None
    return getattr(tool, "name", None)


@wrap_model_call
def _scope_pipeline_tools(request: ModelRequest, handler) -> ModelResponse:
    context = request.runtime.context
    if isinstance(context, PipelineAgentContext):
        task = context.task
        finalize_only = context.finalize_only
    else:
        task = context["task"]
        finalize_only = context.get("finalize_only", False)
    allowed = {_PIPELINE_RESULT_TOOL}
    if not finalize_only:
        allowed.update(_PIPELINE_READ_ONLY_TOOLS)
        allowed.update(tool.name for tool in PIPELINE_TOOLS_BY_TASK[task])
    tools = [tool for tool in request.tools if _tool_name(tool) in allowed]
    return handler(request.override(tools=tools))


def _disable_default_subagent(model: str) -> None:
    provider, separator, _ = model.partition(":")
    if not separator:
        raise ValueError("DATA_REFINER_AGENT_MODEL must use provider:model format")
    register_harness_profile(
        provider,
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False)
        ),
    )


def create_operator_builder_agent(
    model: str | Any | None = None,
    *,
    checkpointer: Any = None,
):
    model = model or os.getenv("DATA_REFINER_AGENT_MODEL", "openai:gpt-5.6-luna")
    if isinstance(model, str):
        _disable_default_subagent(model)
    skill_root = Path(__file__).with_name("skills") / OPERATOR_BUILDER_AGENT
    return create_deep_agent(
        name=OPERATOR_BUILDER_AGENT,
        model=model,
        tools=OPERATOR_BUILDER_TOOLS,
        system_prompt=load_prompt(OPERATOR_BUILDER_AGENT),
        backend=FilesystemBackend(root_dir=skill_root),
        skills=["/"],
        permissions=[
            FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")
        ],
        middleware=[_tool_error_middleware, _enforce_tool_failure_limit],
        subagents=[],
        context_schema=OperatorBuilderContext,
        response_format=OperatorBuildResult,
        checkpointer=checkpointer,
    )


def create_pipeline_agent(
    model: str | Any | None = None,
    *,
    checkpointer: Any = None,
):
    model = model or os.getenv("DATA_REFINER_AGENT_MODEL", "openai:gpt-5.6-luna")
    if isinstance(model, str):
        _disable_default_subagent(model)
    skill_root = Path(__file__).with_name("skills") / PIPELINE_AGENT
    return create_deep_agent(
        name=PIPELINE_AGENT,
        model=model,
        tools=PIPELINE_TOOLS,
        system_prompt=load_prompt(PIPELINE_AGENT),
        backend=FilesystemBackend(root_dir=skill_root),
        skills=["/"],
        permissions=[
            FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")
        ],
        middleware=[
            _tool_error_middleware,
            _enforce_tool_failure_limit,
            _scope_pipeline_tools,
        ],
        subagents=[],
        context_schema=PipelineAgentContext,
        response_format=ToolStrategy(PipelineAgentResult, handle_errors=True),
        checkpointer=checkpointer,
    )


__all__ = [
    "OPERATOR_BUILDER_AGENT",
    "PIPELINE_AGENT",
    "create_operator_builder_agent",
    "create_pipeline_agent",
]
