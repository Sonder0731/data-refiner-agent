from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from deepagents import FilesystemPermission
from deepagents.backends import FilesystemBackend
from deepagents.middleware import SkillsMiddleware
from langchain.agents.middleware import ToolCallRequest, ToolErrorMiddleware
from langchain.agents.structured_output import ToolStrategy
from langchain_core.messages import ToolMessage

from data_refiner_agent.agent_tools import (
    OPERATOR_BUILDER_TOOLS,
    PIPELINE_TOOLS,
    PIPELINE_TOOLS_BY_TASK,
)
from data_refiner_agent.agents import (
    MAX_CONSECUTIVE_TOOL_FAILURES,
    OPERATOR_BUILDER_AGENT,
    PIPELINE_AGENT,
    ToolRetryExhaustedError,
    _enforce_tool_failure_limit,
    _scope_pipeline_tools,
    _tool_error_middleware,
    create_operator_builder_agent,
    create_pipeline_agent,
)
from data_refiner_agent.prompts import load_prompt
from data_refiner_agent.workflow_types import (
    OperatorBuilderContext,
    OperatorBuildResult,
    PipelineAgentContext,
    PipelineAgentResult,
    PipelineTask,
)


@pytest.mark.parametrize(
    ("factory", "name", "tools", "context_schema", "response_schema"),
    [
        (
            create_pipeline_agent,
            PIPELINE_AGENT,
            PIPELINE_TOOLS,
            PipelineAgentContext,
            PipelineAgentResult,
        ),
        (
            create_operator_builder_agent,
            OPERATOR_BUILDER_AGENT,
            OPERATOR_BUILDER_TOOLS,
            OperatorBuilderContext,
            OperatorBuildResult,
        ),
    ],
)
def test_two_deep_agents_have_isolated_tools_and_no_subagents(
    factory, name, tools, context_schema, response_schema
) -> None:
    with patch("data_refiner_agent.agents.create_deep_agent") as create:
        factory("openai:test-model")

    kwargs = create.call_args.kwargs
    assert kwargs["name"] == name
    assert kwargs["tools"] == tools
    assert kwargs["subagents"] == []
    assert kwargs["context_schema"] is context_schema
    if name == PIPELINE_AGENT:
        assert isinstance(kwargs["response_format"], ToolStrategy)
        assert kwargs["response_format"].schema is response_schema
        assert kwargs["response_format"].handle_errors is True
        assert kwargs["middleware"] == [
            _tool_error_middleware,
            _enforce_tool_failure_limit,
            _scope_pipeline_tools,
        ]
    else:
        assert kwargs["response_format"] is response_schema
        assert kwargs["middleware"] == [
            _tool_error_middleware,
            _enforce_tool_failure_limit,
        ]
    assert isinstance(kwargs["backend"], FilesystemBackend)
    assert kwargs["skills"] == ["/"]
    assert kwargs["permissions"] == [
        FilesystemPermission(operations=["write"], paths=["/**"], mode="deny")
    ]


@pytest.mark.parametrize(
    ("factory", "expected_skills"),
    [
        (
            create_pipeline_agent,
            {
                "failure-analysis",
                "inspect-task-context",
                "new-operator-reference",
                "operators-evaluation",
                "pipeline-planning",
                "test-output-validation",
            },
        ),
        (
            create_operator_builder_agent,
            {"build-operator", "inspect-task-context"},
        ),
    ],
)
def test_agents_load_only_their_native_skills(factory, expected_skills) -> None:
    with patch("data_refiner_agent.agents.create_deep_agent") as create:
        factory("openai:test-model")

    kwargs = create.call_args.kwargs
    loaded = SkillsMiddleware(
        backend=kwargs["backend"], sources=kwargs["skills"]
    ).before_agent({}, None, {})
    assert loaded is not None
    assert "skills_load_errors" not in loaded
    assert {skill["name"] for skill in loaded["skills_metadata"]} == expected_skills


@pytest.mark.parametrize(
    "factory", [create_pipeline_agent, create_operator_builder_agent]
)
def test_model_requires_provider_prefix(factory) -> None:
    with pytest.raises(ValueError, match="provider:model"):
        factory("model-without-provider")


def test_agent_tool_schemas_hide_workflow_identity() -> None:
    hidden_fields = {
        "request_id",
        "user_id",
        "operator_round",
        "application_id",
        "pipeline_category",
    }
    for agent_tool in [*PIPELINE_TOOLS, *OPERATOR_BUILDER_TOOLS]:
        assert hidden_fields.isdisjoint(agent_tool.args)


def test_operator_reads_use_name_but_writes_keep_type() -> None:
    lookup_names = {
        "get_operator_implementation",
        "get_operator_document",
        "get_operator_test_implementation",
    }
    for agent_tool in [*PIPELINE_TOOLS, *OPERATOR_BUILDER_TOOLS]:
        if agent_tool.name in lookup_names:
            assert "operator_type" not in agent_tool.args


@pytest.mark.parametrize(
    ("task", "expected"),
    [
        (
            PipelineTask.EVALUATE_OPERATORS,
            {
                "get_input_metadata_for_request",
                "get_operator_market_for_request",
                "get_operator_document",
            },
        ),
        (
            PipelineTask.PLAN_TEST,
            {
                "get_input_metadata_for_request",
                "get_operator_market_for_request",
                "get_operator_document",
                "get_pipeline_example",
                "get_latest_operator_evaluation",
                "search_similar_pipeline_cases",
            },
        ),
        (
            PipelineTask.VERIFY_TEST_OUTPUT,
            {
                "get_input_metadata_for_request",
                "get_test_output_metadata_for_request",
                "get_operator_document",
                "get_test_pipeline",
            },
        ),
        (
            PipelineTask.PLAN_PRODUCTION,
            {
                "get_input_metadata_for_request",
                "get_operator_market_for_request",
                "get_operator_document",
                "get_pipeline_example",
                "get_latest_operator_evaluation",
                "get_test_pipeline",
            },
        ),
        (
            PipelineTask.ANALYZE_TEST_FAILURE,
            {
                "get_input_metadata_for_request",
                "get_operator_market_for_request",
                "get_operator_document",
                "get_test_pipeline",
            },
        ),
        (
            PipelineTask.ANALYZE_PRODUCTION_FAILURE,
            {
                "get_input_metadata_for_request",
                "get_operator_document",
                "get_production_pipeline",
            },
        ),
    ],
)
def test_pipeline_agent_only_sees_tools_for_current_task(task, expected) -> None:
    request = SimpleNamespace(
        runtime=SimpleNamespace(context={"task": task}),
        tools=[
            *PIPELINE_TOOLS,
            SimpleNamespace(name="read_file"),
            SimpleNamespace(name="task"),
            SimpleNamespace(name="ls"),
            SimpleNamespace(name="glob"),
            SimpleNamespace(name="write_file"),
            SimpleNamespace(name="PipelineAgentResult"),
        ],
    )
    request.override = lambda **changes: SimpleNamespace(**{**vars(request), **changes})

    visible = _scope_pipeline_tools.wrap_model_call(
        request, lambda scoped_request: scoped_request.tools
    )
    visible_names = {tool.name for tool in visible}

    assert visible_names == expected | {"read_file", "PipelineAgentResult"}
    assert {tool.name for tool in PIPELINE_TOOLS_BY_TASK[task]} == expected


def test_pipeline_agent_finalize_retry_only_sees_result_tool() -> None:
    request = SimpleNamespace(
        runtime=SimpleNamespace(
            context={"task": PipelineTask.EVALUATE_OPERATORS, "finalize_only": True}
        ),
        tools=[
            *PIPELINE_TOOLS,
            SimpleNamespace(name="read_file"),
            SimpleNamespace(name="task"),
            SimpleNamespace(name="PipelineAgentResult"),
        ],
    )
    request.override = lambda **changes: SimpleNamespace(**{**vars(request), **changes})

    visible = _scope_pipeline_tools.wrap_model_call(
        request, lambda scoped_request: scoped_request.tools
    )

    assert [tool.name for tool in visible] == ["PipelineAgentResult"]


def test_tool_exception_is_returned_to_agent_for_correction() -> None:
    request = ToolCallRequest(
        tool_call={
            "name": "get_operator_implementation",
            "args": {"operator_name": "SingleInMultiOutMapper"},
            "id": "call-1",
            "type": "tool_call",
        },
        tool=None,
        state={"messages": []},
        runtime=SimpleNamespace(),
    )

    def fail(_request):
        raise ValueError("invalid operator_name: SingleInMultiOutMapper")

    result = _tool_error_middleware.wrap_tool_call(request, fail)

    assert isinstance(_tool_error_middleware, ToolErrorMiddleware)
    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "invalid operator_name: SingleInMultiOutMapper" in str(result.content)
    assert "Correct the error and retry" in str(result.content)
    assert f"1/{MAX_CONSECUTIVE_TOOL_FAILURES}" in str(result.content)


def test_tool_failure_limit_stops_after_three_consecutive_failures() -> None:
    messages = [
        ToolMessage(
            "failed",
            tool_call_id=f"call-{attempt}",
            name="get_operator_implementation",
            status="error",
        )
        for attempt in range(MAX_CONSECUTIVE_TOOL_FAILURES)
    ]

    with pytest.raises(
        ToolRetryExhaustedError,
        match="get_operator_implementation.*3 consecutive times",
    ):
        _enforce_tool_failure_limit.before_model(
            {"messages": messages}, SimpleNamespace()
        )


def test_successful_tool_call_resets_failure_limit() -> None:
    messages = [
        ToolMessage(
            "old failure",
            tool_call_id=f"old-{attempt}",
            name="get_operator_implementation",
            status="error",
        )
        for attempt in range(MAX_CONSECUTIVE_TOOL_FAILURES)
    ]
    messages.append(
        ToolMessage(
            "success",
            tool_call_id="success",
            name="get_operator_implementation",
            status="success",
        )
    )
    messages.extend(
        ToolMessage(
            "new failure",
            tool_call_id=f"new-{attempt}",
            name="get_operator_implementation",
            status="error",
        )
        for attempt in range(MAX_CONSECUTIVE_TOOL_FAILURES - 1)
    )

    assert (
        _enforce_tool_failure_limit.before_model(
            {"messages": messages}, SimpleNamespace()
        )
        is None
    )


def test_pipeline_agent_does_not_register_node_owned_tools() -> None:
    registered = {tool.name for tool in PIPELINE_TOOLS}

    assert {
        "validate_pipeline_config",
        "get_application_status",
        "get_spark_runtime_config",
    }.isdisjoint(registered)

    write_tools = {
        agent_tool.name: agent_tool
        for agent_tool in OPERATOR_BUILDER_TOOLS
        if agent_tool.name.startswith("save_operator_")
    }
    assert "operator_type" in write_tools["save_operator_implementation"].args
    assert "operator_type" in write_tools["save_operator_test_implementation"].args


def test_pipeline_agent_validation_does_not_require_full_data_stats() -> None:
    prompt = load_prompt(PIPELINE_AGENT)
    skill = (
        Path(__file__).parents[1]
        / "src/data_refiner_agent/skills/pipeline-agent/test-output-validation/SKILL.md"
    ).read_text(encoding="utf-8")

    assert "must not fail merely because the test profile lacks full-data" in prompt
    assert "subset of samples does not mean" in skill
    assert "do not stop because those statistics are missing" in skill


def test_pipeline_planning_shows_user_requested_scalar_results() -> None:
    skill = (
        Path(__file__).parents[1]
        / "src/data_refiner_agent/skills/pipeline-agent/pipeline-planning/SKILL.md"
    ).read_text(encoding="utf-8")

    assert "scalar or small aggregate result" in skill
    assert "`show: true`" in skill
    assert "unbounded detail data" in skill


@pytest.mark.parametrize(
    "factory", [create_pipeline_agent, create_operator_builder_agent]
)
def test_runtime_agents_have_no_subagent_dispatch_tool(monkeypatch, factory) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "testing-key")
    agent = factory("openai:gpt-5.5")

    tool_names = set(agent.nodes["tools"].bound.tools_by_name)
    assert "task" not in tool_names
    assert "dispatch_to_agent" not in tool_names
