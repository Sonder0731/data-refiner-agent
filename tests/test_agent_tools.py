from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from langchain.agents.middleware import ToolCallRequest
from langchain_core.messages import ToolMessage
from langchain_core.tools import ToolException

from data_refiner_agent.agent_tools import (
    install_workspace_package as install_workspace_package_tool,
)
from data_refiner_agent.agent_tools import (
    pipeline_get_test_output_metadata,
)
from data_refiner_agent.agents import _tool_error_middleware
from data_refiner_agent.tools import (
    get_operator_build_reference_for_round,
    install_workspace_package,
    prepare_user_workspace,
    save_data_metadata,
    save_operator_test_implementation,
    submit_pipeline,
    wait_for_application,
)
from data_refiner_agent.workflow_types import (
    OperatorBuilderContext,
    PipelineAgentContext,
    PipelineTask,
)


@patch("data_refiner_agent.tools.add_package")
@patch("data_refiner_agent.tools.RequestState.get_user_id")
def test_install_workspace_package_forwards_exact_version(
    get_user_id,
    add_package,
) -> None:
    get_user_id.return_value = "@admin:example.test"
    add_package.return_value = {"status": "succeeded", "exit_code": 0}

    result = install_workspace_package("request", "pypinyin", "0.55.0")

    assert result["status"] == "succeeded"
    add_package.assert_called_once_with("@admin:example.test", "pypinyin", "0.55.0")


@patch("data_refiner_agent.agent_tools.operations.install_workspace_package")
def test_install_package_http_error_is_returned_to_agent(install_package) -> None:
    request = httpx.Request("POST", "http://api/workspace/add-package")
    response = httpx.Response(422, request=request)
    install_package.side_effect = httpx.HTTPStatusError(
        "invalid package version",
        request=request,
        response=response,
    )
    runtime = SimpleNamespace(
        context=OperatorBuilderContext(
            user_id="@admin:example.test",
            request_id="request",
        )
    )

    request = ToolCallRequest(
        tool_call={
            "name": "install_workspace_package",
            "args": {"package_name": "pypinyin", "version": "0.55.0"},
            "id": "call-1",
            "type": "tool_call",
        },
        tool=install_workspace_package_tool,
        state={"messages": []},
        runtime=runtime,
    )
    result = _tool_error_middleware.wrap_tool_call(
        request,
        lambda _request: install_workspace_package_tool.func(
            package_name="pypinyin",
            version="0.55.0",
            runtime=runtime,
        ),
    )

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "invalid package version" in str(result.content)


@patch("data_refiner_agent.agent_tools.operations.install_workspace_package")
def test_install_package_failed_result_becomes_tool_error(install_package) -> None:
    install_package.return_value = {"status": "failed", "error": "timed out"}
    runtime = SimpleNamespace(
        context=OperatorBuilderContext(
            user_id="@admin:example.test",
            request_id="request",
        )
    )

    with pytest.raises(ToolException, match="timed out"):
        install_workspace_package_tool.func(
            package_name="pypinyin",
            version="0.55.0",
            runtime=runtime,
        )


def test_install_package_tool_rejects_invalid_version() -> None:
    with pytest.raises(ValueError, match="valid exact Python package version"):
        install_workspace_package_tool.args_schema.model_validate(
            {"package_name": "pypinyin", "version": "latest stable"}
        )


@patch("data_refiner_agent.agent_tools.operations.get_test_output_metadata_for_request")
def test_plan_test_cannot_read_test_output_metadata(get_test_output) -> None:
    runtime = SimpleNamespace(
        context=PipelineAgentContext(
            user_id="@admin:example.test",
            request_id="request",
            task=PipelineTask.PLAN_TEST,
            pipeline_category="test",
        )
    )

    with pytest.raises(ToolException, match="only available for verify-test-output"):
        pipeline_get_test_output_metadata.func(runtime=runtime)

    get_test_output.assert_not_called()
    assert pipeline_get_test_output_metadata.handle_tool_error is True


@patch(
    "data_refiner_agent.agent_tools.operations.get_test_output_metadata_for_request",
    side_effect=ValueError("test output metadata was not found or is empty"),
)
def test_missing_test_output_metadata_is_recoverable(get_test_output) -> None:
    runtime = SimpleNamespace(
        context=PipelineAgentContext(
            user_id="@admin:example.test",
            request_id="request",
            task=PipelineTask.VERIFY_TEST_OUTPUT,
            pipeline_category="test",
        )
    )

    with pytest.raises(ToolException, match="was not found or is empty"):
        pipeline_get_test_output_metadata.func(runtime=runtime)

    get_test_output.assert_called_once_with("request")


@patch("data_refiner_agent.tools.NewOperatorBuildReference.get")
def test_build_reference_is_read_by_exact_round(get_reference) -> None:
    get_reference.return_value = {
        "id": 7,
        "rounds": 2,
        "content": "build guide",
    }

    result = get_operator_build_reference_for_round("request", 2)

    assert result["id"] == 7
    get_reference.assert_called_once_with("request", 2)


@patch("data_refiner_agent.tools.save_user_container")
@patch("data_refiner_agent.tools.create_container")
def test_prepare_workspace_always_reconciles_container_and_mapping(
    create_container,
    save_user_container,
) -> None:
    create_container.return_value = {
        "name": "data-refiner-user-workspace-admin",
        "status": "running",
    }
    save_user_container.return_value = {
        "container_name": "data-refiner-user-workspace-admin",
        "api_url": "http://data-refiner-user-workspace-admin:8000",
    }

    result = prepare_user_workspace("@admin:example.test")

    create_container.assert_called_once()
    container_options = create_container.call_args.args[0]
    assert container_options["image"] == "data-refiner-user-workspace:0.0.1"
    assert container_options["network"] == "data-refiner-agent-sparknet"
    save_user_container.assert_called_once_with(
        "@admin:example.test", "data-refiner-user-workspace-admin"
    )
    assert result["workspace"] == save_user_container.return_value


@patch("data_refiner_agent.tools.RequestState.update_input_data_metadata_id")
@patch("data_refiner_agent.tools.DataMetadata.upsert")
@patch("data_refiner_agent.tools.connect_database")
def test_save_input_metadata_links_request_state(
    connect_database,
    upsert,
    update_request_state,
) -> None:
    connection = connect_database.return_value.__enter__.return_value
    upsert.return_value = {"id": 42, "category": "input", "content": "schema"}

    result = save_data_metadata(
        "a3988658-859c-4545-8467-26e12d6547d0",
        "input",
        "schema",
    )

    assert result["id"] == 42
    update_request_state.assert_called_once_with(
        "a3988658-859c-4545-8467-26e12d6547d0",
        42,
        connection=connection,
    )


@patch("data_refiner_agent.tools.RequestState.update_test_output_data_metadata_id")
@patch("data_refiner_agent.tools.DataMetadata.upsert")
@patch("data_refiner_agent.tools.connect_database")
def test_save_test_output_metadata_links_request_state(
    connect_database,
    upsert,
    update_request_state,
) -> None:
    connection = connect_database.return_value.__enter__.return_value
    upsert.return_value = {
        "id": 43,
        "category": "test_output",
        "content": "schema",
    }

    result = save_data_metadata(
        "a3988658-859c-4545-8467-26e12d6547d0",
        "test_output",
        "schema",
    )

    assert result["id"] == 43
    update_request_state.assert_called_once_with(
        "a3988658-859c-4545-8467-26e12d6547d0",
        43,
        connection=connection,
    )


@patch("data_refiner_agent.tools.run_pipeline")
@patch("data_refiner_agent.tools.RequestState.get_user_id")
def test_submit_pipeline_uses_hdfs_basename(get_user_id, run_pipeline) -> None:
    get_user_id.return_value = "@admin:example.test"
    run_pipeline.return_value = {"application_id": "application_1_1"}

    result = submit_pipeline(
        "a3988658-859c-4545-8467-26e12d6547d0",
        "hdfs://namenode/pipelines/full.yaml",
        ["--num-executors 4"],
    )

    assert result == {"application_id": "application_1_1"}
    run_pipeline.assert_called_once_with(
        user_id="@admin:example.test",
        pipeline_name="full.yaml",
        spark_runtime_config=["--num-executors 4"],
    )


@patch("data_refiner_agent.tools.write_operator_test_code")
@patch("data_refiner_agent.tools.RequestState.get_user_id")
def test_operator_test_imports_are_rewritten(get_user_id, write_test) -> None:
    get_user_id.return_value = "@admin:example.test"
    write_test.return_value = {"path": "workspace/tests/test_new.py"}

    save_operator_test_implementation(
        "a3988658-859c-4545-8467-26e12d6547d0",
        "filter",
        "new_filter",
        "from data_refiner.ops.filter import Filter\nfrom tests.tools import TEST_DATA",
    )

    saved = write_test.call_args.args[3]
    assert "from workspace.ops.filter import Filter" in saved
    assert "from workspace.tests.tools import TEST_DATA" in saved


@patch("data_refiner_agent.tools.time.sleep")
@patch("data_refiner_agent.tools.get_monitor_task")
def test_wait_for_application_stops_at_terminal_status(get_status, sleep) -> None:
    get_status.side_effect = [
        {"status": "RUNNING"},
        {"result": {"final_status": "SUCCEEDED"}},
    ]

    result = wait_for_application(
        "application_1_1",
        poll_interval_seconds=1,
        timeout_seconds=30,
    )

    assert result == {"result": {"final_status": "SUCCEEDED"}}
    sleep.assert_called_once_with(1)


def test_submit_pipeline_rejects_non_string_config() -> None:
    with pytest.raises(ValueError, match="only strings"):
        submit_pipeline("request", "/pipeline.yaml", ["--conf ok", 1])
