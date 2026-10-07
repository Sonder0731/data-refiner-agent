import json
from typing import Any
from unittest.mock import patch

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from data_refiner_agent import tools as operations
from data_refiner_agent import workflow as workflow_module
from data_refiner_agent.subgraphs import (
    _validate_runtime_args,
    create_application_monitor_subgraph,
)
from data_refiner_agent.workflow import create_workflow
from data_refiner_agent.workflow_types import (
    DataDescription,
    FinalReport,
    IntakeResult,
    OperatorBuildResult,
    OutputSpec,
    PipelineAgentContext,
    PipelineAgentResult,
    PipelineTask,
    SparkSizingResult,
    WorkflowStage,
)
from data_refiner_agent_tools import RequestWorkflow


class FakeStructuredRunnable:
    def __init__(self, model: "FakeModel", schema: type) -> None:
        self.model = model
        self.schema = schema

    def invoke(self, prompt: str, config: dict[str, Any]) -> Any:
        self.model.invocations.append((self.schema, prompt, config))
        return self.model.responses[self.schema].pop(0)


class FakeModel:
    def __init__(self, **responses: list[dict[str, Any]]) -> None:
        schemas = {
            schema.__name__: schema
            for schema in (
                DataDescription,
                FinalReport,
                IntakeResult,
                SparkSizingResult,
            )
        }
        self.responses = {
            schemas[name]: list(values) for name, values in responses.items()
        }
        self.invocations: list[tuple[type, str, dict[str, Any]]] = []

    def with_structured_output(self, schema: type) -> FakeStructuredRunnable:
        return FakeStructuredRunnable(self, schema)


class FakeAgent:
    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.invocations: list[tuple[Any, Any, Any]] = []

    def invoke(self, state, config, *, context):
        self.invocations.append((state, config, context))
        response = self.responses.pop(0)
        if isinstance(response, tuple):
            structured_response, messages = response
            return {
                "structured_response": structured_response,
                "messages": messages,
            }
        return {"structured_response": response}


def _result(action: str, **values: Any) -> dict[str, Any]:
    return PipelineAgentResult(
        action=action, summary=values.pop("summary", action), **values
    ).model_dump(mode="json")


def _stub_business(monkeypatch) -> dict[str, Any]:
    store: dict[str, Any] = {
        "request": None,
        "metadata": {},
        "evaluations": {},
        "references": {},
        "pipelines": {},
        "runtime_config": None,
        "submissions": 0,
        "workspace_syncs": [],
        "profile_reads": [],
    }

    def request_get(request_id):
        row = store["request"]
        return dict(row) if row else None

    def request_create(**kwargs):
        store["request"] = {
            "request_id": str(kwargs["request_id"]),
            "user_id": kwargs["user_id"],
            "user_request": kwargs["user_request"],
            "current_stage": kwargs["current_stage"],
        }
        return request_get(kwargs["request_id"])

    def transition(request_id, next_stage, assignee):
        store["request"]["current_stage"] = next_stage
        return request_get(request_id)

    monkeypatch.setattr(workflow_module.RequestState, "get", request_get)
    monkeypatch.setattr(workflow_module.RequestWorkflow, "create", request_create)
    monkeypatch.setattr(workflow_module.RequestWorkflow, "transition", transition)
    monkeypatch.setattr(
        workflow_module.operations, "prepare_user_workspace", lambda *a, **k: {}
    )

    def read_data(data_type, source):
        store["profile_reads"].append((data_type, source))
        return f"schema and examples for {source}"

    monkeypatch.setattr(workflow_module.operations, "read_data", read_data)

    def save_metadata(request_id, category, content):
        row = {"id": f"{category}-metadata", "content": content}
        store["metadata"][category] = row
        return row

    monkeypatch.setattr(workflow_module.operations, "save_data_metadata", save_metadata)

    def save_evaluation(request_id, round_number, content):
        row = {
            "id": f"evaluation-{round_number}",
            "rounds": round_number,
            "content": content,
        }
        store["evaluations"][round_number] = row
        return row

    def save_reference(request_id, round_number, content):
        row = {
            "id": f"reference-{round_number}",
            "rounds": round_number,
            "content": content,
        }
        store["references"][round_number] = row
        return row

    monkeypatch.setattr(
        workflow_module.operations, "save_operator_evaluation", save_evaluation
    )
    monkeypatch.setattr(
        workflow_module.operations, "save_operator_build_reference", save_reference
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_operator_evaluation_for_round",
        lambda request_id, round_number: store["evaluations"][round_number],
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_operator_build_reference_for_round",
        lambda request_id, round_number: store["references"][round_number],
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_operator_market_for_request",
        lambda request_id: "available operators",
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "validate_pipeline_config",
        lambda request_id, pipeline_yaml: {"success": True},
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "validate_pipeline_outputs",
        lambda pipeline_yaml, outputs, environment: {"success": True},
    )

    def save_pipeline(request_id, category, content):
        row = {"id": f"{category}-pipeline", "content": content}
        store["pipelines"][category] = row
        return {
            "workspace": {"hdfs_path": f"hdfs:///pipelines/{category}.yaml"},
            "database": row,
        }

    monkeypatch.setattr(
        workflow_module.operations, "save_pipeline_config", save_pipeline
    )

    def submit(request_id, path, runtime_args):
        store["submissions"] += 1
        return {"application_id": f"application_{store['submissions']}_1"}

    monkeypatch.setattr(workflow_module.operations, "submit_pipeline", submit)
    monkeypatch.setattr(
        workflow_module.operations,
        "register_application_monitor",
        lambda application_id: {},
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "wait_for_application",
        lambda application_id, **kwargs: {"status": "SUCCEEDED"},
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_input_metadata_for_request",
        lambda request_id: store["metadata"]["input"]["content"],
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_output_metadata_for_request",
        lambda request_id: store["metadata"]["output"]["content"],
        raising=False,
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_production_pipeline_for_request",
        lambda request_id: store["pipelines"]["production"]["content"],
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_cluster_resources_for_agent",
        lambda: {"available_memory_mb": 16384, "available_vcores": 8},
    )

    def save_runtime_config(request_id, config):
        store["runtime_config"] = config
        return {"id": "runtime-config"}

    monkeypatch.setattr(
        workflow_module.operations,
        "save_spark_runtime_config",
        save_runtime_config,
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_spark_runtime_config_for_request",
        lambda request_id: ["--num-executors 2"],
    )
    monkeypatch.setattr(
        workflow_module.operations, "save_pipeline_case", lambda *a, **k: {}
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "sync_workspace_for_request",
        lambda request_id: store["workspace_syncs"].append("workspace"),
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "sync_conda_env_for_request",
        lambda request_id: store["workspace_syncs"].append("conda"),
    )
    return store


def _processing_model() -> FakeModel:
    return FakeModel(
        IntakeResult=[
            {
                "route": "process",
                "data_type": "file",
                "data_source": "hdfs:///data/input",
                "outputs": [
                    {
                        "name": "result",
                        "data_type": "file",
                        "target": "hdfs:///data/output",
                    }
                ],
            }
        ],
        DataDescription=[
            {"description": "input profile"},
            {"description": "test output profile"},
            {"description": "output profile"},
        ],
        SparkSizingResult=[
            {
                "runtime_args": [
                    "--num-executors 2",
                    "--executor-cores 2",
                    "--executor-memory 2g",
                    "--driver-memory 1g",
                ],
                "rationale": "fits available resources",
            }
        ],
        FinalReport=[{"answer": "workflow completed"}],
    )


def _happy_pipeline_agent() -> FakeAgent:
    return FakeAgent(
        _result(
            "plan-test-pipeline",
            evaluation_report="all operators are available",
        ),
        _result("submit-test-pipeline", pipeline_yaml="test: pipeline"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )


def test_workflow_transition_map_separates_test_and_production() -> None:
    RequestWorkflow.validate_transition(
        WorkflowStage.SUBMITTING_TEST_PIPELINE,
        WorkflowStage.MONITORING_TEST_PIPELINE,
    )
    RequestWorkflow.validate_transition(
        WorkflowStage.MONITORING_TEST_PIPELINE,
        WorkflowStage.PROFILING_TEST_OUTPUT,
    )
    RequestWorkflow.validate_transition(
        WorkflowStage.PROFILING_TEST_OUTPUT,
        WorkflowStage.PLANNING_TEST_PIPELINE,
    )
    RequestWorkflow.validate_transition(
        WorkflowStage.PROFILING_OUTPUT,
        WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
    )
    with pytest.raises(ValueError, match="Illegal workflow transition"):
        RequestWorkflow.validate_transition(
            WorkflowStage.MONITORING_TEST_PIPELINE,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
        )


def test_output_spec_rejects_concatenated_locations() -> None:
    with pytest.raises(ValueError, match="exactly one location"):
        OutputSpec(
            name="results",
            data_type="file",
            target="hdfs:///data/docs；hdfs:///data/terms",
        )


def test_pipeline_output_validation_accepts_each_declared_test_writer() -> None:
    outputs = [
        {
            "name": "documents",
            "data_type": "file",
            "target": "hdfs:///data/documents",
            "test_target": "hdfs:///data/documents.__test_abc",
        },
        {
            "name": "terms",
            "data_type": "file",
            "target": "hdfs:///data/terms",
            "test_target": "hdfs:///data/terms.__test_abc",
        },
    ]
    pipeline_yaml = """
write_documents:
  op_name: path_writer
  path: hdfs:///data/documents.__test_abc
write_terms:
  op_name: path_writer
  path: hdfs:///data/terms.__test_abc
"""

    assert operations.validate_pipeline_outputs(pipeline_yaml, outputs, "test") == {
        "success": True
    }


def test_pipeline_output_validation_rejects_production_target_in_test() -> None:
    outputs = [
        {
            "name": "documents",
            "data_type": "file",
            "target": "hdfs:///data/documents",
            "test_target": "hdfs:///data/documents.__test_abc",
        }
    ]
    pipeline_yaml = """
write_documents:
  op_name: path_writer
  path: hdfs:///data/documents
"""

    result = operations.validate_pipeline_outputs(pipeline_yaml, outputs, "test")

    assert result["success"] is False
    assert "production targets" in result["error"]


def test_pipeline_output_validation_rejects_unavailable_dataframe() -> None:
    pipeline_yaml = """
write_missing:
  op_name: path_writer
  input_df: missing_df
  path: hdfs:///data/result
"""

    result = operations.validate_pipeline_outputs(
        pipeline_yaml,
        [{"name": "result", "data_type": "file", "target": "hdfs:///data/result"}],
        "production",
    )

    assert result["success"] is False
    assert "missing_df" in result["error"]


def test_pipeline_output_validation_requires_sql_view_from_earlier_node() -> None:
    outputs = [
        {
            "name": "result",
            "data_type": "file",
            "target": "hdfs:///data/result",
        }
    ]
    invalid_yaml = """
read:
  op_name: regular_path_reader
  output_df: source_df
sql:
  op_name: spark_sql_executor
  output_df: result_df
  temp_view_name: source_view
  sql_query: SELECT * FROM source_view
write:
  op_name: path_writer
  input_df: result_df
  path: hdfs:///data/result
"""
    valid_yaml = invalid_yaml.replace(
        "  output_df: source_df\nsql:",
        "  output_df: source_df\n  temp_view_name: source_view\nsql:",
    ).replace("  temp_view_name: source_view\n  sql_query", "  sql_query")

    invalid = operations.validate_pipeline_outputs(invalid_yaml, outputs, "production")
    valid = operations.validate_pipeline_outputs(valid_yaml, outputs, "production")

    assert invalid["success"] is False
    assert "source_view" in invalid["error"]
    assert valid == {"success": True}


def test_spark_runtime_validation_enforces_available_resources() -> None:
    error = _validate_runtime_args(
        [
            "--num-executors 4",
            "--executor-cores 4",
            "--executor-memory 4g",
            "--driver-memory 1g",
        ],
        {"available_memory_mb": 8192, "available_vcores": 8},
    )

    assert "vCores" in error


@pytest.mark.parametrize(
    "key",
    [
        "spark.driver.cores",
        "spark.driver.memoryOverhead",
        "spark.sql.adaptive.coalescePartitions.enabled",
    ],
)
def test_spark_runtime_validation_rejects_unsupported_conf(key: str) -> None:
    error = _validate_runtime_args(
        [
            "--num-executors 2",
            "--executor-cores 2",
            "--executor-memory 2g",
            "--driver-memory 1g",
            f"--conf {key}=1",
        ],
        {"available_memory_mb": 16384, "available_vcores": 8},
    )

    assert error == f"Spark configuration cannot be overridden: {key}"


def test_spark_runtime_validation_accepts_supported_conf() -> None:
    error = _validate_runtime_args(
        [
            "--num-executors 2",
            "--executor-cores 2",
            "--executor-memory 2g",
            "--driver-memory 1g",
            "--conf spark.executor.memoryOverhead=512m",
            "--conf spark.sql.shuffle.partitions=48",
            "--conf spark.default.parallelism=48",
            "--conf spark.sql.adaptive.enabled=true",
        ],
        {"available_memory_mb": 16384, "available_vcores": 8},
    )

    assert error == ""


def test_spark_sizing_repairs_unsupported_conf_before_persisting(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    model = _processing_model()
    model.responses[SparkSizingResult] = [
        {
            "runtime_args": [
                "--num-executors 3",
                "--executor-cores 2",
                "--executor-memory 3g",
                "--driver-memory 2g",
                "--conf spark.driver.cores=1",
            ],
            "rationale": "contains an unsupported setting",
        },
        {
            "runtime_args": [
                "--num-executors 3",
                "--executor-cores 2",
                "--executor-memory 3g",
                "--driver-memory 2g",
                "--conf spark.sql.shuffle.partitions=48",
            ],
            "rationale": "uses only supported settings",
        },
    ]
    workflow = create_workflow(
        model,
        pipeline_agent=_happy_pipeline_agent(),
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "spark-config-repair-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert store["runtime_config"][-1] == "--conf spark.sql.shuffle.partitions=48"
    sizing_calls = [call for call in model.invocations if call[0] is SparkSizingResult]
    assert len(sizing_calls) == 2
    assert "spark.driver.cores" in sizing_calls[1][1]


def test_graph_contains_three_subgraphs_and_two_agents() -> None:
    workflow = create_workflow(
        FakeModel(), pipeline_agent=FakeAgent(), operator_builder=FakeAgent()
    )
    mermaid = workflow.get_graph(xray=True).draw_mermaid()

    assert "subgraph data-profile" in mermaid
    assert "subgraph spark-sizing" in mermaid
    assert "subgraph application-monitor" in mermaid
    assert "subgraph operator-evaluation" not in mermaid
    assert "subgraph pipeline-config" not in mermaid
    assert {name for name in workflow.nodes if name.endswith("-agent")} == {
        "pipeline-agent",
        "operator-builder-agent",
    }


def test_preview_and_clarification_use_checkpointed_graph(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_module.operations,
        "read_data",
        lambda data_type, source: "raw schema and examples",
    )
    model = FakeModel(
        IntakeResult=[
            {
                "route": "clarify",
                "clarification_question": "What is the data source?",
            },
            {
                "route": "preview",
                "data_type": "file",
                "data_source": "hdfs:///data/input",
            },
        ],
        DataDescription=[{"description": "schema and examples"}],
        FinalReport=[{"answer": "preview result"}],
    )
    workflow = create_workflow(
        model,
        pipeline_agent=FakeAgent(),
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )
    config = {"configurable": {"thread_id": "clarification-thread"}}

    paused = workflow.invoke(
        {"query": "Preview the data", "user_id": "@admin:test", "room_id": "room"},
        config=config,
    )
    result = workflow.invoke(Command(resume="hdfs:///data/input"), config=config)

    assert paused["__interrupt__"][0].value == {"question": "What is the data source?"}
    assert result["final_answer"] == "preview result"
    intake_calls = [call for call in model.invocations if call[0] is IntakeResult]
    assert (
        "only stage that may clarify requirements with the user" in intake_calls[0][1]
    )
    second_intake = intake_calls[1]
    assert "Additional user information: hdfs:///data/input" in second_intake[1]


def test_processing_flow_uses_pipeline_agent_without_builder(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    pipeline = _happy_pipeline_agent()
    builder = FakeAgent()
    model = _processing_model()
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=builder,
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "processing-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert result["final_answer"] == "workflow completed"
    assert result["input_metadata_id"] == "input-metadata"
    assert result["test_output_metadata_id"] == "test_output-metadata"
    assert result["output_metadata_id"] == "output-metadata"
    assert result["outputs"][0]["test_target"].startswith("hdfs:///data/output.__test_")
    assert result["test_run"]["application_status"] == "SUCCEEDED"
    assert result["production_run"]["application_status"] == "SUCCEEDED"
    assert store["submissions"] == 2
    final_prompt = [call for call in model.invocations if call[0] is FinalReport][0][1]
    assert "Production output metadata: output profile" in final_prompt
    assert "Production run metrics: []" in final_prompt
    assert [call[2].task for call in pipeline.invocations] == [
        PipelineTask.EVALUATE_OPERATORS,
        PipelineTask.PLAN_TEST,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_PRODUCTION,
    ]
    assert not builder.invocations


def test_processing_flow_profiles_multiple_outputs_individually(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    model = _processing_model()
    model.responses[IntakeResult] = [
        {
            "route": "process",
            "data_type": "file",
            "data_source": "hdfs:///data/input",
            "outputs": [
                {
                    "name": "documents",
                    "data_type": "file",
                    "target": "hdfs:///data/documents",
                },
                {
                    "name": "terms",
                    "data_type": "file",
                    "target": "hdfs:///data/terms",
                },
            ],
        }
    ]
    pipeline = _happy_pipeline_agent()
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {
            "query": "produce documents and terms",
            "user_id": "@admin:test",
            "room_id": "room",
        },
        config={"configurable": {"thread_id": "multi-output-thread"}},
    )

    suffix = result["request_id"].replace("-", "")[:12]
    assert result["stage"] == WorkflowStage.COMPLETED
    assert [output["name"] for output in result["outputs"]] == [
        "documents",
        "terms",
    ]
    assert store["profile_reads"] == [
        ("file", "hdfs:///data/input"),
        ("file", f"hdfs:///data/documents.__test_{suffix}"),
        ("file", f"hdfs:///data/terms.__test_{suffix}"),
        ("file", "hdfs:///data/documents"),
        ("file", "hdfs:///data/terms"),
    ]
    assert all(len(call[2].outputs) == 2 for call in pipeline.invocations)


def test_failed_output_profile_is_returned_for_pipeline_replanning(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    failed_once = False

    def read_data(data_type, source):
        nonlocal failed_once
        store["profile_reads"].append((data_type, source))
        if "terms.__test_" in source and not failed_once:
            failed_once = True
            raise RuntimeError("terms output does not exist")
        return f"schema and examples for {source}"

    monkeypatch.setattr(workflow_module.operations, "read_data", read_data)
    model = _processing_model()
    model.responses[IntakeResult] = [
        {
            "route": "process",
            "data_type": "file",
            "data_source": "hdfs:///data/input",
            "outputs": [
                {
                    "name": "documents",
                    "data_type": "file",
                    "target": "hdfs:///data/documents",
                },
                {
                    "name": "terms",
                    "data_type": "file",
                    "target": "hdfs:///data/terms",
                },
            ],
        }
    ]
    pipeline = FakeAgent(
        _result("plan-test-pipeline", evaluation_report="covered"),
        _result("submit-test-pipeline", pipeline_yaml="test: first"),
        _result("submit-test-pipeline", pipeline_yaml="test: repaired"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {
            "query": "produce documents and terms",
            "user_id": "@admin:test",
            "room_id": "room",
        },
        config={"configurable": {"thread_id": "multi-output-repair-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert result["test_run"]["submit_attempts"] == 2
    assert [call[2].task for call in pipeline.invocations] == [
        PipelineTask.EVALUATE_OPERATORS,
        PipelineTask.PLAN_TEST,
        PipelineTask.PLAN_TEST,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_PRODUCTION,
    ]
    assert (
        "terms output does not exist" in pipeline.invocations[2][2].validation_feedback
    )
    assert store["pipelines"]["test"]["content"] == "test: repaired"


def test_empty_pipeline_agent_response_is_retried(monkeypatch) -> None:
    _stub_business(monkeypatch)
    pipeline = FakeAgent(
        _result(
            "plan-test-pipeline",
            evaluation_report="all operators are available",
        ),
        (None, [{"role": "assistant", "content": "Collected tool evidence"}]),
        _result("submit-test-pipeline", pipeline_yaml="test: pipeline"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "empty-pipeline-response-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert [call[2].task for call in pipeline.invocations] == [
        PipelineTask.EVALUATE_OPERATORS,
        PipelineTask.PLAN_TEST,
        PipelineTask.PLAN_TEST,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_PRODUCTION,
    ]
    retry_state, _, retry_context = pipeline.invocations[2]
    assert (
        "previous call returned no structured result"
        in retry_state["messages"][-1]["content"]
    )
    assert retry_state["messages"][0]["content"] == "Collected tool evidence"
    assert retry_context.finalize_only is True


def test_invalid_stage_action_is_retried_before_workflow_fails(monkeypatch) -> None:
    _stub_business(monkeypatch)
    pipeline = FakeAgent(
        _result(
            "plan-test-pipeline",
            evaluation_report="all operators are available",
        ),
        _result("submit-test-pipeline", pipeline_yaml="test: pipeline"),
        _result("plan-production-pipeline"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "invalid-stage-action-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    retry_state, _, retry_context = pipeline.invocations[4]
    assert "plan-production-pipeline" in retry_state["messages"][-1]["content"]
    assert "size-production" in retry_state["messages"][-1]["content"]
    assert retry_context.finalize_only is True


def test_repeated_empty_pipeline_agent_response_fails_cleanly(monkeypatch) -> None:
    _stub_business(monkeypatch)
    pipeline = FakeAgent(
        _result(
            "plan-test-pipeline",
            evaluation_report="all operators are available",
        ),
        None,
        None,
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "repeated-empty-response-thread"}},
    )

    assert result["stage"] == WorkflowStage.FAILED
    assert (
        result["error"]["message"]
        == "Pipeline Agent returned no structured result twice in a row"
    )
    assert len(pipeline.invocations) == 3


def test_processing_without_output_uses_unique_temp_hive_table(monkeypatch) -> None:
    _stub_business(monkeypatch)
    model = _processing_model()
    model.responses[IntakeResult] = [
        {
            "route": "process",
            "data_type": "file",
            "data_source": "hdfs:///data/input.csv",
        }
    ]
    pipeline = _happy_pipeline_agent()
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {
            "query": "deduplicate input",
            "user_id": "@admin:test",
            "room_id": "room",
        },
        config={"configurable": {"thread_id": "default-output-thread"}},
    )

    suffix = result["request_id"].replace("-", "")
    output_target = f"temp.data_refiner_{suffix}"
    output = result["outputs"][0]
    assert result["stage"] == WorkflowStage.COMPLETED
    assert output["data_type"] == "table"
    assert output["target"] == output_target
    assert output["test_target"] == f"{output_target}__test_{suffix[:12]}"
    assert all(
        call[2].outputs[0].target == output_target for call in pipeline.invocations
    )
    intake_calls = [call for call in model.invocations if call[0] is IntakeResult]
    assert "this does not require clarification" in intake_calls[0][1]
    final_prompt = [call for call in model.invocations if call[0] is FinalReport][0][1]
    assert output_target in final_prompt


def test_missing_operator_uses_builder_then_reevaluates(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    model = _processing_model()
    pipeline = FakeAgent(
        _result(
            "build-operator",
            evaluation_report="one operator is missing",
            build_reference="build a tokenizer mapper",
        ),
        _result(
            "plan-test-pipeline",
            evaluation_report="all operators are available",
        ),
        _result("submit-test-pipeline", pipeline_yaml="test: pipeline"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    builder = FakeAgent(
        OperatorBuildResult(
            status="succeeded",
            summary="built",
            operator_name="tokenizer",
            operator_type="mapper",
            test_exit_code=0,
            docs_synced=True,
        ).model_dump()
    )
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=builder,
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "builder-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert result["operator_round"] == 2
    assert len(builder.invocations) == 1
    assert builder.invocations[0][2].build_reference_id == "reference-1"
    assert store["workspace_syncs"] == ["workspace", "conda"]
    assert result["workspace_synced"] is True
    assert result["conda_env_synced"] is True
    final_prompt = [call for call in model.invocations if call[0] is FinalReport][0][1]
    assert "Operator workspace synchronized: True" in final_prompt
    assert "Operator conda environment synchronized: True" in final_prompt
    assert "When a synchronization status is True" in final_prompt


def test_operator_sync_failure_stops_before_conda_sync(monkeypatch) -> None:
    store = _stub_business(monkeypatch)

    def fail_workspace_sync(request_id):
        raise RuntimeError("HDFS unavailable")

    monkeypatch.setattr(
        workflow_module.operations,
        "sync_workspace_for_request",
        fail_workspace_sync,
    )
    pipeline = FakeAgent(
        _result(
            "build-operator",
            evaluation_report="one operator is missing",
            build_reference="build a tokenizer mapper",
        )
    )
    builder = FakeAgent(
        OperatorBuildResult(
            status="succeeded",
            summary="built",
            test_exit_code=0,
            docs_synced=True,
        ).model_dump()
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=builder,
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "sync-failure-thread"}},
    )

    assert result["stage"] == WorkflowStage.FAILED
    assert "workspace synchronization failed" in result["error"]["message"]
    assert store["workspace_syncs"] == []
    assert result["workspace_synced"] is False


def test_conda_sync_failure_stops_before_operator_reevaluation(monkeypatch) -> None:
    store = _stub_business(monkeypatch)

    def fail_conda_sync(request_id):
        raise RuntimeError("conda packaging failed")

    monkeypatch.setattr(
        workflow_module.operations,
        "sync_conda_env_for_request",
        fail_conda_sync,
    )
    pipeline = FakeAgent(
        _result(
            "build-operator",
            evaluation_report="one operator is missing",
            build_reference="build a tokenizer mapper",
        )
    )
    builder = FakeAgent(
        OperatorBuildResult(
            status="succeeded",
            summary="built",
            test_exit_code=0,
            docs_synced=True,
        ).model_dump()
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=builder,
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "conda-sync-failure-thread"}},
    )

    assert result["stage"] == WorkflowStage.FAILED
    assert "conda environment synchronization failed" in result["error"]["message"]
    assert result["workspace_synced"] is True
    assert result["conda_env_synced"] is False
    assert store["workspace_syncs"] == ["workspace"]
    assert len(pipeline.invocations) == 1


def test_test_failure_persists_new_round_guide_before_builder(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    statuses = iter(["FAILED", "SUCCEEDED", "SUCCEEDED"])
    monkeypatch.setattr(
        workflow_module.operations,
        "wait_for_application",
        lambda application_id, **kwargs: {
            "status": next(statuses),
            "logs": {"driver": "operator import failed"},
        },
    )
    pipeline = FakeAgent(
        _result("plan-test-pipeline", evaluation_report="initial coverage"),
        _result("submit-test-pipeline", pipeline_yaml="test: first"),
        _result(
            "build-operator",
            evaluation_report="runtime failure reveals a missing operator",
            build_reference="build a compatible tokenizer mapper",
        ),
        _result("plan-test-pipeline", evaluation_report="coverage after repair"),
        _result("submit-test-pipeline", pipeline_yaml="test: repaired"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    builder = FakeAgent(
        OperatorBuildResult(
            status="succeeded",
            summary="built",
            operator_name="compatible_tokenizer",
            operator_type="mapper",
            test_exit_code=0,
            docs_synced=True,
        ).model_dump()
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=builder,
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "monitor-repair-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert store["evaluations"][2]["content"].startswith("runtime failure")
    assert store["references"][2]["content"].startswith("build a compatible")
    assert len(builder.invocations) == 1
    assert builder.invocations[0][2].operator_round == 2
    assert builder.invocations[0][2].build_reference_id == "reference-2"
    failure_context = pipeline.invocations[2][2]
    assert isinstance(failure_context, PipelineAgentContext)
    assert failure_context.task == PipelineTask.ANALYZE_TEST_FAILURE
    assert failure_context.application_id == "application_1_1"
    assert "operator import failed" in failure_context.failure_status
    assert "Failure evidence" in pipeline.invocations[2][0]["messages"][0]["content"]


def test_monitor_collects_node_owned_production_failure_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_module.operations,
        "register_application_monitor",
        lambda application_id: {},
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "wait_for_application",
        lambda application_id, **kwargs: {
            "status": "FAILED",
            "logs": {"driver": "executor lost"},
        },
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "get_spark_runtime_config_for_request",
        lambda request_id: ["--num-executors 2", "--executor-memory 2g"],
    )

    monitor = create_application_monitor_subgraph()
    result = monitor.invoke(
        {
            "request_id": "request",
            "stage": WorkflowStage.MONITORING_PRODUCTION_PIPELINE,
            "production_run": {"application_id": "application_1_1"},
        }
    )
    evidence = json.loads(result["failure_status"])

    assert evidence["application"]["logs"]["driver"] == "executor lost"
    assert evidence["spark_runtime_args"] == [
        "--num-executors 2",
        "--executor-memory 2g",
    ]


def test_extract_pipeline_metrics_maps_sparse_intermediate_counts() -> None:
    pipeline_yaml = """
read:
  op_name: regular_path_reader
  count: true
normalize:
  op_name: url_normalization_mapper
filter_score:
  op_name: numeric_filter
  count: true
normalize_again:
  op_name: url_normalization_mapper
filter_length:
  op_name: numeric_filter
  count: true
write:
  op_name: path_writer
"""
    payload = {
        "logs": {
            "containers": [
                {
                    "executor_id": "driver",
                    "stdout": (
                        "\x1b[1mThe data count of output of "
                        "RegularPathReader: 100\x1b[0m\n"
                        "The data count of output of NumericFilter: 80\n"
                        "The data count of output of NumericFilter: 50\n"
                    ),
                    "stdout_truncated": False,
                }
            ]
        }
    }

    metrics, error = operations.extract_pipeline_metrics(payload, pipeline_yaml)

    assert error is None
    assert metrics == [
        {
            "step_index": 0,
            "step_name": "read",
            "operator_name": "regular_path_reader",
            "row_count": 100,
            "previous_step_name": None,
            "row_count_delta": None,
        },
        {
            "step_index": 2,
            "step_name": "filter_score",
            "operator_name": "numeric_filter",
            "row_count": 80,
            "previous_step_name": "read",
            "row_count_delta": -20,
        },
        {
            "step_index": 4,
            "step_name": "filter_length",
            "operator_name": "numeric_filter",
            "row_count": 50,
            "previous_step_name": "filter_score",
            "row_count_delta": -30,
        },
    ]


def test_extract_pipeline_metrics_rejects_truncated_driver_log() -> None:
    metrics, error = operations.extract_pipeline_metrics(
        {
            "logs": {
                "containers": [
                    {
                        "executor_id": "driver",
                        "stdout": (
                            "The data count of output of RegularPathReader: 100"
                        ),
                        "stdout_truncated": True,
                    }
                ]
            }
        },
        "read:\n  op_name: regular_path_reader\n  count: true\n",
    )

    assert metrics == []
    assert error == "driver stdout is truncated; pipeline metrics are incomplete"


def test_monitor_preserves_successful_production_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_module.operations,
        "register_application_monitor",
        lambda application_id: {},
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "wait_for_application",
        lambda application_id, **kwargs: {
            "status": "SUCCEEDED",
            "logs": {
                "containers": [
                    {
                        "executor_id": "driver",
                        "stdout": ("The data count of output of RegularPathReader: 10"),
                        "stdout_truncated": False,
                    }
                ]
            },
        },
    )

    monitor = create_application_monitor_subgraph()
    result = monitor.invoke(
        {
            "request_id": "request",
            "stage": WorkflowStage.MONITORING_PRODUCTION_PIPELINE,
            "pipeline_agent_result": {
                "pipeline_yaml": (
                    "read:\n  op_name: regular_path_reader\n  count: true\n"
                )
            },
            "production_run": {"application_id": "application_1_1"},
        }
    )

    assert result["production_run"]["metrics"][0]["step_name"] == "read"
    assert result["production_run"]["metrics"][0]["row_count"] == 10
    assert result["production_run"]["metrics_error"] is None


def test_invalid_pipeline_is_returned_to_pipeline_agent_before_persisting(
    monkeypatch,
) -> None:
    store = _stub_business(monkeypatch)
    validations = iter(
        [
            {"success": False, "error": "unknown operator"},
            {"success": True},
            {"success": True},
        ]
    )
    monkeypatch.setattr(
        workflow_module.operations,
        "validate_pipeline_config",
        lambda request_id, pipeline_yaml: next(validations),
    )
    pipeline = FakeAgent(
        _result("plan-test-pipeline", evaluation_report="covered"),
        _result("submit-test-pipeline", pipeline_yaml="test: invalid"),
        _result("submit-test-pipeline", pipeline_yaml="test: repaired"),
        _result("plan-production-pipeline"),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    workflow = create_workflow(
        _processing_model(),
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "process data", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "pipeline-repair-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert store["pipelines"]["test"]["content"] == "test: repaired"
    feedback = pipeline.invocations[2][2].validation_feedback
    assert feedback.startswith("Primary correction for this round:")
    assert "unknown operator" in feedback


def test_rejected_test_output_is_replanned_before_production(monkeypatch) -> None:
    store = _stub_business(monkeypatch)
    model = _processing_model()
    model.responses[DataDescription].insert(
        2, {"description": "repaired test output profile"}
    )
    mismatch = (
        "The output still contains status=invalid records that should be filtered; "
        "correct the filter parameters"
    )
    pipeline = FakeAgent(
        _result("plan-test-pipeline", evaluation_report="covered"),
        _result("submit-test-pipeline", pipeline_yaml="test: first"),
        _result("retry-test-pipeline", summary=mismatch),
        _result("submit-test-pipeline", pipeline_yaml="test: repaired"),
        _result(
            "plan-production-pipeline", summary="Test output satisfies the request"
        ),
        _result("size-production", pipeline_yaml="production: pipeline"),
    )
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "filter invalid rows", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "test-output-repair-thread"}},
    )

    assert result["stage"] == WorkflowStage.COMPLETED
    assert store["submissions"] == 3
    assert [call[2].task for call in pipeline.invocations] == [
        PipelineTask.EVALUATE_OPERATORS,
        PipelineTask.PLAN_TEST,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_TEST,
        PipelineTask.VERIFY_TEST_OUTPUT,
        PipelineTask.PLAN_PRODUCTION,
    ]
    feedback = pipeline.invocations[3][2].validation_feedback
    assert feedback.startswith("Primary correction for this round:")
    assert mismatch in feedback
    assert store["pipelines"]["test"]["content"] == "test: repaired"


def test_pipeline_replanning_keeps_all_feedback_and_reports_last_rejection(
    monkeypatch,
) -> None:
    _stub_business(monkeypatch)
    model = _processing_model()
    model.responses[DataDescription] = [
        {"description": "input profile"},
        {"description": "first test output profile"},
        {"description": "second test output profile"},
        {"description": "third test output profile"},
    ]
    first_error = "Do not use min_frequency=2 on individual documents"
    second_error = "Do not combine drop and renames"
    third_error = "The output still contains duplicate characters"
    pipeline = FakeAgent(
        _result("plan-test-pipeline", evaluation_report="covered"),
        _result("submit-test-pipeline", pipeline_yaml="test: first"),
        _result("retry-test-pipeline", summary=first_error),
        _result("submit-test-pipeline", pipeline_yaml="test: second"),
        _result("retry-test-pipeline", summary=second_error),
        _result("submit-test-pipeline", pipeline_yaml="test: third"),
        _result("retry-test-pipeline", summary=third_error),
    )
    workflow = create_workflow(
        model,
        pipeline_agent=pipeline,
        operator_builder=FakeAgent(),
        checkpointer=MemorySaver(),
    )

    result = workflow.invoke(
        {"query": "clean text", "user_id": "@admin:test", "room_id": "room"},
        config={"configurable": {"thread_id": "feedback-history-thread"}},
    )

    third_plan_context = pipeline.invocations[5][2]
    assert first_error in third_plan_context.validation_feedback
    assert second_error in third_plan_context.validation_feedback
    assert result["pipeline_feedback_history"] == [
        first_error,
        second_error,
        third_error,
    ]
    assert third_error in result["error"]["message"]


@patch("data_refiner_agent_tools.workflows.request_workflow.History.append")
@patch(
    "data_refiner_agent_tools.workflows.request_workflow.RequestState.update_active_handoff"
)
@patch(
    "data_refiner_agent_tools.workflows.request_workflow.RequestState.compare_and_set_current_stage"
)
@patch("data_refiner_agent_tools.workflows.request_workflow.RequestState.get")
@patch("data_refiner_agent_tools.workflows.request_workflow.connect_database")
def test_transition_uses_database_compare_and_set(
    connect_database,
    get_state,
    compare_and_set,
    update_handoff,
    append_history,
) -> None:
    get_state.return_value = {"current_stage": WorkflowStage.RECEIVED}
    compare_and_set.return_value = {"current_stage": WorkflowStage.WORKSPACE_PREPARING}
    update_handoff.return_value = {"current_stage": WorkflowStage.WORKSPACE_PREPARING}

    result = RequestWorkflow.transition(
        "a3988658-859c-4545-8467-26e12d6547d0",
        WorkflowStage.WORKSPACE_PREPARING,
        "prepare-workspace",
    )

    assert result["current_stage"] == WorkflowStage.WORKSPACE_PREPARING
    compare_and_set.assert_called_once()
    update_handoff.assert_called_once()
    append_history.assert_called_once()
