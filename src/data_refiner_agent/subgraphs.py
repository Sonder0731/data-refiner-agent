import json
import re
import shlex
from typing import Any, Literal

from langchain_core.callbacks.manager import CallbackManager, dispatch_custom_event
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from data_refiner_agent import tools as operations
from data_refiner_agent.model import invoke_structured
from data_refiner_agent.workflow_types import (
    DataDescription,
    DataProfileState,
    MonitorOutcome,
    MonitorState,
    OutputSpec,
    PipelineRun,
    SparkSizingResult,
    SparkSizingState,
    WorkflowStage,
)

MAX_PROMPT_CHARS = 40_000
SUCCESS_STATUSES = {"SUCCEEDED", "SUCCESS", "COMPLETED", "FINISHED"}
REQUIRED_SPARK_OPTIONS = (
    "--num-executors",
    "--executor-cores",
    "--executor-memory",
    "--driver-memory",
)
INTEGER_SPARK_OPTIONS = {"--num-executors", "--executor-cores"}
MEMORY_SPARK_OPTIONS = {"--executor-memory", "--driver-memory"}
ALLOWED_SPARK_CONF_KEYS = {
    "spark.executor.memoryOverhead",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
}


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value[:MAX_PROMPT_CHARS]
    return json.dumps(value, ensure_ascii=False, default=str)[:MAX_PROMPT_CHARS]


def _model_config(config: RunnableConfig, name: str) -> RunnableConfig:
    return {
        **config,
        "metadata": {**config.get("metadata", {}), "agent_name": name},
    }


def _progress(config: RunnableConfig, message: str) -> None:
    dispatch_custom_event("progress", message, config=config)


def _run(value: PipelineRun | dict[str, Any] | None) -> PipelineRun:
    return PipelineRun.model_validate(value or {})


def create_data_profile_subgraph(model: Any):
    def read(state: DataProfileState, config: RunnableConfig) -> dict[str, Any]:
        stage = state.get("stage")
        if stage not in {
            WorkflowStage.PROFILING_TEST_OUTPUT,
            WorkflowStage.PROFILING_OUTPUT,
        }:
            source = state["data_source"]
            data_type = state["data_type"]
            _progress(config, f"Reading data profile: {source}")
            try:
                return {
                    "raw_profile": operations.read_data(data_type, source),
                    "profile_error": "",
                }
            except Exception as exc:
                return {
                    "profile_error": (
                        f"Failed to profile input data: {type(exc).__name__}: {exc}"
                    )
                }

        test = stage == WorkflowStage.PROFILING_TEST_OUTPUT
        profiles: list[str] = []
        errors: list[str] = []
        for raw_output in state.get("outputs", []):
            output = OutputSpec.model_validate(raw_output)
            source = output.test_target if test else output.target
            if not source:
                errors.append(f"Output {output.name!r} has no target")
                continue
            _progress(config, f"Reading data profile: {source}")
            try:
                raw_profile = operations.read_data(output.data_type, source)
            except Exception as exc:
                errors.append(
                    f"Failed to profile output {output.name!r} ({source}): "
                    f"{type(exc).__name__}: {exc}"
                )
                continue
            profiles.append(
                f"Output name: {output.name}\n"
                f"Data type: {output.data_type}\n"
                f"Target: {source}\n"
                f"Read result:\n{raw_profile}"
            )
        if errors:
            return {"profile_error": "\n".join(errors)}
        return {"raw_profile": "\n\n---\n\n".join(profiles), "profile_error": ""}

    def describe(state: DataProfileState, config: RunnableConfig) -> dict[str, Any]:
        result = invoke_structured(
            model,
            DataDescription,
            "Summarize the following read result as a bounded data profile. "
            "Preserve fields, types, samples, size, and evident data-quality "
            "information. Do not speculate.\n\n" + _text(state["raw_profile"]),
            _model_config(config, "data-profile-llm"),
        )
        return {"profile_description": result.description}

    def persist(state: DataProfileState, config: RunnableConfig) -> dict[str, Any]:
        description = state["profile_description"]
        if not state.get("request_id"):
            return {"preview_result": description}
        category: Literal["input", "test_output", "output"] = {
            WorkflowStage.PROFILING_TEST_OUTPUT: "test_output",
            WorkflowStage.PROFILING_OUTPUT: "output",
        }.get(WorkflowStage(state["stage"]), "input")
        _progress(config, f"Saving {category} data profile")
        row = operations.save_data_metadata(state["request_id"], category, description)
        return {f"{category}_metadata_id": row["id"]}

    graph = StateGraph(DataProfileState)
    graph.add_node("read-data", read)
    graph.add_node("describe-data", describe)
    graph.add_node("persist-metadata", persist)
    graph.add_edge(START, "read-data")
    graph.add_conditional_edges(
        "read-data",
        lambda state: "error" if state.get("profile_error") else "describe",
        {"error": END, "describe": "describe-data"},
    )
    graph.add_edge("describe-data", "persist-metadata")
    graph.add_edge("persist-metadata", END)
    return graph.compile()


def _memory_mb(value: str) -> int:
    value = value.strip().lower()
    if value.endswith("g"):
        return int(float(value[:-1]) * 1024)
    if value.endswith("m"):
        return int(float(value[:-1]))
    return int(value)


def _validate_runtime_args(args: list[str], resources: dict[str, Any]) -> str:
    values: dict[str, str] = {}
    for argument in args:
        parts = shlex.split(argument)
        if len(parts) != 2:
            return f"Spark parameter must contain an option and a value: {argument}"
        option, value = parts
        if option in INTEGER_SPARK_OPTIONS:
            if not value.isdigit() or int(value) <= 0:
                return f"{option} must be a positive integer"
            values[option] = value
        elif option in MEMORY_SPARK_OPTIONS:
            if not re.fullmatch(r"[1-9]\d*[mMgG]", value):
                return f"{option} must use a value such as 512m or 2g"
            values[option] = value
        elif option == "--conf":
            key, separator, conf_value = value.partition("=")
            if not separator or not conf_value:
                return f"--conf must use key=value format: {value}"
            if key not in ALLOWED_SPARK_CONF_KEYS:
                return f"Spark configuration cannot be overridden: {key}"
        else:
            return f"Spark option cannot be overridden: {option}"
    for option in REQUIRED_SPARK_OPTIONS:
        if option not in values:
            return f"missing {option}"
    executors = int(values["--num-executors"])
    cores = int(values["--executor-cores"])
    executor_memory = _memory_mb(values["--executor-memory"])
    driver_memory = _memory_mb(values["--driver-memory"])
    if executors < 1 or cores < 1:
        return "executor count and cores must be positive"
    available_cores = int(resources.get("available_vcores", 0))
    available_memory = int(resources.get("available_memory_mb", 0))
    if executors * cores > available_cores:
        return "requested executor vCores exceed available_vcores"
    if executors * executor_memory + driver_memory > available_memory:
        return "requested memory exceeds available_memory_mb"
    return ""


def create_spark_sizing_subgraph(model: Any, max_attempts: int = 2):
    def load(state: SparkSizingState, config: RunnableConfig) -> dict[str, Any]:
        _progress(config, "Reading production pipeline and cluster resources")
        context = {
            "input_metadata": operations.get_input_metadata_for_request(
                state["request_id"]
            ),
            "production_pipeline": operations.get_production_pipeline_for_request(
                state["request_id"]
            ),
            "operator_market": operations.get_operator_market_for_request(
                state["request_id"]
            ),
            "cluster_resources": operations.get_cluster_resources_for_agent(),
        }
        return {
            "sizing_context": _text(context),
            "cluster_resources": context["cluster_resources"],
            "sizing_attempt": 0,
            "sizing_error": "",
        }

    def size(state: SparkSizingState, config: RunnableConfig) -> dict[str, Any]:
        repair = (
            f"Correct the previous configuration error: {state['sizing_error']}. "
            if state.get("sizing_error")
            else ""
        )
        result = invoke_structured(
            model,
            SparkSizingResult,
            "Generate Spark arguments from the input size, pipeline, and currently "
            "available resources. The result must include --num-executors, "
            "--executor-cores, --executor-memory, and --driver-memory. Additional "
            "arguments may only set --conf spark.executor.memoryOverhead, "
            "spark.sql.shuffle.partitions, spark.default.parallelism, or "
            "spark.sql.adaptive.enabled. Total resources must not exceed "
            "available_vcores or available_memory_mb. Each list item may be one "
            f"complete argument string. {repair}\nContext: {state['sizing_context']}",
            _model_config(config, "spark-sizing-llm"),
        )
        return {
            "runtime_args": result.runtime_args,
            "sizing_rationale": result.rationale,
            "sizing_attempt": state.get("sizing_attempt", 0) + 1,
        }

    def validate(state: SparkSizingState) -> dict[str, Any]:
        try:
            error = _validate_runtime_args(
                state["runtime_args"], state["cluster_resources"]
            )
        except (TypeError, ValueError) as exc:
            error = str(exc)
        return {"sizing_error": error}

    def route_validation(state: SparkSizingState) -> Literal["persist", "repair"]:
        if not state.get("sizing_error"):
            return "persist"
        if state.get("sizing_attempt", 0) >= max_attempts:
            raise ValueError(f"invalid Spark runtime config: {state['sizing_error']}")
        return "repair"

    def persist(state: SparkSizingState, config: RunnableConfig) -> dict[str, Any]:
        _progress(config, "Saving Spark runtime arguments")
        row = operations.save_spark_runtime_config(
            state["request_id"], state["runtime_args"]
        )
        return {"spark_runtime_config_id": row["id"]}

    graph = StateGraph(SparkSizingState)
    graph.add_node("load-sizing-context", load)
    graph.add_node("size-spark", size)
    graph.add_node("validate-sizing", validate)
    graph.add_node("persist-sizing", persist)
    graph.add_edge(START, "load-sizing-context")
    graph.add_edge("load-sizing-context", "size-spark")
    graph.add_edge("size-spark", "validate-sizing")
    graph.add_conditional_edges(
        "validate-sizing",
        route_validation,
        {"persist": "persist-sizing", "repair": "size-spark"},
    )
    graph.add_edge("persist-sizing", END)
    return graph.compile()


def create_application_monitor_subgraph():
    def register(state: MonitorState, config: RunnableConfig) -> dict[str, Any]:
        environment: Literal["test", "production"] = (
            "test"
            if state["stage"] == WorkflowStage.MONITORING_TEST_PIPELINE
            else "production"
        )
        run = _run(state.get(f"{environment}_run"))
        if not run.application_id:
            raise ValueError(f"{environment} application_id is missing")
        _progress(
            config,
            f"Registering {environment} application monitoring: {run.application_id}",
        )
        operations.register_application_monitor(run.application_id)
        return {
            "monitor_environment": environment,
            "monitored_application_id": run.application_id,
        }

    def wait(state: MonitorState, config: RunnableConfig) -> dict[str, Any]:
        try:
            payload = operations.wait_for_application(
                state["monitored_application_id"],
                callbacks=CallbackManager.configure(config.get("callbacks")),
            )
            status = operations.find_application_status(payload) or "UNKNOWN"
            outcome = (
                MonitorOutcome.SUCCEEDED
                if status in SUCCESS_STATUSES
                else MonitorOutcome.FAILED
            )
        except TimeoutError as exc:
            payload = {"status": "TIMEOUT", "error": str(exc)}
            status = "TIMEOUT"
            outcome = MonitorOutcome.TIMEOUT
        environment = state["monitor_environment"]
        metrics: list[dict[str, Any]] = []
        metrics_error: str | None = None
        if outcome == MonitorOutcome.SUCCEEDED:
            pipeline_result = state.get("pipeline_agent_result")
            pipeline_yaml = (
                pipeline_result.get("pipeline_yaml", "")
                if isinstance(pipeline_result, dict)
                else ""
            )
            metrics, metrics_error = operations.extract_pipeline_metrics(
                payload, pipeline_yaml
            )
        run = PipelineRun.model_validate(
            {
                **_run(state.get(f"{environment}_run")).model_dump(mode="json"),
                "application_status": status,
                "metrics": metrics,
                "metrics_error": metrics_error,
            }
        )
        failure_evidence: dict[str, Any] = {"application": payload}
        if outcome == MonitorOutcome.FAILED and environment == "production":
            failure_evidence["spark_runtime_args"] = (
                operations.get_spark_runtime_config_for_request(state["request_id"])
            )
        return {
            f"{environment}_run": run.model_dump(mode="json"),
            "monitor_outcome": outcome.value,
            "failure_status": (
                "" if outcome == MonitorOutcome.SUCCEEDED else _text(failure_evidence)
            ),
        }

    graph = StateGraph(MonitorState)
    graph.add_node("register-monitor", register)
    graph.add_node("wait-for-application", wait)
    graph.add_edge(START, "register-monitor")
    graph.add_edge("register-monitor", "wait-for-application")
    graph.add_edge("wait-for-application", END)
    return graph.compile()


__all__ = [
    "create_application_monitor_subgraph",
    "create_data_profile_subgraph",
    "create_spark_sizing_subgraph",
]
