import json
import os
import re
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import httpx
import yaml
from langchain_core.callbacks.manager import CallbackManager
from psycopg.rows import dict_row

from data_refiner_agent_tools import (
    DataMetadata,
    NewOperatorBuildReference,
    OperatorsEvaluation,
    Pipeline,
    PipelineCase,
    PipelineRetrievalIndex,
    RequestState,
    SparkRuntimeConfig,
    add_package,
    connect_database,
    create_container,
    create_monitor_task,
    extract_username,
    get_cluster_resources,
    get_installed_packages,
    get_monitor_task,
    get_operator_code,
    get_operator_docs,
    get_operator_market,
    get_operator_test_code,
    get_pipeline_example,
    request_embedding,
    request_schema,
    run_doc_checker,
    run_pipeline,
    run_pytest,
    save_user_container,
    sync_conda_env,
    sync_workspace,
    validate_pipeline,
    write_operator_code,
    write_operator_test_code,
    write_pipeline,
)

DataCategory = Literal["input", "test_output", "output"]
OperatorType = Literal[
    "builtin",
    "deduplicator",
    "filter",
    "mapper",
    "other",
    "reader",
    "reducer",
    "sampler",
    "writer",
]
PipelineCategory = Literal["test", "production"]

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_PIPELINE_COUNT = re.compile(
    r"The data count of output of [A-Za-z0-9_]+: (?P<row_count>\d+)"
)
_SQL_RELATION = re.compile(
    r"\b(?:FROM|JOIN)\s+`?(?P<name>[A-Za-z_][A-Za-z0-9_.]*)`?", re.IGNORECASE
)
_SQL_CTE = re.compile(
    r"(?:\bWITH|,)\s*`?(?P<name>[A-Za-z_][A-Za-z0-9_]*)`?\s+AS\s*\(",
    re.IGNORECASE,
)


def _report_progress(callbacks: CallbackManager | None, message: str) -> None:
    if callbacks:
        callbacks.on_custom_event("progress", message)


def _require_request(request_id: str) -> dict[str, Any]:
    state = RequestState.get(request_id)
    if state is None:
        raise ValueError(f"request_id={request_id} was not found")
    return state


def _request_user_id(request_id: str) -> str:
    user_id = RequestState.get_user_id(request_id)
    if user_id is None:
        raise ValueError(f"request_id={request_id} was not found")
    return user_id


def _require_content(row: dict[str, Any] | None, description: str) -> str:
    if row is None or not row.get("content"):
        raise ValueError(f"{description} was not found or is empty")
    return str(row["content"])


def prepare_user_workspace(
    user_id: str,
    readiness_timeout_seconds: int = 90,
    callbacks: CallbackManager | None = None,
) -> dict[str, Any]:
    """Ensure a user's workspace container is running, healthy, and registered."""
    user_name = extract_username(user_id)
    container_name = f"data-refiner-user-workspace-{user_name}"
    container = create_container(
        {
            "image": os.getenv(
                "DATA_REFINER_WORKSPACE_IMAGE",
                "data-refiner-user-workspace:0.0.1",
            ),
            "name": container_name,
            "network": os.getenv(
                "DATA_REFINER_DOCKER_NETWORK",
                "data-refiner-agent-sparknet",
            ),
            "restart_policy": "unless-stopped",
            "environment": {"USER_NAME": user_name},
        }
    )

    deadline = time.monotonic() + readiness_timeout_seconds
    while True:
        try:
            workspace = save_user_container(user_id, container_name)
            return {"container": container, "workspace": workspace}
        except httpx.HTTPError:
            if time.monotonic() >= deadline:
                raise
            _report_progress(callbacks, "Workspace is not ready; continuing to wait")
            time.sleep(2)


def save_pipeline_case(
    request_id: str,
    belong: Literal["data-refiner", "user"],
    task_summary: str,
    processing_steps: str,
) -> dict[str, Any]:
    """Archive a completed production pipeline and its retrieval embedding."""
    state = _require_request(request_id)
    original_query = str(state["user_request"])
    pipeline = _require_content(
        Pipeline.get(request_id, "production"), "production pipeline"
    )
    input_description = _require_content(
        DataMetadata.get(request_id, "input"), "input metadata"
    )
    output_description = _require_content(
        DataMetadata.get(request_id, "output"), "output metadata"
    )
    runtime_rows = SparkRuntimeConfig.list_by_request_id(request_id)
    if len(runtime_rows) != 1 or not runtime_rows[0].get("config"):
        raise ValueError("exactly one non-empty Spark runtime config is required")

    embedding = request_embedding(original_query)["embeddings"][0]
    pipeline_case = PipelineCase.create(
        original_user_query=original_query,
        belong=belong,
        user_id=state["user_id"] if belong == "user" else None,
        input_data_desc=input_description,
        pipeline=pipeline,
        task_summary=task_summary,
        processing_steps=processing_steps,
        output_data_description=output_description,
        spark_runtime_config=runtime_rows[0]["config"],
    )
    retrieval = PipelineRetrievalIndex.create(
        pipeline_case_id=pipeline_case["id"],
        retrieval_text=original_query,
        embedding=embedding,
    )
    return {"pipeline_case": pipeline_case, "retrieval_index": retrieval}


def read_data(data_type: Literal["file", "table"], data_source: str) -> str:
    """Read schema, bounded examples, and size for an HDFS file or Hive table."""
    return request_schema(data_type=data_type, data_source=data_source)


def save_data_metadata(
    request_id: str,
    category: DataCategory,
    content: str,
) -> dict[str, Any]:
    """Persist an input, test-output, or production-output description."""
    with connect_database(row_factory=dict_row) as connection:
        row = DataMetadata.upsert(request_id, category, content, connection=connection)
        update_reference = {
            "input": RequestState.update_input_data_metadata_id,
            "test_output": RequestState.update_test_output_data_metadata_id,
            "output": RequestState.update_output_data_metadata_id,
        }[category]
        update_reference(request_id, row["id"], connection=connection)
        return row


def read_data_metadata(
    request_id: str,
    category: DataCategory,
) -> dict[str, Any]:
    """Read a persisted input, test-output, or output data description."""
    row = DataMetadata.get(request_id, category)
    if row is None:
        raise ValueError(
            f"{category} metadata for request_id={request_id} was not found"
        )
    return row


def get_operator_market_for_request(request_id: str) -> str:
    """Read the built-in and user-workspace operator market."""
    return str(get_operator_market(_request_user_id(request_id))["result"])


def get_operator_document(
    request_id: str,
    operator_name: str,
) -> str:
    """Read one operator document from the request user's catalog."""
    result = get_operator_docs(_request_user_id(request_id), operator_name)
    return str(result["result"])


def get_pipeline_example_for_agent() -> str:
    """Read the canonical Data Refiner pipeline example."""
    return str(get_pipeline_example()["result"])


def save_operator_evaluation(
    request_id: str,
    rounds: int,
    content: str,
) -> dict[str, Any]:
    """Persist one operator-market evaluation round."""
    with connect_database(row_factory=dict_row) as connection:
        row = OperatorsEvaluation.upsert(
            request_id, rounds, content, connection=connection
        )
        RequestState.update_operators_evaluation_id(
            request_id, row["id"], connection=connection
        )
        return row


def get_operator_evaluation_for_round(request_id: str, rounds: int) -> dict[str, Any]:
    """Read one exact operator-evaluation round."""
    row = OperatorsEvaluation.get(request_id, rounds)
    _require_content(row, f"operator evaluation round {rounds}")
    return row


def get_latest_operator_evaluation_for_request(request_id: str) -> str:
    """Read the newest operator-evaluation report for a request."""
    rows = OperatorsEvaluation.list_by_request_id(request_id)
    return _require_content(rows[-1] if rows else None, "operator evaluation")


def save_operator_build_reference(
    request_id: str,
    rounds: int,
    content: str,
) -> dict[str, Any]:
    """Persist one missing-operator build guide."""
    with connect_database(row_factory=dict_row) as connection:
        row = NewOperatorBuildReference.upsert(
            request_id, rounds, content, connection=connection
        )
        RequestState.update_new_operator_build_reference_id(
            request_id, row["id"], connection=connection
        )
        return row


def get_operator_build_reference_for_round(
    request_id: str, rounds: int
) -> dict[str, Any]:
    """Read one exact missing-operator build-guide round."""
    row = NewOperatorBuildReference.get(request_id, rounds)
    _require_content(row, f"operator build reference round {rounds}")
    return row


def search_similar_pipeline_cases(request_id: str) -> dict[str, Any]:
    """Return the five pipeline cases most similar to the original request."""
    state = _require_request(request_id)
    user_request = str(state["user_request"])
    embedding = request_embedding(user_request)["embeddings"][0]
    return {
        "request_id": request_id,
        "user_request": user_request,
        "matches": PipelineRetrievalIndex.search_similar(embedding, limit=5),
    }


def validate_pipeline_config(request_id: str, pipeline_yaml: str) -> dict[str, Any]:
    """Statically validate a YAML pipeline for the request user's workspace."""
    return validate_pipeline(_request_user_id(request_id), pipeline_yaml)


def validate_pipeline_outputs(
    pipeline_yaml: str,
    outputs: list[dict[str, Any]],
    environment: PipelineCategory,
) -> dict[str, Any]:
    """Require one correctly targeted writer for every declared output."""
    try:
        config = yaml.safe_load(pipeline_yaml)
    except yaml.YAMLError as exc:
        return {"success": False, "error": f"Invalid pipeline YAML: {exc}"}
    if not isinstance(config, dict):
        return {"success": False, "error": "Pipeline YAML root must be a mapping"}

    writers: list[tuple[str, str]] = []
    dataframes: set[str] = set()
    views: set[str] = set()
    for node_name, params in config.items():
        if not isinstance(params, dict):
            continue
        op_name = params.get("op_name")
        input_df = params.get("input_df")
        if isinstance(input_df, str) and input_df not in dataframes:
            return {
                "success": False,
                "error": (
                    f"Pipeline node {node_name!r} reads DataFrame {input_df!r} "
                    "before an earlier node produces it"
                ),
            }
        if op_name == "spark_sql_executor" and isinstance(
            params.get("sql_query"), str
        ):
            query = params["sql_query"]
            ctes = {match.group("name") for match in _SQL_CTE.finditer(query)}
            missing_views = sorted(
                {
                    match.group("name")
                    for match in _SQL_RELATION.finditer(query)
                    if "." not in match.group("name")
                    and match.group("name") not in views
                    and match.group("name") not in ctes
                }
            )
            if missing_views:
                return {
                    "success": False,
                    "error": (
                        f"Spark SQL node {node_name!r} references unavailable views "
                        f"{missing_views}; temp_view_name must be set on an earlier "
                        "producer because the current node registers its view only "
                        "after the SQL has executed"
                    ),
                }
        if op_name == "path_writer" and isinstance(params.get("path"), str):
            writers.append(("file", params["path"]))
        elif op_name == "hive_table_writer" and isinstance(
            params.get("table_name"), str
        ):
            writers.append(("table", params["table_name"]))
        if isinstance(params.get("output_df"), str):
            dataframes.add(params["output_df"])
        if isinstance(params.get("temp_view_name"), str):
            views.add(params["temp_view_name"])

    key = "test_target" if environment == "test" else "target"
    missing_targets = [
        str(output.get("name", "<unnamed>"))
        for output in outputs
        if not output.get(key)
    ]
    if missing_targets:
        return {
            "success": False,
            "error": f"Declared outputs are missing {key}: {missing_targets}",
        }
    expected = [(str(output["data_type"]), str(output[key])) for output in outputs]
    production = {
        (str(output["data_type"]), str(output["target"])) for output in outputs
    }
    if environment == "test":
        unsafe = [target for target in writers if target in production]
        if unsafe:
            return {
                "success": False,
                "error": f"Test pipeline writes production targets: {unsafe}",
            }
    if sorted(writers) != sorted(expected):
        missing = list(expected)
        unexpected: list[tuple[str, str]] = []
        for writer in writers:
            if writer in missing:
                missing.remove(writer)
            else:
                unexpected.append(writer)
        return {
            "success": False,
            "error": (
                "Pipeline writer targets do not match declared outputs; "
                f"missing={missing}, unexpected={unexpected}"
            ),
        }
    return {"success": True}


def save_pipeline_config(
    request_id: str,
    category: PipelineCategory,
    pipeline_yaml: str,
) -> dict[str, Any]:
    """Write a validated pipeline to the workspace and persist its YAML."""
    workspace = write_pipeline(_request_user_id(request_id), pipeline_yaml)
    with connect_database(row_factory=dict_row) as connection:
        database = Pipeline.upsert(
            request_id, category, pipeline_yaml, connection=connection
        )
        update_reference = (
            RequestState.update_test_pipeline_id
            if category == "test"
            else RequestState.update_pipeline_id
        )
        update_reference(request_id, database["id"], connection=connection)
    return {"workspace": workspace, "database": database}


def get_spark_runtime_config_for_request(request_id: str) -> list[str]:
    """Read and validate the request's single Spark runtime argument list."""
    rows = SparkRuntimeConfig.list_by_request_id(request_id)
    if len(rows) != 1:
        raise ValueError(
            f"request_id={request_id} must have exactly one runtime config"
        )
    try:
        config = json.loads(rows[0]["config"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Spark runtime config is not valid JSON") from exc
    if not isinstance(config, list) or not all(
        isinstance(item, str) for item in config
    ):
        raise ValueError("Spark runtime config must be a JSON array of strings")
    return config


def submit_pipeline(
    request_id: str,
    hdfs_pipeline_path: str,
    spark_runtime_config: list[str],
) -> dict[str, Any]:
    """Submit a persisted pipeline and return its Spark application ID."""
    if not all(isinstance(item, str) for item in spark_runtime_config):
        raise ValueError("spark_runtime_config must contain only strings")
    return run_pipeline(
        user_id=_request_user_id(request_id),
        pipeline_name=Path(hdfs_pipeline_path).name,
        spark_runtime_config=spark_runtime_config,
    )


def get_installed_packages_for_request(request_id: str) -> dict[str, Any]:
    """Read packages installed in the request user's workspace."""
    return get_installed_packages(_request_user_id(request_id))


def get_operator_implementation(
    request_id: str,
    operator_name: str,
) -> str:
    """Read one existing operator implementation."""
    result = get_operator_code(_request_user_id(request_id), operator_name)
    return str(result["result"])


def get_operator_test_implementation(
    request_id: str,
    operator_name: str,
) -> str:
    """Read one existing operator test implementation."""
    result = get_operator_test_code(_request_user_id(request_id), operator_name)
    return str(result["result"])


def install_workspace_package(
    request_id: str,
    package_name: str,
    version: str | None = None,
) -> dict[str, Any]:
    """Install one missing Python package in the request user's workspace."""
    return add_package(_request_user_id(request_id), package_name, version)


def save_operator_implementation(
    request_id: str,
    operator_type: OperatorType,
    operator_name: str,
    code: str,
) -> dict[str, Any]:
    """Save generated operator source in the request user's workspace."""
    return write_operator_code(
        _request_user_id(request_id), operator_type, operator_name, code
    )


def save_operator_test_implementation(
    request_id: str,
    operator_type: OperatorType,
    operator_name: str,
    test_code: str,
) -> dict[str, Any]:
    """Save generated operator test source in the request user's workspace."""
    workspace_test_code = test_code.replace(
        "from data_refiner.ops", "from workspace.ops"
    ).replace("from tests.tools", "from workspace.tests.tools")
    return write_operator_test_code(
        _request_user_id(request_id),
        operator_type,
        operator_name,
        workspace_test_code,
    )


def test_operator_implementation(request_id: str, test_path: str) -> dict[str, Any]:
    """Run one generated operator test file in the request user's workspace."""
    return run_pytest(_request_user_id(request_id), test_path)


def sync_operator_documentation(request_id: str) -> dict[str, Any]:
    """Run Data Refiner's documentation checker after operator tests pass."""
    return run_doc_checker(_request_user_id(request_id))


def sync_workspace_for_request(request_id: str) -> dict[str, Any]:
    """Build and upload the request user's workspace wheel to HDFS."""
    return sync_workspace(_request_user_id(request_id))


def sync_conda_env_for_request(request_id: str) -> dict[str, Any]:
    """Package and upload the request user's Python environment to HDFS."""
    return sync_conda_env(_request_user_id(request_id))


def get_input_metadata_for_request(request_id: str) -> str:
    """Read the persisted input description used for Spark sizing."""
    return _require_content(DataMetadata.get(request_id, "input"), "input metadata")


def get_test_output_metadata_for_request(request_id: str) -> str:
    """Read the persisted test-output description used for semantic validation."""
    return _require_content(
        DataMetadata.get(request_id, "test_output"), "test output metadata"
    )


def get_output_metadata_for_request(request_id: str) -> str:
    """Read the persisted production-output description used for final reporting."""
    return _require_content(DataMetadata.get(request_id, "output"), "output metadata")


def get_production_pipeline_for_request(request_id: str) -> str:
    """Read the persisted production pipeline used for Spark sizing."""
    return _require_content(
        Pipeline.get(request_id, "production"), "production pipeline"
    )


def get_test_pipeline_for_request(request_id: str) -> str:
    """Read the persisted test pipeline."""
    return _require_content(Pipeline.get(request_id, "test"), "test pipeline")


def get_cluster_resources_for_agent() -> dict[str, Any]:
    """Read current YARN memory and vCore capacity."""
    return get_cluster_resources()


def save_spark_runtime_config(
    request_id: str,
    config: list[str],
) -> dict[str, Any]:
    """Create or replace the request's Spark CLI argument list."""
    if not all(isinstance(item, str) for item in config):
        raise ValueError("config must contain only strings")
    serialized = json.dumps(config, ensure_ascii=False)
    rows = SparkRuntimeConfig.list_by_request_id(request_id)
    if len(rows) > 1:
        raise ValueError(f"request_id={request_id} has multiple runtime configs")
    if rows:
        return SparkRuntimeConfig.update_config(rows[0]["id"], serialized)
    return SparkRuntimeConfig.create(str(uuid4()), request_id, serialized)


def register_application_monitor(application_id: str) -> dict[str, Any]:
    """Register one Spark application with the monitor service."""
    return create_monitor_task(application_id)


def get_application_status(application_id: str) -> dict[str, Any]:
    """Read the latest monitor-service payload for one Spark application."""
    return get_monitor_task(application_id)


def find_application_status(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("final_status", "application_status", "state", "status"):
            value = payload.get(key)
            if isinstance(value, str):
                return value.upper()
        for value in payload.values():
            status = find_application_status(value)
            if status is not None:
                return status
    return None


def extract_pipeline_metrics(
    monitor_payload: dict[str, Any],
    pipeline_yaml: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Map driver count logs to every count-enabled YAML step in order."""
    try:
        pipeline = yaml.safe_load(pipeline_yaml)
    except yaml.YAMLError as exc:
        return [], f"pipeline YAML could not be parsed: {exc}"
    if not isinstance(pipeline, dict):
        return [], "pipeline YAML must contain a top-level mapping"

    counted_steps = [
        (index, str(step_name), str(config.get("op_name", "")))
        for index, (step_name, config) in enumerate(pipeline.items())
        if isinstance(config, dict) and config.get("count") is True
    ]
    if not counted_steps:
        return [], None

    logs = monitor_payload.get("logs")
    containers = logs.get("containers") if isinstance(logs, dict) else None
    if not isinstance(containers, list):
        return [], "monitor response does not contain container logs"
    driver = next(
        (
            container
            for container in containers
            if isinstance(container, dict) and container.get("executor_id") == "driver"
        ),
        None,
    )
    if driver is None:
        return [], "monitor response does not contain driver logs"
    if driver.get("stdout_truncated") is True:
        return [], "driver stdout is truncated; pipeline metrics are incomplete"

    stdout = driver.get("stdout")
    if not isinstance(stdout, str):
        return [], "driver stdout is unavailable"
    row_counts = [
        int(match.group("row_count"))
        for match in _PIPELINE_COUNT.finditer(_ANSI_ESCAPE.sub("", stdout))
    ]
    if len(row_counts) != len(counted_steps):
        return [], (
            f"expected {len(counted_steps)} pipeline count metrics, "
            f"found {len(row_counts)}"
        )

    metrics: list[dict[str, Any]] = []
    previous_step_name: str | None = None
    previous_row_count: int | None = None
    for (step_index, step_name, operator_name), row_count in zip(
        counted_steps, row_counts, strict=True
    ):
        metrics.append(
            {
                "step_index": step_index,
                "step_name": step_name,
                "operator_name": operator_name,
                "row_count": row_count,
                "previous_step_name": previous_step_name,
                "row_count_delta": (
                    None
                    if previous_row_count is None
                    else row_count - previous_row_count
                ),
            }
        )
        previous_step_name = step_name
        previous_row_count = row_count
    return metrics, None


def wait_for_application(
    application_id: str,
    poll_interval_seconds: int = 10,
    timeout_seconds: int = 3600,
    callbacks: CallbackManager | None = None,
) -> dict[str, Any]:
    """Poll a registered Spark application until a terminal status is returned."""
    terminal_statuses = {
        "SUCCEEDED",
        "SUCCESS",
        "COMPLETED",
        "FINISHED",
        "FAILED",
        "KILLED",
        "CANCELLED",
        "ERROR",
    }
    deadline = time.monotonic() + timeout_seconds
    while True:
        result = get_monitor_task(application_id)
        status = find_application_status(result)
        if status in terminal_statuses:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"monitoring {application_id} timed out")
        _report_progress(callbacks, f"Spark application status: {status or 'UNKNOWN'}")
        time.sleep(poll_interval_seconds)
