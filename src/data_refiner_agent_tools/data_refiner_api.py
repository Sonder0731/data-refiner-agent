import re
from typing import Any

from data_refiner_agent_tools._config import (
    DEFAULT_DATA_REFINER_API_URL,
    resolve_base_url,
)
from data_refiner_agent_tools._http import get_json, post_json

OPERATOR_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SIZE_UNITS = ("B", "KB", "MB", "GB", "TB")


def _base_url(base_url: str | None) -> str:
    return resolve_base_url(
        base_url,
        "DATA_REFINER_URL",
        DEFAULT_DATA_REFINER_API_URL,
    )


def _format_size(size_bytes: int) -> str:
    value = float(size_bytes)
    unit = SIZE_UNITS[0]
    for unit in SIZE_UNITS:
        if value < 1024 or unit == SIZE_UNITS[-1]:
            break
        value /= 1024
    amount = f"{value:.2f}".rstrip("0").rstrip(".")
    return f"{amount} {unit}"


def read_file_schema(
    file_path: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/schemas/file",
        {"file_path": file_path},
    )


def read_table_schema(
    table_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/schemas/table",
        {"table_name": table_name},
    )


def read_hdfs_size(
    hdfs_path: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/resources/hdfs",
        {"hdfs_path": hdfs_path},
    )


def read_hive_size(
    table_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/resources/hive",
        {"table_name": table_name},
    )


def get_cluster_resources(
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return get_json(f"{_base_url(base_url)}/resources/cluster")


def request_schema(
    data_type: str,
    data_source: str,
    *,
    base_url: str | None = None,
) -> str:
    if data_type == "file":
        schema = read_file_schema(data_source, base_url=base_url)
        size = read_hdfs_size(data_source, base_url=base_url)
    elif data_type == "table":
        schema = read_table_schema(data_source, base_url=base_url)
        size = read_hive_size(data_source, base_url=base_url)
    else:
        raise ValueError("data_type must be file or table")
    return f"{schema['result']}\nData size:\n{_format_size(size['size_bytes'])}"


def get_operator_market(
    user_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/operators/market",
        {"user_id": user_id},
    )


def get_installed_packages(
    user_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/operators/installed-packages",
        {"user_id": user_id},
    )


def _operator_url(
    operator_name: str,
    suffix: str,
    base_url: str | None,
) -> str:
    if not OPERATOR_NAME_PATTERN.fullmatch(operator_name):
        raise ValueError(f"invalid operator_name: {operator_name}")
    return (
        f"{_base_url(base_url)}/operators/processing_operator/{operator_name}/{suffix}"
    )


def get_operator_docs(
    user_id: str,
    operator_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        _operator_url(operator_name, "docs", base_url),
        {"user_id": user_id},
    )


def get_operator_code(
    user_id: str,
    operator_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        _operator_url(operator_name, "code", base_url),
        {"user_id": user_id},
    )


def get_operator_test_code(
    user_id: str,
    operator_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        _operator_url(operator_name, "test-code", base_url),
        {"user_id": user_id},
    )


def get_pipeline_example(
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return get_json(f"{_base_url(base_url)}/pipelines/example")


def validate_pipeline(
    user_id: str,
    pipeline_config_yaml_string: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/pipelines/validate",
        {
            "user_id": user_id,
            "pipeline_config_yaml_string": pipeline_config_yaml_string,
        },
    )


def run_pipeline(
    user_id: str,
    pipeline_name: str,
    spark_runtime_config: list[str],
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/pipelines/run",
        {
            "user_id": user_id,
            "pipeline_name": pipeline_name,
            "spark_runtime_config": spark_runtime_config,
        },
    )


def save_user_container(
    user_id: str,
    container_name: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/user-containers",
        {"user_id": user_id, "container_name": container_name},
        timeout=10,
    )


def write_operator_code(
    user_id: str,
    operator_type: str,
    operator_name: str,
    content: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/write-operator-code",
        {
            "user_id": user_id,
            "operator_type": operator_type,
            "operator_name": operator_name,
            "content": content,
        },
    )


def write_operator_test_code(
    user_id: str,
    operator_type: str,
    operator_name: str,
    content: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/write-operator-test-code",
        {
            "user_id": user_id,
            "operator_type": operator_type,
            "operator_name": operator_name,
            "content": content,
        },
    )


def write_pipeline(
    user_id: str,
    pipeline_config_yaml_string: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/write-pipeline",
        {
            "user_id": user_id,
            "pipeline_config_yaml_string": pipeline_config_yaml_string,
        },
    )


def run_pytest(
    user_id: str,
    path: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/run-pytest",
        {"user_id": user_id, "path": path},
        timeout=1000,
    )


def run_doc_checker(
    user_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/run-doc-checker",
        {"user_id": user_id},
    )


def add_package(
    user_id: str,
    package_name: str,
    version: str | None = None,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    payload = {"user_id": user_id, "package_name": package_name}
    if version is not None:
        payload["version"] = version
    return post_json(
        f"{_base_url(base_url)}/workspace/add-package",
        payload,
        timeout=620,
    )


def sync_workspace(
    user_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/sync-workspace",
        {"user_id": user_id},
    )


def sync_conda_env(
    user_id: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return post_json(
        f"{_base_url(base_url)}/workspace/sync-conda-env",
        {"user_id": user_id},
        timeout=2000,
    )
