"""Pipeline validation, examples, and execution."""

import logging
import os
import re
import subprocess
import shlex

from data_refiner_api.models.operations import PipelineValidationResponse
from data_refiner_api.paths import LocalPath
from data_refiner_api.services import workspace_proxy
from data_refiner_api.services.user_workspaces import normalize_user_id

logger = logging.getLogger(__name__)
_APPLICATION_ID = re.compile(r"\bapplication_\d+_\d+\b")
_PIPELINE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml$")
DEFAULT_OPTIONS = {
    "--num-executors": "2",
    "--executor-cores": "1",
    "--executor-memory": "512m",
    "--driver-memory": "512m",
}

DEFAULT_CONF = {
    "spark.executor.memoryOverhead": "1G",
    "spark.dynamicAllocation.enabled": "false",
    "spark.yarn.submit.waitAppCompletion": "false",
    "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./PYTHON_ENV/bin/python",
    "spark.executorEnv.PYSPARK_PYTHON": "./PYTHON_ENV/bin/python",
    "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|api[_-]?key",
}

ALLOWED_CONF_KEYS = {
    "spark.executor.memoryOverhead",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
}

INTEGER_OPTIONS = {
    "--num-executors",
    "--executor-cores",
}

MEMORY_OPTIONS = {
    "--executor-memory",
    "--driver-memory",
}

class PipelineSubmissionError(RuntimeError):
    """Raised when Spark cannot be submitted or its application id is absent."""


def validate_workspace_pipeline(
    api_url: str,
    pipeline_config_yaml_string: str,
) -> PipelineValidationResponse:
    """Validate a pipeline through its workspace API."""
    return PipelineValidationResponse.model_validate(
        workspace_proxy.post_url(
            api_url,
            "/pipeline-validation",
            {
                "pipeline_config_yaml_string": pipeline_config_yaml_string,
            },
        )
    )

def merge_spark_args(
        custom_args: list[str],
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    options = DEFAULT_OPTIONS.copy()
    conf = DEFAULT_CONF.copy()
    extra_files: list[str] = []

    for argument in custom_args:
        parts = shlex.split(argument)

        if len(parts) != 2:
            raise ValueError(
                f"Spark parameter must contain an option and a value: {argument}"
            )

        option, value = parts

        if option == "--files":
            files = value.split(",")
            if any(not file_path for file_path in files):
                raise ValueError("--files contains an empty path")
            extra_files.extend(files)
            continue

        if option in INTEGER_OPTIONS:
            if not value.isdigit() or int(value) <= 0:
                raise ValueError(f"{option} must be a positive integer")
            options[option] = value
            continue

        if option in MEMORY_OPTIONS:
            if not re.fullmatch(r"[1-9]\d*[mMgG]", value):
                raise ValueError(
                    f"{option} must use a value such as 512m or 2g"
                )
            options[option] = value
            continue

        if option == "--conf":
            key, separator, conf_value = value.partition("=")

            if not separator or not conf_value:
                raise ValueError(
                    f"--conf must use key=value format: {value}"
                )

            if key not in ALLOWED_CONF_KEYS:
                raise ValueError(
                    f"Spark configuration cannot be overridden: {key}"
                )

            conf[key] = conf_value
            continue

        raise ValueError(f"Spark option cannot be overridden: {option}")

    return options, conf, extra_files

def submit_command(user_name: str, pipeline_name: str, custom_args: list[str] | None = None) -> list[str | int]:
    """Build the existing Spark command for a user's persisted pipeline."""
    if normalize_user_id(user_name) != user_name:
        raise ValueError("user_name must be a normalized workspace user name")
    if not _PIPELINE_NAME.fullmatch(pipeline_name):
        raise ValueError("pipeline_name must be a YAML filename without directories")
    options, conf, extra_files = merge_spark_args(custom_args or [])
    if api_key := os.getenv("LITELLM_API_KEY"):
        conf["spark.yarn.appMasterEnv.LITELLM_API_KEY"] = api_key
        conf["spark.executorEnv.LITELLM_API_KEY"] = api_key
    workspace_path = f"hdfs:///workspaces/{user_name}"
    pipeline_file = f"{workspace_path}/pipelines/{pipeline_name}"
    for file_path in extra_files:
        if (
            not file_path.startswith(f"{workspace_path}/")
            or ".." in file_path.split("/")
            or "#" in file_path
        ):
            raise ValueError(
                f"--files path must belong to the user's workspace: {file_path}"
            )
    files_argument = ",".join([pipeline_file, *extra_files])

    command = [
        "spark-submit",
        "--master",
        "yarn",
        "--deploy-mode",
        "cluster",
    ]

    for option, value in options.items():
        command.extend([option, value])

    command.extend(
        [
            "--archives",
            f"{workspace_path}/data_refiner_env.tar.gz#PYTHON_ENV,{workspace_path}/data-refiner-runtime-resources.zip#data-refiner-runtime-resources",
        ]
    )

    for key, value in conf.items():
        command.extend(["--conf", f"{key}={value}"])

    command.extend(
        [
            "--files",
            files_argument,
            "--jars",
            "hdfs:///jars/graphframes-0.8.4-spark3.5-s_2.12.jar",
            "--py-files",
            (
                f"{workspace_path}/"
                "data_refiner_user_workspace-0.0.1-py3-none-any.whl"
            ),
            "hdfs:///python_scripts/run_cluster_args.py",
            "--pipeline_name",
            pipeline_name,
        ]
    )


    return command


def run_pipeline(user_id: str, pipeline_name: str,custom_args: list[str] | None = None) -> str:
    """Submit a user's persisted pipeline and return its YARN application id."""
    user_name = normalize_user_id(user_id)
    try:
        completed = subprocess.run(
            [str(argument) for argument in submit_command(user_name, pipeline_name,custom_args)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        output = "\n".join(
            value for value in (getattr(exc, "stdout", None), getattr(exc, "stderr", None))
            if value
        ).strip()
        raise PipelineSubmissionError(
            output or "spark-submit failed before the application was submitted"
        ) from exc

    output = "\n".join(value for value in (completed.stdout, completed.stderr) if value)
    match = _APPLICATION_ID.search(output)
    if match is None:
        raise PipelineSubmissionError(
            "spark-submit completed without returning an application id"
        )
    application_id = match.group(0)
    logger.info("Submitted pipeline %s for %s as %s", pipeline_name, user_name, application_id)
    return application_id


def get_pipeline_example() -> str:
    """Return the bundled pipeline example."""
    logger.info("Reading pipeline example")
    path = LocalPath.repo_root().joinpath("language_identification.yaml")
    return path.read_text(encoding="utf-8")
