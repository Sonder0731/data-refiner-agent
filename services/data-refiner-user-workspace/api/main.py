import json
import os
import subprocess
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager
from enum import Enum
from importlib.metadata import distributions
from pathlib import Path
from typing import Annotated

import yaml
from fastapi import FastAPI, HTTPException, Path as ApiPath
from packaging.version import InvalidVersion, Version
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)
from path_set import LocalPath
from webhdfs_client import WebHDFSStorage


@asynccontextmanager
async def lifespan(_: FastAPI):
    package_conda_env()
    sync_workspace()
    sync_runtime_resources()
    yield


app = FastAPI(lifespan=lifespan)
WORKSPACE = LocalPath.workspace_root()
CONDA_ENV = "data_refiner_env"
CONDA_PACK = Path(sys.executable).with_name("conda-pack")
REQUIREMENTS = LocalPath.repo_root() / "requirements.txt"
RUNTIME_RESOURCES = LocalPath.repo_root() / "data-refiner-runtime-resources.zip"
ADD_PACKAGE_TIMEOUT = 600


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


class FileRequest(BaseModel):
    operator_type: str
    operator_name: str
    content: str


class PytestRequest(BaseModel):
    path: str


class PackageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_name: str = Field(pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
    version: str | None = None

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return str(Version(value))
        except InvalidVersion as exc:
            raise ValueError("version must be a valid exact package version") from exc


class InstalledPackage(BaseModel):
    name: str
    version: str


class InstalledPackagesResponse(BaseModel):
    python: str
    packages: list[InstalledPackage]


class WorkSpace(BaseModel):
    user_name: str = Field(pattern=r"^@?[A-Za-z0-9][A-Za-z0-9._-]*$")


class PipelineRequest(BaseModel):
    pipeline_config_yaml_string: str = Field(min_length=1)


class OperatorType(str, Enum):
    builtin = "builtin"
    reader = "reader"
    mapper = "mapper"
    filter = "filter"
    deduplicator = "deduplicator"
    reducer = "reducer"
    sampler = "sampler"
    writer = "writer"
    other = "other"


class TextResponse(BaseModel):
    result: str


ProcessingOperatorName = Annotated[
    str,
    ApiPath(
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Processing operator module name without an extension.",
    ),
]


def current_user_name() -> str:
    user_name = os.getenv("USER_NAME")
    if user_name is None:
        raise HTTPException(500, "USER_NAME environment variable is not set")

    try:
        return WorkSpace(user_name=user_name).user_name
    except ValidationError as exc:
        raise HTTPException(500, "USER_NAME environment variable is invalid") from exc


@app.get("/ops_market", response_model=TextResponse, tags=["operators"])
def get_ops_market() -> TextResponse:
    try:
        result = (WORKSPACE / "docs" / "ops_market.md").read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator market: {exc}",
        ) from exc
    return TextResponse(result=result)


def resolve_processing_operator_type(
    operator_name: str,
) -> OperatorType | None:
    """Resolve a workspace operator's physical category by name."""
    matches = [
        operator_type
        for operator_type in OperatorType
        if (
            WORKSPACE
            / "ops"
            / operator_type.value
            / f"{operator_name}.py"
        ).is_file()
    ]
    if len(matches) > 1:
        raise HTTPException(
            409,
            f"Ambiguous processing operator name: {operator_name}",
        )
    return matches[0] if matches else None


@app.get(
    "/processing_operator/{operator_name}/docs",
    response_model=TextResponse,
    tags=["operators"],
)
def get_processing_operator_docs_by_name(
    operator_name: ProcessingOperatorName,
) -> TextResponse:
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        raise HTTPException(
            404,
            f"Processing operator document not found: {operator_name}",
        )
    return get_processing_operator_docs(operator_type, operator_name)


@app.get(
    "/processing_operator/{operator_name}/code",
    response_model=TextResponse,
    tags=["operators"],
)
def get_processing_operator_code_by_name(
    operator_name: ProcessingOperatorName,
) -> TextResponse:
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        raise HTTPException(
            404,
            f"Processing operator source not found: {operator_name}",
        )
    return get_operator_code(operator_type, operator_name)


@app.get(
    "/processing_operator/{operator_name}/test-code",
    response_model=TextResponse,
    tags=["operators"],
)
def get_processing_operator_test_code_by_name(
    operator_name: ProcessingOperatorName,
) -> TextResponse:
    operator_type = resolve_processing_operator_type(operator_name)
    if operator_type is None:
        raise HTTPException(
            404,
            f"Processing operator test source not found: {operator_name}",
        )
    return get_operator_test_code(operator_type, operator_name)


@app.get(
    "/{operator_type}/{operator_name}/docs",
    response_model=TextResponse,
    tags=["operators"],
)
def get_processing_operator_docs(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        ApiPath(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the .md extension.",
        ),
    ],
) -> TextResponse:
    try:
        result = (
            WORKSPACE / "docs" / operator_type.value / f"{operator_name}.md"
        ).read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator document: {exc}",
        ) from exc
    return TextResponse(result=result)


def operator_path(operator_type: str, operator_name: str) -> Path:
    try:
        path = WORKSPACE.joinpath(f"ops/{operator_type}/{operator_name}.py")
    except ValueError:
        raise HTTPException(400, "path must be inside workspace")
    return path


@app.get(
    "/{operator_type}/{operator_name}/code",
    response_model=TextResponse,
    tags=["operators"],
)
def get_operator_code(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        ApiPath(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the .py extension.",
        ),
    ],
) -> TextResponse:
    try:
        result = operator_path(operator_type.value, operator_name).read_text(
            encoding="utf-8"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator code: {exc}",
        ) from exc
    return TextResponse(result=result)


@app.get(
    "/{operator_type}/{operator_name}/test-code",
    response_model=TextResponse,
    tags=["operators"],
)
def get_operator_test_code(
    operator_type: OperatorType,
    operator_name: Annotated[
        str,
        ApiPath(
            min_length=1,
            pattern=r"^[a-z][a-z0-9_]*$",
            description="Operator module name without the test_ prefix and .py extension.",
        ),
    ],
) -> TextResponse:
    try:
        result = operator_test_path(operator_type.value, operator_name).read_text(
            encoding="utf-8"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read operator test code: {exc}",
        ) from exc
    return TextResponse(result=result)


def operator_test_path(operator_type: str, operator_name: str) -> Path:
    try:
        path = WORKSPACE.joinpath(f"tests/{operator_type}/test_{operator_name}.py")
    except ValueError:
        raise HTTPException(400, "path must be inside workspace")
    return path

@app.post("/write-operator-code")
def write_operator_file(request: FileRequest):
    target = operator_path(request.operator_type, request.operator_name)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(request.content, encoding="utf-8")
    return {"path": str(target.relative_to(WORKSPACE))}


@app.post("/write-operator-test-code")
def write_operator_test_file(request: FileRequest):
    target = operator_test_path(request.operator_type, request.operator_name)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(request.content, encoding="utf-8")
    return {"path": str(target)}


def sync_conda_dependencies():
    try:
        export = subprocess.run(
            [
                "uv",
                "export",
                "--locked",
                "--no-dev",
                "--no-emit-project",
                "--no-emit-package",
                "packaging",
                "--no-emit-package",
                "setuptools",
                "--no-hashes",
                "--format",
                "requirements.txt",
                "--output-file",
                str(REQUIREMENTS),
            ],
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(LocalPath.repo_root()),
            timeout=300,
            check=False,
        )
        if export.returncode:
            raise HTTPException(
                500, f"requirements export failed: {export.stderr[-4000:]}"
            )

        install = subprocess.run(
            [
                "conda",
                "run",
                "--name",
                CONDA_ENV,
                "python",
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "-r",
                str(REQUIREMENTS),
            ],
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(LocalPath.repo_root()),
            timeout=600,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "conda dependency sync timed out")

    if install.returncode:
        raise HTTPException(500, f"conda dependency sync failed: {install.stderr[-4000:]}")

    return {
        "exit_code": install.returncode,
        "stdout": install.stdout,
        "stderr": install.stderr,
    }


@app.post("/run-pytest")
def run_pytest(request: PytestRequest):
    target = Path(request.path)
    if not target.is_absolute():
        target = LocalPath.repo_root() / target
    target = target.resolve()

    if not target.is_relative_to(WORKSPACE.resolve()):
        raise HTTPException(400, "test path must be inside workspace")
    if target.suffix != ".py" or not target.name.startswith("test_"):
        raise HTTPException(400, "path must point to a test_*.py file")
    if not target.is_file():
        raise HTTPException(404, "test file not found")

    try:
        cmd = ["uv", "run", "pytest", "-q", "-s", "--tb=short", str(target)]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(WORKSPACE.parent),
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "pytest timed out after 300 seconds")

    response = {
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "conda_sync": None,
    }
    if result.returncode == 0:
        response["conda_sync"] = sync_conda_dependencies()
    return response


@app.post("/run-doc-checker")
def run_doc_checker():
    try:
        result = subprocess.run(
            ["uv", "run", "doc-checker"],
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(LocalPath.repo_root()),
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "doc checker timed out after 300 seconds")

    return {
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


@app.post("/add-package")
def add_package(request: PackageRequest):
    requirement = request.package_name
    if request.version is not None:
        requirement += f"=={request.version}"
    try:
        result = subprocess.run(
            ["uv", "add", requirement],
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(LocalPath.repo_root()),
            timeout=ADD_PACKAGE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(
            408, f"uv add timed out after {ADD_PACKAGE_TIMEOUT} seconds"
        )

    if result.returncode:
        raise HTTPException(500, f"uv add failed: {result.stderr[-4000:]}")

    return {
        "status": "succeeded",
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


@app.get("/installed-packages", response_model=InstalledPackagesResponse)
def get_installed_packages() -> InstalledPackagesResponse:
    packages = sorted(
        (
            InstalledPackage(name=distribution.name, version=distribution.version)
            for distribution in distributions()
        ),
        key=lambda package: package.name.casefold(),
    )
    return InstalledPackagesResponse(python=sys.executable, packages=packages)


@app.post("/write-pipeline")
def write_pipeline(request: PipelineRequest):
    user_name = current_user_name()
    try:
        yaml.safe_load(request.pipeline_config_yaml_string)
    except yaml.YAMLError as exc:
        raise HTTPException(400, f"Invalid YAML: {exc}") from exc

    filename = f"{uuid.uuid4()}.yaml"
    hdfs_path = f"/workspaces/{user_name}/pipelines/{filename}"
    write_string = request.pipeline_config_yaml_string
    try:
        write_string = yaml.dump(
            json.loads(request.pipeline_config_yaml_string),
            allow_unicode=True,
            sort_keys=False
        )
    except:
        pass


    storage = WebHDFSStorage(
        namenode_url="http://master:9870",
        user="jupyter",
        root_dir=f"/workspaces/{user_name}",
    )
    storage.write_text(hdfs_path, write_string)
    return {"hdfs_path": hdfs_path}


@app.post("/sync-workspace")
def sync_workspace():
    user_name = current_user_name()
    try:
        build = subprocess.run(
            ["uv", "build"],
            capture_output=True,
            text=True,
            errors="replace",
            cwd=str(LocalPath.repo_root()),
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "workspace build timed out")

    if build.returncode:
        raise HTTPException(500, f"workspace build failed: {build.stderr[-4000:]}")

    storage = WebHDFSStorage(
        namenode_url="http://master:9870",
        user="jupyter",
        root_dir=f"/workspaces/{user_name}",
    )
    wheel = LocalPath.repo_root() / "dist/data_refiner_user_workspace-0.0.1-py3-none-any.whl"
    storage.upload_file(wheel, f"/workspaces/{user_name}/{wheel.name}", True)
    return {"result": "Sync workspace successfully"}


def sync_runtime_resources():
    user_name = current_user_name()
    storage = WebHDFSStorage(
        namenode_url="http://master:9870",
        user="jupyter",
        root_dir=f"/workspaces/{user_name}",
    )
    hdfs_path = f"/workspaces/{user_name}/{RUNTIME_RESOURCES.name}"
    storage.upload_file(RUNTIME_RESOURCES, hdfs_path, overwrite=True)
    return {"result": "success", "hdfs_path": hdfs_path}


@app.post("/sync-conda-env")
def package_conda_env():
    user_name = current_user_name()
    try:
        info = subprocess.run(
            ["conda", "info", "--envs", "--json"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(408, "conda info timed out")

    if info.returncode:
        raise HTTPException(500, f"conda info failed: {info.stderr[-4000:]}")

    try:
        envs = json.loads(info.stdout)["envs"]
    except (json.JSONDecodeError, KeyError):
        raise HTTPException(500, "invalid conda info output")

    env_path = next((Path(path) for path in envs if Path(path).name == CONDA_ENV), None)
    if env_path is None or not env_path.is_dir():
        raise HTTPException(500, f"conda environment not found: {CONDA_ENV}")

    with tempfile.TemporaryDirectory(prefix="data-refiner-env-") as temp_dir:
        archive = Path(temp_dir) / f"{CONDA_ENV}.tar.gz"
        try:
            packed = subprocess.run(
                [
                    str(CONDA_PACK),
                    "--prefix",
                    str(env_path),
                    "--output",
                    str(archive),
                    "--format",
                    "tar.gz",
                    "--compress-level",
                    "1",
                    "--n-threads",
                    "-1",
                    "--force",
                ],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=1800,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(408, "conda environment packaging timed out")

        if packed.returncode:
            raise HTTPException(500, f"conda-pack failed: {packed.stderr[-4000:]}")

        storage = WebHDFSStorage(
            namenode_url="http://master:9870",
            user="jupyter",
            root_dir=f"/workspaces/{user_name}",
        )
        hdfs_path = f"/workspaces/{user_name}/{archive.name}"
        storage.upload_file(archive, hdfs_path, overwrite=True)

    return {"result": "success", "hdfs_path": hdfs_path}


# region: pipeline validation
import yaml
import importlib
from data_refiner.core.meta_operator import Operator
from typing import Any
from data_refiner.ops_registry import OPS_MAPPING

@app.post("/pipeline-validation")
def pipeline_validation(request: PipelineRequest):
    return validate_pipeline(request.pipeline_config_yaml_string)


def _workspace_ops_mapping() -> dict[str, type[Operator]]:
    """Reload and return operators registered by the writable workspace."""
    try:
        import workspace.ops.ops_market as sub_ops_market
    except ModuleNotFoundError as exc:
        raise RuntimeError(f"Workspace operator market is unavailable: {exc}") from exc

    try:
        importlib.reload(sub_ops_market)
        from workspace.ops.ops_market import OPS_SET

        mnf = lambda x: x.split(".")[-1]
        OPS_MAPPING = {mnf(op.__module__): op for op in OPS_SET}
    except ModuleNotFoundError as exc:
        raise RuntimeError(f"Workspace operator market cannot be reloaded: {exc}") from exc
    return OPS_MAPPING


def _operator_parameter_names(cls: type[Operator]) -> set[str]:
    names = {"op_name"}
    for base in cls.__mro__:
        model = base.__dict__.get("config")
        if isinstance(model, type) and issubclass(model, BaseModel):
            names.update(model.model_fields)
    return names


def _invalid(error: str) -> dict[str, Any]:
    return {"success": False, "error": error}
def validate_pipeline(pipeline_config_yaml_string: str) -> dict[str, Any]:
    """Validate a pipeline and persist it under the workspace when valid."""
    print("Validating pipeline configuration")
    workspace_ops_mapping = _workspace_ops_mapping()

    try:
        config = yaml.safe_load(pipeline_config_yaml_string)
    except yaml.YAMLError as exc:
        print("Pipeline configuration is not valid YAML")
        return _invalid(f"Invalid pipeline configuration: {exc}")

    if not isinstance(config, dict):
        return _invalid(
            "Invalid pipeline configuration: the YAML root must be a mapping"
        )

    for node_name, op_config in config.items():
        if not isinstance(op_config, dict):
            return _invalid(
                f"Operator parameters invalid: node {node_name!r} must be a mapping"
            )

        op_name = op_config.get("op_name")
        if not isinstance(op_name, str) or not op_name:
            return _invalid(
                f"Operator parameters invalid: node {node_name!r} "
                "must define a non-empty op_name"
            )

        try:
            cls: type[Operator] | None = OPS_MAPPING.get(op_name)
            if cls is None:
                cls = workspace_ops_mapping.get(op_name)
            if cls is None:
                raise ValueError(f"unknown operator: {op_name}")
            unsupported = sorted(set(op_config) - _operator_parameter_names(cls))
            if unsupported:
                unsupported_names = ", ".join(unsupported)
                raise ValueError(
                    f"node {node_name!r} has unsupported parameters for "
                    f"{op_name}: {unsupported_names}"
                )
            cls(**op_config)
        except Exception as exc:
            print(f"Operator {op_name} failed validation: {exc}")
            return _invalid(f"Operator parameters invalid: {exc}")

    return {
        "success": True,
        "message": "Pipeline configuration is valid.",
    }
# endregion
