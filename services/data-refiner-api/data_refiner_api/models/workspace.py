"""Contracts for user workspace proxy endpoints."""

from typing import Annotated

from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_refiner_api.models.operations import OperatorType

UserId = Annotated[
    str,
    Field(min_length=1, max_length=255, pattern=r"^\S+$"),
]
OperatorName = Annotated[
    str,
    Field(
        min_length=1,
        max_length=255,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
]


class WorkspaceRequest(BaseModel):
    user_id: UserId


class WriteOperatorRequest(WorkspaceRequest):
    operator_type: OperatorType
    operator_name: OperatorName
    content: str


class WritePipelineRequest(WorkspaceRequest):
    pipeline_config_yaml_string: str = Field(
        min_length=1,
        description="Pipeline configuration serialized as a YAML string.",
    )


class RunPytestRequest(WorkspaceRequest):
    path: str = Field(min_length=1)


class AddPackageRequest(WorkspaceRequest):
    model_config = ConfigDict(extra="forbid")

    package_name: str = Field(
        pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$"
    )
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
