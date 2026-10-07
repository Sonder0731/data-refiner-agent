"""Contracts for operator documentation and pipeline endpoints."""

from typing import Annotated, Literal,List

from pydantic import BaseModel, Field

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


class TextResponse(BaseModel):
    """A plain-text business result wrapped in a JSON response."""

    result: str


class OperatorMarketRequest(BaseModel):
    user_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^\S+$",
        ),
    ]


class OperatorDocsRequest(BaseModel):
    user_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^\S+$",
        ),
    ]


class InstalledPackagesRequest(BaseModel):
    user_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^\S+$",
        ),
    ]


class InstalledPackage(BaseModel):
    name: str
    version: str


class InstalledPackagesResponse(BaseModel):
    python: str
    packages: list[InstalledPackage]


class PipelineValidationRequest(BaseModel):
    user_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^\S+$",
        ),
    ]
    pipeline_config_yaml_string: Annotated[
        str,
        Field(
            min_length=1,
            description="Pipeline configuration serialized as a YAML string.",
        ),
    ]


class PipelineValidationResponse(BaseModel):
    success: bool
    message: str | None = None
    error: str | None = None


class PipelineRunRequest(BaseModel):
    """Identify a user's persisted pipeline to submit to Spark."""

    user_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^\S+$",
        ),
    ]
    pipeline_name: Annotated[
        str,
        Field(
            min_length=1,
            max_length=255,
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*\.ya?ml$",
        ),
    ]
    spark_runtime_config: List[str]=Field(default_factory=list)


class PipelineRunResponse(BaseModel):
    """The YARN application created by a pipeline submission."""

    application_id: str
