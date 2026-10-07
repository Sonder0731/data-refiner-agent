"""Contracts for resource endpoints."""

from typing import Annotated

from pydantic import BaseModel, Field


class HdfsResourceRequest(BaseModel):
    """Request the size of an HDFS input."""

    hdfs_path: Annotated[
        str,
        Field(
            min_length=1,
            description="HDFS file or directory used to estimate input size.",
        ),
    ]


class HiveResourceRequest(BaseModel):
    """Request the size of a Hive table."""

    table_name: Annotated[
        str,
        Field(
            min_length=1,
            description="Hive table used to estimate input size.",
        ),
    ]


class DataSizeResponse(BaseModel):
    """Logical data size in bytes."""

    size_bytes: Annotated[int, Field(ge=0)]


class ClusterResourceResponse(BaseModel):
    """YARN cluster memory and virtual-core capacity."""

    total_memory_mb: Annotated[int, Field(ge=0)]
    available_memory_mb: Annotated[int, Field(ge=0)]
    allocated_memory_mb: Annotated[int, Field(ge=0)]
    total_vcores: Annotated[int, Field(ge=0)]
    available_vcores: Annotated[int, Field(ge=0)]
    allocated_vcores: Annotated[int, Field(ge=0)]
