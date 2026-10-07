"""Models used by schema inference endpoints."""

from typing import Annotated

from pydantic import BaseModel, Field


class FileSchemaRequest(BaseModel):
    file_path: Annotated[
        str,
        Field(
            min_length=1,
            description="HDFS file or directory path, for example hdfs:///data/events",
        ),
    ]


class TableSchemaRequest(BaseModel):
    table_name: Annotated[
        str,
        Field(
            min_length=1,
            description="Hive table name, optionally qualified with a database",
        ),
    ]


class SchemaResponse(BaseModel):
    result: str
