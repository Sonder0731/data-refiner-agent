"""Schema inference endpoints."""

from fastapi import APIRouter, HTTPException

from data_refiner_api.models.schemas import (
    FileSchemaRequest,
    SchemaResponse,
    TableSchemaRequest,
)
from data_refiner_api.services import schema_inference

router = APIRouter(tags=["schemas"])


@router.post("/file", response_model=SchemaResponse)
def read_file_schema(request: FileSchemaRequest) -> SchemaResponse:
    try:
        result = schema_inference.infer_file_schema(request.file_path)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to infer file schema: {exc}",
        ) from exc
    return SchemaResponse(result=result)


@router.post("/table", response_model=SchemaResponse)
def read_table_schema(request: TableSchemaRequest) -> SchemaResponse:
    try:
        result = schema_inference.infer_table_schema(request.table_name)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to infer table schema: {exc}",
        ) from exc
    return SchemaResponse(result=result)
