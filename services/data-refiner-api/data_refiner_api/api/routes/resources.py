"""Data size and YARN cluster resource endpoints."""

from fastapi import APIRouter, HTTPException

from data_refiner_api.models.resources import (
    ClusterResourceResponse,
    DataSizeResponse,
    HdfsResourceRequest,
    HiveResourceRequest,
)
from data_refiner_api.services import resource_planning

router = APIRouter(tags=["resources"])


@router.post("/hdfs", response_model=DataSizeResponse)
def read_hdfs_size(
    request: HdfsResourceRequest,
) -> DataSizeResponse:
    try:
        size_bytes = resource_planning.get_hdfs_size(request.hdfs_path)
    except resource_planning.ResourceInspectionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read HDFS data size: {exc}",
        ) from exc
    return DataSizeResponse(size_bytes=size_bytes)


@router.post("/hive", response_model=DataSizeResponse)
def read_hive_size(
    request: HiveResourceRequest,
) -> DataSizeResponse:
    try:
        size_bytes = resource_planning.get_hive_size(request.table_name)
    except resource_planning.ResourceInspectionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Hive data size: {exc}",
        ) from exc
    return DataSizeResponse(size_bytes=size_bytes)


@router.get("/cluster", response_model=ClusterResourceResponse)
def read_cluster_resources() -> ClusterResourceResponse:
    try:
        return ClusterResourceResponse(
            **resource_planning.get_cluster_resources()
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to read YARN cluster resources: {exc}",
        ) from exc
