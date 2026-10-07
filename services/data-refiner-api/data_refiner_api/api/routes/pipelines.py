"""Pipeline validation and execution endpoints."""

from fastapi import APIRouter, HTTPException

from data_refiner_api.models.operations import (
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineValidationRequest,
    PipelineValidationResponse,
    TextResponse,
)
from data_refiner_api.services import (
    pipeline_operations,
    user_workspaces,
    workspace_proxy,
)

router = APIRouter(tags=["pipelines"])


@router.get("/example", response_model=TextResponse)
def get_pipeline_example() -> TextResponse:
    try:
        result = pipeline_operations.get_pipeline_example()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read pipeline example: {exc}",
        ) from exc
    return TextResponse(result=result)


@router.post(
    "/validate",
    response_model=PipelineValidationResponse,
    response_model_exclude_none=True,
)
def validate_pipeline(
    request: PipelineValidationRequest,
) -> PipelineValidationResponse:
    try:
        api_url = user_workspaces.get_api_url(request.user_id)
        return pipeline_operations.validate_workspace_pipeline(
            api_url,
            request.pipeline_config_yaml_string,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except workspace_proxy.WorkspaceProxyError as exc:
        raise HTTPException(exc.status_code, detail=exc.detail) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to validate pipeline configuration: {exc}",
        ) from exc


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(request: PipelineRunRequest) -> PipelineRunResponse:
    try:
        application_id = pipeline_operations.run_pipeline(
            request.user_id,
            request.pipeline_name,
            request.spark_runtime_config
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except pipeline_operations.PipelineSubmissionError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run pipeline: {exc}",
        ) from exc
    return PipelineRunResponse(application_id=application_id)
