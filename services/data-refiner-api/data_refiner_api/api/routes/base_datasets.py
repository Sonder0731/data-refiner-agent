"""Create base datasets used for semantic lookup."""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, StringConstraints

from data_refiner_api.services import base_datasets as base_dataset_service

router = APIRouter(tags=["base-datasets"])
NonEmptyText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class BaseDatasetCreateRequest(BaseModel):
    source_type: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=100,
            pattern=r"^\S+$",
        ),
    ]
    source_identifier: NonEmptyText
    data_name: NonEmptyText
    data_description: NonEmptyText
    field_descriptions: dict[str, Any] = Field(default_factory=dict)
    examples: list[dict[str, Any]] = Field(default_factory=list)


class BaseDatasetCreateResponse(BaseDatasetCreateRequest):
    id: int
    embedding_dimensions: int
    created_at: datetime
    updated_at: datetime


class BaseDatasetSearchRequest(BaseModel):
    query: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=10000),
    ]
    limit: Annotated[int, Field(ge=1, le=20)] = 5


class BaseDatasetSearchResult(BaseDatasetCreateRequest):
    id: int
    similarity: float


class BaseDatasetSearchResponse(BaseModel):
    results: list[BaseDatasetSearchResult]


@router.post(
    "",
    response_model=BaseDatasetCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_base_dataset(
    request: BaseDatasetCreateRequest,
) -> BaseDatasetCreateResponse:
    try:
        result = base_dataset_service.create_base_dataset(
            **request.model_dump()
        )
    except base_dataset_service.BaseDatasetAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except base_dataset_service.EmbeddingServiceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to create base dataset",
        ) from exc
    return BaseDatasetCreateResponse.model_validate(result)


@router.post("/search", response_model=BaseDatasetSearchResponse)
def search_base_datasets(
    request: BaseDatasetSearchRequest,
) -> BaseDatasetSearchResponse:
    try:
        results = base_dataset_service.search_base_datasets(
            request.query,
            request.limit,
        )
    except base_dataset_service.EmbeddingServiceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to search base datasets",
        ) from exc
    return BaseDatasetSearchResponse.model_validate({"results": results})
