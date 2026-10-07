"""User-to-container API address mappings."""

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from data_refiner_api.services import user_workspaces

router = APIRouter(tags=["user-workspaces"])


class UserContainerRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=255, pattern=r"^\S+$")
    container_name: str = Field(
        min_length=1,
        max_length=255,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$",
    )


class UserContainerResponse(UserContainerRequest):
    api_url: str
    updated_at: datetime


@router.post("", response_model=UserContainerResponse)
def save_user_container(
    request: UserContainerRequest,
) -> UserContainerResponse:
    try:
        mapping = user_workspaces.save_mapping(
            request.user_id,
            request.container_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Failed to save user-container mapping",
        ) from exc
    return UserContainerResponse.model_validate(mapping)
