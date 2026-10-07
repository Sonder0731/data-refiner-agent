import os
import secrets
from typing import Annotated, Literal

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field, StringConstraints


API_KEY = os.environ["CONTAINER_API_KEY"]

DockerName = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$", max_length=128),
]
AbsolutePath = Annotated[str, StringConstraints(pattern=r"^/", max_length=4096)]
Port = Annotated[int, Field(ge=1, le=65535)]


class VolumeMount(BaseModel):
    target: AbsolutePath
    read_only: bool = False


class CreateContainerRequest(BaseModel):
    image: str = Field(min_length=1, max_length=512)
    name: DockerName | None = None
    command: list[str] | None = None
    environment: dict[str, str] = Field(default_factory=dict)
    ports: dict[Port, Port] = Field(default_factory=dict)
    volumes: dict[DockerName, VolumeMount] = Field(default_factory=dict)
    network: DockerName | None = None
    restart_policy: Literal["no", "always", "unless-stopped", "on-failure"] = "no"
    memory: str | None = Field(default=None, pattern=r"^[1-9][0-9]*[bkmgBKMG]?$", max_length=32)
    working_dir: AbsolutePath | None = None


class CreateContainerResponse(BaseModel):
    id: str
    name: str
    image: str
    status: str


app = FastAPI(title="Docker Container API", version="0.0.1")


@app.post("/containers", response_model=CreateContainerResponse, status_code=201)
def create_container(
    request: CreateContainerRequest,
    response: Response,
    x_api_key: Annotated[str | None, Header()] = None,
) -> CreateContainerResponse:
    if x_api_key is None or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="invalid API key")

    options = {
        "image": request.image,
        "name": request.name,
        "command": request.command,
        "environment": request.environment,
        "ports": request.ports,
        "volumes": {
            name: {"bind": mount.target, "mode": "ro" if mount.read_only else "rw"}
            for name, mount in request.volumes.items()
        },
        "restart_policy": {"Name": request.restart_policy},
        "detach": True,
        "labels": {"managed-by": "docker-container-api"},
    }
    for key, value in {
        "network": request.network,
        "mem_limit": request.memory,
        "working_dir": request.working_dir,
    }.items():
        if value is not None:
            options[key] = value

    client = None
    try:
        client = docker.from_env()
        if request.name is not None:
            try:
                container = client.containers.get(request.name)
            except NotFound:
                pass
            else:
                container.reload()
                response.status_code = 200
                return CreateContainerResponse(
                    id=container.id,
                    name=container.name,
                    image=request.image,
                    status=container.status,
                )

        container = client.containers.run(**options)
        container.reload()
        return CreateContainerResponse(
            id=container.id,
            name=container.name,
            image=request.image,
            status=container.status,
        )
    except ImageNotFound as exc:
        raise HTTPException(status_code=404, detail=f"image not found: {request.image}") from exc
    except APIError as exc:
        raise HTTPException(status_code=400, detail=str(exc.explanation or exc)) from exc
    except DockerException as exc:
        raise HTTPException(status_code=503, detail="Docker daemon unavailable") from exc
    finally:
        if client is not None:
            client.close()
