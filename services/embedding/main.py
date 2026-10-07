from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI
from pydantic import BaseModel, Field, StringConstraints
from sentence_transformers import SentenceTransformer

MODEL_ID = "ibm-granite/granite-embedding-97m-multilingual-r2"
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class EmbeddingRequest(BaseModel):
    texts: Annotated[list[Text], Field(min_length=1, max_length=128)]


class EmbeddingResponse(BaseModel):
    model: str
    embeddings: list[list[float]]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.model = SentenceTransformer(MODEL_ID)
    yield


app = FastAPI(title="Granite Embedding API", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/embed", response_model=EmbeddingResponse)
def embed(payload: EmbeddingRequest) -> EmbeddingResponse:
    embeddings = app.state.model.encode(
        payload.texts,
        normalize_embeddings=True,
    )
    return EmbeddingResponse(model=MODEL_ID, embeddings=embeddings.tolist())
