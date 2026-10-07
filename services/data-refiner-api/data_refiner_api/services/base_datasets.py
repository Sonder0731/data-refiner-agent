"""Create base dataset records with generated embeddings."""

import json
import math
import os
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import psycopg

from db import DEFAULT_DATABASE_DSN

DEFAULT_EMBEDDING_API_URL = "http://embedding-api:18000"
EMBEDDING_DIMENSIONS = 384


class EmbeddingServiceError(RuntimeError):
    """Raised when an embedding cannot be generated."""


class BaseDatasetAlreadyExistsError(RuntimeError):
    """Raised when a base dataset already exists."""


def _request_embedding(text: str) -> list[float]:
    api_url = os.getenv(
        "EMBEDDING_API_URL",
        DEFAULT_EMBEDDING_API_URL,
    ).rstrip("/")
    request = Request(
        f"{api_url}/embed",
        data=json.dumps({"texts": [text]}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise EmbeddingServiceError(
            "Failed to request the embedding service"
        ) from exc

    try:
        embedding = payload["embeddings"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise EmbeddingServiceError(
            "Embedding service response is invalid"
        ) from exc

    if (
        not isinstance(embedding, list)
        or len(embedding) != EMBEDDING_DIMENSIONS
        or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            for value in embedding
        )
    ):
        raise EmbeddingServiceError(
            f"Embedding must contain {EMBEDDING_DIMENSIONS} finite numbers"
        )
    try:
        normalized_embedding = [float(value) for value in embedding]
    except OverflowError as exc:
        raise EmbeddingServiceError(
            f"Embedding must contain {EMBEDDING_DIMENSIONS} finite numbers"
        ) from exc
    if any(not math.isfinite(value) for value in normalized_embedding):
        raise EmbeddingServiceError(
            f"Embedding must contain {EMBEDDING_DIMENSIONS} finite numbers"
        )
    return normalized_embedding


def create_base_dataset(
    *,
    source_type: str,
    source_identifier: str,
    data_name: str,
    data_description: str,
    field_descriptions: dict[str, Any],
    examples: list[dict[str, Any]],
) -> dict[str, Any]:
    embedding = _request_embedding(data_description)
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)

    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO base_dataset (
                    source_type,
                    source_identifier,
                    data_name,
                    data_description,
                    field_descriptions,
                    examples,
                    embedding
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::vector)
                ON CONFLICT (source_type, source_identifier) DO NOTHING
                RETURNING id, created_at, updated_at
                """,
                (
                    source_type,
                    source_identifier,
                    data_name,
                    data_description,
                    json.dumps(field_descriptions, ensure_ascii=False),
                    json.dumps(examples, ensure_ascii=False),
                    json.dumps(embedding),
                ),
            )
            row = cursor.fetchone()

    if row is None:
        raise BaseDatasetAlreadyExistsError(
            "A base dataset already exists for this data source"
        )
    return {
        "id": row[0],
        "source_type": source_type,
        "source_identifier": source_identifier,
        "data_name": data_name,
        "data_description": data_description,
        "field_descriptions": field_descriptions,
        "examples": examples,
        "embedding_dimensions": len(embedding),
        "created_at": row[1],
        "updated_at": row[2],
    }


def search_base_datasets(query: str, limit: int = 5) -> list[dict[str, Any]]:
    embedding = _request_embedding(query)
    serialized_embedding = json.dumps(embedding)
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)

    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    source_type,
                    source_identifier,
                    data_name,
                    data_description,
                    field_descriptions,
                    examples,
                    1 - (embedding <=> %s::vector) AS similarity
                FROM base_dataset
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (serialized_embedding, serialized_embedding, limit),
            )
            rows = cursor.fetchall()

    fields = (
        "id",
        "source_type",
        "source_identifier",
        "data_name",
        "data_description",
        "field_descriptions",
        "examples",
        "similarity",
    )
    return [dict(zip(fields, row, strict=True)) for row in rows]
