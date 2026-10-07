from collections.abc import Sequence
from typing import Any

from data_refiner_agent_tools._config import (
    DEFAULT_EMBEDDING_API_URL,
    resolve_base_url,
)
from data_refiner_agent_tools._http import post_json


def request_embeddings(
    texts: Sequence[str],
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    if not 1 <= len(texts) <= 128:
        raise ValueError("texts must contain between 1 and 128 items")
    if any(not isinstance(text, str) or not text for text in texts):
        raise ValueError("each text must be a non-empty string")
    url = resolve_base_url(
        base_url,
        "EMBEDDING_API_URL",
        DEFAULT_EMBEDDING_API_URL,
    )
    return post_json(f"{url}/embed", {"texts": list(texts)})


def request_embedding(
    text: str,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    return request_embeddings([text], base_url=base_url)
