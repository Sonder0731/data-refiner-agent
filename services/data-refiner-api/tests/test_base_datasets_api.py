import io
import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from data_refiner_api.api.routes import base_datasets as base_dataset_route
from data_refiner_api.services import base_datasets


def _payload() -> dict:
    return {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/chinese_news",
        "data_name": "Chinese news",
        "data_description": "Chinese news articles",
        "field_descriptions": {
            "headline": {"description": "News headline"}
        },
        "examples": [{"headline": "Example"}],
    }


def test_embedding_request_uses_container_service_name(monkeypatch) -> None:
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        return io.BytesIO(
            json.dumps({"embeddings": [[0.0] * 384]}).encode()
        )

    monkeypatch.delenv("EMBEDDING_API_URL", raising=False)
    monkeypatch.setattr(base_datasets, "urlopen", fake_urlopen)

    assert base_datasets._request_embedding("Chinese news") == [0.0] * 384
    request, timeout = calls[0]
    assert request.full_url == "http://embedding-api:18000/embed"
    assert json.loads(request.data) == {"texts": ["Chinese news"]}
    assert timeout == 10


def test_create_base_dataset_saves_generated_embedding(monkeypatch) -> None:
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, statement, parameters) -> None:
            executed.append((statement, parameters))

        def fetchone(self):
            return (7, now, now)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        base_datasets,
        "_request_embedding",
        lambda text: [0.5] * 384,
    )
    monkeypatch.setattr(
        base_datasets.psycopg,
        "connect",
        lambda dsn, connect_timeout: Connection(),
    )

    result = base_datasets.create_base_dataset(**_payload())

    assert result["id"] == 7
    assert result["embedding_dimensions"] == 384
    statement, parameters = executed[0]
    assert "%s::vector" in statement
    assert json.loads(parameters[4]) == _payload()["field_descriptions"]
    assert json.loads(parameters[5]) == _payload()["examples"]
    assert json.loads(parameters[6]) == [0.5] * 384


def test_search_base_datasets_returns_nearest_matches(monkeypatch) -> None:
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, statement, parameters) -> None:
            executed.append((statement, parameters))

        def fetchall(self):
            return [
                (
                    7,
                    "hdfs",
                    "hdfs:///data/chinese_news",
                    "Chinese news",
                    "Chinese news articles",
                    {"headline": "News headline"},
                    [{"headline": "Example"}],
                    0.91,
                )
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        base_datasets,
        "_request_embedding",
        lambda text: [0.5] * 384,
    )
    monkeypatch.setattr(
        base_datasets.psycopg,
        "connect",
        lambda dsn, connect_timeout: Connection(),
    )

    results = base_datasets.search_base_datasets("Chinese reports", limit=3)

    assert results[0]["source_identifier"] == "hdfs:///data/chinese_news"
    assert results[0]["similarity"] == 0.91
    statement, parameters = executed[0]
    assert "ORDER BY embedding <=> %s::vector" in statement
    assert parameters == (json.dumps([0.5] * 384),) * 2 + (3,)


def test_base_dataset_conflict_is_returned_as_http_409(monkeypatch) -> None:
    def fail(**kwargs):
        raise base_datasets.BaseDatasetAlreadyExistsError("already exists")

    monkeypatch.setattr(
        base_dataset_route.base_dataset_service,
        "create_base_dataset",
        fail,
    )

    with pytest.raises(HTTPException) as exc_info:
        base_dataset_route.create_base_dataset(
            base_dataset_route.BaseDatasetCreateRequest(**_payload())
        )

    assert exc_info.value.status_code == 409


def test_invalid_embedding_dimension_is_rejected(monkeypatch) -> None:
    monkeypatch.setattr(
        base_datasets,
        "urlopen",
        lambda *_args, **_kwargs: io.BytesIO(
            json.dumps({"embeddings": [[0.0] * 3]}).encode()
        ),
    )

    with pytest.raises(base_datasets.EmbeddingServiceError, match="384"):
        base_datasets._request_embedding("Chinese news")
