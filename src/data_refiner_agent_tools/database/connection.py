from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from data_refiner_agent_tools._config import resolve_database_dsn


def connect_database(dsn: str | None = None, **kwargs):
    return psycopg.connect(resolve_database_dsn(dsn), **kwargs)


def request_uuid(request_id: str | UUID) -> UUID:
    return UUID(str(request_id))


@contextmanager
def connection_scope(
    *,
    dsn: str | None = None,
    connection=None,
) -> Iterator[Any]:
    if connection is not None:
        yield connection
        return
    with connect_database(dsn, row_factory=dict_row) as owned_connection:
        yield owned_connection


def fetch_one(
    query: str,
    parameters: Sequence[Any] = (),
    *,
    dsn: str | None = None,
    connection=None,
) -> dict[str, Any] | None:
    with connection_scope(dsn=dsn, connection=connection) as active_connection:
        row = active_connection.execute(query, parameters).fetchone()
    return dict(row) if row is not None else None


def fetch_all(
    query: str,
    parameters: Sequence[Any] = (),
    *,
    dsn: str | None = None,
    connection=None,
) -> list[dict[str, Any]]:
    with connection_scope(dsn=dsn, connection=connection) as active_connection:
        rows = active_connection.execute(query, parameters).fetchall()
    return [dict(row) for row in rows]


def require_row(
    row: dict[str, Any] | None,
    description: str,
) -> dict[str, Any]:
    if row is None:
        raise LookupError(f"{description} was not found")
    return row


def validate_choice(value: str, choices: set[str], field: str) -> None:
    if value not in choices:
        allowed = ", ".join(sorted(choices))
        raise ValueError(f"{field} must be one of: {allowed}")


def validate_rounds(rounds: int) -> None:
    if rounds < 1:
        raise ValueError("rounds must be greater than zero")
