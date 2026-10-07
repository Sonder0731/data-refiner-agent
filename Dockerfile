FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.11-slim-bookworm

COPY --from=uv /uv /uvx /usr/local/bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
RUN uv sync --locked --no-dev

ENTRYPOINT ["/app/.venv/bin/data-refiner-agent"]
