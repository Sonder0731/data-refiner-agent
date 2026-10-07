FROM python:3.11-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.29 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HF_HOME=/app/.cache/huggingface

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY main.py ./
RUN uv run --no-sync python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('ibm-granite/granite-embedding-97m-multilingual-r2')"

ENV HF_HUB_OFFLINE=1 \
    PATH="/app/.venv/bin:$PATH"

USER 10001
EXPOSE 18000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "18000"]
