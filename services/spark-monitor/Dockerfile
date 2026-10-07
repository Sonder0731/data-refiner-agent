FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_CACHE_DIR=/tmp/uv-cache \
    YARN_RESOURCE_MANAGER_URL=http://hadoop-hive-spark-docker-master-1:8088 \
    SPARK_HISTORY_URL=http://hadoop-hive-spark-docker-history-1:18080

WORKDIR /app

RUN python -m pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev \
    && rm -rf /tmp/uv-cache

COPY main.py ./

EXPOSE 8010

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8010/health', timeout=3)"

CMD ["uv", "run", "--locked", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]
