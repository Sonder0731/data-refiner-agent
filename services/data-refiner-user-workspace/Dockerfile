# Build from this directory: docker build -t data-refiner-workspace:0.0.1 .
FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.11-slim-bookworm

ARG MINICONDA_INSTALLER=Miniconda3-py311_26.5.3-2-Linux-x86_64.sh
ARG MINICONDA_SHA256=cdca3dd8440759bb87c60b227e26946263fe2856b30b2bcfdd964c38254fb8eb

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_CACHE_DIR=/tmp/uv-cache \
    PATH=/opt/conda/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openjdk-17-jre-headless \
    && curl --fail --silent --show-error \
        "https://repo.anaconda.com/miniconda/${MINICONDA_INSTALLER}" \
        -o /tmp/miniconda.sh \
    && echo "${MINICONDA_SHA256}  /tmp/miniconda.sh" | sha256sum -c - \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /app
COPY --chown=app:app . ./

RUN uv sync --locked --no-dev \
    && uv pip install --python /app/.venv/bin/python hatchling uv==0.11.29 \
    && uv run dr_init \
    && uv run dr_pack_resources \
    && uv export --locked --no-dev --no-emit-project \
        --no-emit-package packaging --no-emit-package setuptools --no-hashes \
        --format requirements.txt --output-file requirements.txt \
    && CONDA_PLUGINS_AUTO_ACCEPT_TOS=yes \
        conda create --yes --name data_refiner_env python=3.11 pip \
    && conda run --name data_refiner_env \
        python -m pip install --no-cache-dir -r requirements.txt \
    && conda clean --all --yes \
    && rm -rf /tmp/uv-cache \
    && chown -R app:app /app /opt/conda \
    && chown app:app /app/requirements.txt

USER app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=45m --retries=6 \
    CMD ["/app/.venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/openapi.json', timeout=2)"]

CMD ["/app/.venv/bin/uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
