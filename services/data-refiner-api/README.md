# Data Refiner API

FastAPI service for inspecting schemas and operator documentation, validating
Data Refiner pipeline YAML, and running validated pipelines through Spark.

## Install and run

```bash
uv sync
./.venv/bin/python main.py
```

Spark must be able to reach the target HDFS cluster and Hive metastore. Configure
these in the normal Spark/Hadoop configuration (for example `core-site.xml`,
`hdfs-site.xml`, and `hive-site.xml`).

Interactive API documentation is available at `http://localhost:8000/docs`.

The user-container mapping endpoint connects to the local PostgreSQL container
on port `5433`. Enter its password without saving it in the repository:

```bash
read -rsp "PostgreSQL password: " PGPASSWORD && export PGPASSWORD
```

## Project structure

```text
.
├── main.py                         # ASGI compatibility entry point
├── data_refiner_api/
│   ├── main.py                     # Application factory and assembly
│   ├── api/
│   │   ├── router.py               # Top-level router registration
│   │   └── routes/
│   │       ├── health.py
│   │       ├── operators.py
│   │       ├── pipelines.py
│   │       ├── resources.py
│   │       └── schemas.py
│   ├── models/
│   │   ├── operations.py
│   │   └── schemas.py              # Request and response models
│   └── services/
│       ├── operator_catalog.py
│       ├── pipeline_operations.py
│       ├── resource_planning.py     # Spark/YARN resource recommendations
│       └── schema_inference.py      # Spark/HDFS business logic
├── workspace/                       # Custom operators and validated pipelines
├── language_identification.yaml     # Bundled pipeline example
├── run_pipeline_args.py             # Standalone pipeline CLI entry point
└── tests/
```

To add a new API area, create a route module under `api/routes` and register its
router in `api/router.py`. Keep transport concerns in route modules, Pydantic
contracts in `models`, and Spark or other business logic in `services`.

## Endpoints

Infer the schema of an HDFS file or directory:

```bash
curl -X POST http://localhost:8000/schemas/file \
  -H 'Content-Type: application/json' \
  -d '{"file_path":"hdfs:///data/events"}'
```

Infer a Hive table schema:

```bash
curl -X POST http://localhost:8000/schemas/table \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"analytics.events"}'
```

Read the built-in and workspace operator markets:

```bash
curl -X POST http://localhost:8000/operators/market \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1"}'
```

Read one processing operator document:

```bash
curl -X POST \
  http://localhost:8000/operators/deduplicator/exact_deduplicator/docs \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1"}'
```

Read an operator's source or test source. The API checks the installed
Data Refiner package first, then the user's workspace container:

```bash
curl -X POST \
  http://localhost:8000/operators/filter/length_filter/code \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1"}'

curl -X POST \
  http://localhost:8000/operators/filter/length_filter/test-code \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1"}'
```

Validate a pipeline through the user's workspace API:

```bash
curl -X POST http://localhost:8000/pipelines/validate \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1","pipeline_config_yaml_string":"sample_node:\n  op_name: sample\n  sample: 0.5\n"}'
```

Submit a persisted pipeline from a user's HDFS workspace. The API returns the
YARN application id after submission without waiting for completion:

```bash
curl -X POST http://localhost:8000/pipelines/run \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"@admin:matrix-local.agentteams.io","pipeline_name":"xxx.yaml"}'
```

Read the bundled pipeline example:

```bash
curl http://localhost:8000/pipelines/example
```

Read the logical size of an HDFS file or directory:

```bash
curl -X POST http://localhost:8000/resources/hdfs \
  -H 'Content-Type: application/json' \
  -d '{"hdfs_path":"hdfs:///data/events"}'
```

Read the logical size of a Hive table:

```bash
curl -X POST http://localhost:8000/resources/hive \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"analytics.events"}'
```

Create or update a user's FastAPI container mapping:

```bash
curl -X POST http://localhost:8000/user-containers \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1","container_name":"hiclaw-worker-pipeline-agent"}'
```

The endpoint health-checks `http://<container_name>:8000/health` before saving
the mapping. The API process must be on the same Docker network as the
workspace containers so their names resolve. Set `DATABASE_URL` to override
the PostgreSQL connection settings.

Workspace development operations are proxied through the user's mapped
workspace container. Matrix IDs are normalized before lookup, so
`@admin:matrix-local.agentteams.io:18081` maps to workspace user `admin`:

```bash
curl -X POST http://localhost:8000/workspace/write-operator-code \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"@admin:matrix-local.agentteams.io:18081","operator_type":"filter","operator_name":"length_filter","content":"..."}'

curl -X POST http://localhost:8000/workspace/run-pytest \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"@admin:matrix-local.agentteams.io:18081","path":"workspace/tests/filter/test_length_filter.py"}'

curl -X POST http://localhost:8000/workspace/sync-workspace \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"@admin:matrix-local.agentteams.io:18081"}'
```
