# Spark Monitor

Monitor Spark applications through YARN and Spark History Server. The service
keeps task state in memory and exposes it through HTTP; it does not send
AgentTeams or Matrix messages.

## Run

```bash
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8010
```

## Create a monitor task

`POST /monitor/tasks` starts one background watcher per `application_id`:

```json
{
  "application_id": "application_1785846810816_0001"
}
```

The response is `202 Accepted`:

```json
{
  "application_id": "application_1785846810816_0001",
  "status": "watching"
}
```

Duplicate submissions for the same application return `already_watching` or
`already_finished`.

## Query task status

`GET /monitor/tasks/{application_id}` returns `404` when the application has
not been registered. Before the application reaches a terminal state, the
response contains no log field:

```json
{
  "application_id": "application_1785846810816_0001",
  "status": "RUNNING"
}
```

The initial status may be `WATCHING`; later values come from YARN or Spark
History. `SUCCEEDED`, `FAILED`, `KILLED`, and `FINISHED` are terminal. Only
then does the response include logs:

```json
{
  "application_id": "application_1785846810816_0001",
  "status": "FAILED",
  "logs": {
    "status": "available",
    "spark_attempt_id": "1",
    "containers": [
      {
        "executor_id": "driver",
        "container_id": "container_1",
        "host": "worker1:8042",
        "stdout": "driver output",
        "stdout_truncated": false
      }
    ],
    "errors": []
  }
}
```

The watcher polls every five seconds. At completion it reads the last 64 KiB
of stdout per driver/executor. Spark History may index executor metadata after
YARN reports completion, so log lookup retries every five seconds up to 12
times. Retrieval failures are returned in `logs.errors`.

All watcher state is process-local and is lost when the service restarts.

## Container

The Compose service joins the existing Spark Docker network:

```bash
docker compose build
docker compose up -d
```

Environment variables:

| Name | Host default / container default |
| --- | --- |
| `YARN_RESOURCE_MANAGER_URL` | `http://127.0.0.1:8088` / `http://hadoop-hive-spark-docker-master-1:8088` |
| `SPARK_HISTORY_URL` | `http://127.0.0.1:18080` / `http://hadoop-hive-spark-docker-history-1:18080` |
