# Docker Container API

Authenticated FastAPI service that creates detached Docker containers through
the host Docker socket.

## Deploy

```bash
docker compose -f ../../compose.yaml up -d container-api
```

## Call

From another container, replace `HOST_GATEWAY` with its Docker bridge gateway
(usually the first address shown by `ip route`):

```bash
HOST_GATEWAY=$(ip route | awk '/default/ {print $3; exit}')
curl -sS -X POST "http://${HOST_GATEWAY}:19999/containers" \
  -H "X-API-Key: YOUR_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "image": "data-refiner-user-workspace:0.0.1",
    "name": "data-refiner-test",
    "environment": {"MODE": "test"},
    "ports": {"8000": 20000},
    "volumes": {"data-refiner-data": {"target": "/workspace"}},
    "restart_policy": "unless-stopped",
    "memory": "1g"
  }'
```

Read the deployed key from `.env`. The API intentionally rejects host bind
mounts and privileged container settings.

## Test

```bash
uv run python -m unittest -v
```
