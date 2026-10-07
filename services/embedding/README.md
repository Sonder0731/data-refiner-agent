# Granite Embedding API

使用 [ibm-granite/granite-embedding-97m-multilingual-r2](https://huggingface.co/ibm-granite/granite-embedding-97m-multilingual-r2) 将文本批量转换为 384 维归一化向量。

## 本地运行

```bash
uv sync --locked
uv run uvicorn main:app --host 0.0.0.0 --port 18000
```

首次本地启动会从 Hugging Face 下载模型。接口文档位于 `http://localhost:18000/docs`。

```bash
curl -X POST http://localhost:18000/embed \
  -H 'Content-Type: application/json' \
  -d '{"texts":["今天天气很好","The weather is nice today."]}'
```

响应格式（以下向量已截断）：

```json
{
  "model": "ibm-granite/granite-embedding-97m-multilingual-r2",
  "embeddings": [[0.01, -0.02], [0.03, 0.04]]
}
```

每次请求可传入 1 至 128 条非空文本。`GET /health` 用于存活检查。

## Docker

模型会在构建阶段下载到镜像中，容器运行时无需访问 Hugging Face。根目录的统一
Compose 会将服务加入 `data-refiner-agent-sparknet`，同网络容器通过
`http://embedding-api:18000` 访问。

```bash
docker compose -f ../../compose.yaml up -d embedding-api
```

停止服务：

```bash
docker compose -f ../../compose.yaml down
```
