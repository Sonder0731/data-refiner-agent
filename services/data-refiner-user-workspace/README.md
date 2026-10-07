# Data Refiner User Workspace

Data Refiner 的用户扩展工作区，用于开发、测试、记录和打包自定义 PySpark 算子。项目产物是一个仅包含 `workspace/` 的 wheel，由 Data Refiner API、Agent 或其他宿主程序加载；本仓库本身不是独立的流水线运行器。

当前内置示例是 [`LengthFilter`](workspace/ops/filter/length_filter.py)，用于按字符串长度对 DataFrame 行进行打标或过滤。

## 主要能力

- 按算子类型组织用户代码：`reader`、`mapper`、`filter`、`deduplicator`、`reducer`、`sampler`、`writer`、`builtin` 和 `other`。
- 自动扫描 `workspace.ops`，发现带有 `@processing_operator` 标记的算子类。
- 根据类说明、Pydantic 参数模型和 `CONSTRAINT` 自动生成算子文档。
- 运行 pytest，并将 `workspace/` 构建为可分发的 wheel。
- 通过辅助 HTTP API 写入算子代码、触发检查，并在指定环境中同步 wheel 到 WebHDFS。

## 环境要求

- Python `3.11`；`pyproject.toml` 明确限制为 `>=3.11,<3.12`。
- [uv](https://docs.astral.sh/uv/)。
- 可供 PySpark 3.5.2 使用的 Java 运行时。
- PyPI 上的 `data-refiner-pyspark==1.0.0`。

- 只有调用 `/sync-workspace` 时才需要可访问的 WebHDFS。

## 快速开始

```bash
uv sync
uv run pytest
uv run doc-checker
uv build
```

命令说明：

- `uv run pytest`：运行 `pyproject.toml` 配置的 `tests/` 测试。目前该测试验证用户算子能够被自动发现。
- `uv run doc-checker`：重建 `workspace/docs/ops_market.md` 和各算子文档。生成文件会被覆盖，不要在其中维护无法从代码元数据生成的内容。
- `uv build`：在 `dist/` 中生成 `data_refiner_user_workspace-0.0.1` 的 wheel 和源码包。

## 项目结构

```text
.
├── api/main.py                 # 辅助 FastAPI 接口
├── doc_checker.py              # 算子文档生成器
├── path_set.py                 # 本地目录约定
├── webhdfs_client.py           # WebHDFS 文件操作封装
├── tests/                      # 默认 pytest 测试目录
└── workspace/                  # wheel 中实际打包的内容
    ├── docs/                   # 自动生成的算子市场和详情文档
    ├── jar/                    # GraphFrames Spark 依赖
    ├── ops/
    │   ├── ops_market.py       # 自定义算子发现
    │   └── <operator_type>/    # 各类型算子实现
    ├── pipelines/              # 流水线 YAML 的持久化目录
    ├── results/                # 结果预留目录
    └── tests/                  # 随 wheel 分发的 Spark 测试支持代码
```

根目录的 API、文档工具和 WebHDFS 客户端不会被打进 wheel。

## 开发自定义算子

1. 在 `workspace/ops/<operator_type>/` 下创建 snake_case 命名的模块。
2. 继承 Data Refiner 提供的合适元算子，例如 `Filter`、`SimpleMapper`、`Reader` 或 `Writer`。
3. 使用 `@processing_operator` 标记算子类。
4. 用 Pydantic `BaseModel` 声明算子特有参数，并通过 `check_params` 读取。
5. 实现 `process()`；需要通用输出处理时使用 `@resonance`。
6. 为 `OperatorConstraint` 子类提供 `CONSTRAINT`，然后运行测试和文档生成。

最小可参考实现见：

- 代码：[`workspace/ops/filter/length_filter.py`](workspace/ops/filter/length_filter.py)
- 参数与约束文档：[`workspace/docs/filter/length_filter.md`](workspace/docs/filter/length_filter.md)
- 算子市场：[`workspace/docs/ops_market.md`](workspace/docs/ops_market.md)

`LengthFilter` 在流水线中的配置形式如下；宿主程序需要先加载并注册 workspace 算子：

```yaml
filter_by_length:
  op_name: length_filter
  input_df: raw_df
  output_df: filtered_df
  field: text
  min_length: 10
  max_length: 1000
  mode: filter
```

`mode` 支持：

- `tag`：保留全部行并添加布尔标记列。
- `filter`：仅保留满足条件的行，并移除临时标记列。
- `tag_and_filter`：仅保留满足条件的行，同时保留标记列。

## 算子发现与文档

`workspace.ops.ops_market` 使用 `pkgutil.walk_packages` 递归导入 `workspace.ops` 下的模块。只有同时满足以下条件的类会进入 `OPS_MAPPING`：

- 类定义在当前被扫描的模块中，而不是从其他模块导入；
- 类带有 `__operator_type__ == "PROCESSING"`，通常由 `@processing_operator` 设置；
- 模块不是 `workspace.ops.ops_market` 本身。

`doc-checker` 使用该发现结果生成：

- 总览：`workspace/docs/ops_market.md`；
- 详情：`workspace/docs/<operator_type>/<module_name>.md`。

详情文档来自类 docstring、继承关系、各层 Pydantic `*Params`、`EXAMPLE` 和 `CONSTRAINT`。

## 辅助 HTTP API

开发环境可从项目根目录启动：

```bash
uv run uvicorn api.main:app --host 127.0.0.1 --port 8000
```

启动后可访问 `http://localhost:8000/docs` 查看 OpenAPI 页面。

> 该 API 没有鉴权，并且能够写入 Python 文件和执行测试代码，只应在可信的本地开发环境中使用。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/write-operator-code` | 写入 `workspace/ops/<type>/<name>.py` |
| `POST` | `/write-operator-test-code` | 写入 `workspace/tests/<type>/test_<name>.py` |
| `POST` | `/write-pipeline` | 将流水线 YAML 写入用户的 WebHDFS 工作区 |
| `POST` | `/run-pytest` | 运行一个已存在的 `test_*.py` 文件并返回退出码和输出 |
| `POST` | `/run-doc-checker` | 运行算子文档生成器 |
| `POST` | `/sync-workspace` | 构建 wheel 并上传到用户的 WebHDFS 工作区 |
| `POST` | `/sync-conda-env` | 打包并上传 Conda 环境到用户的 WebHDFS 工作区 |
| 函数 | `sync_runtime_resources` | 上传镜像构建时生成的 Data Refiner 运行资源包 |

写入接口的请求体：

```json
{
  "operator_type": "filter",
  "operator_name": "length_filter",
  "content": "..."
}
```

测试接口的请求体：

```json
{
  "path": "workspace/tests/filter/test_length_filter.py"
}
```

流水线接口的请求体：

```json
{
  "pipeline_config_yaml_string": "node:\n  op_name: sample\n"
}
```

`/sync-workspace` 和 `/sync-conda-env` 不需要请求体。以上接口都从容器环境变量
`USER_NAME` 读取当前用户名称。

同步功能当前固定使用：

```text
NameNode: http://master:9870
HDFS user: jupyter
Target: /workspaces/<USER_NAME>/data_refiner_user_workspace-0.0.1-py3-none-any.whl
```

## 当前限制

- `workspace.ops.ops_market.OPS_MAPPING` 当前是算子类列表，已验证可用于发现和文档生成；要求 `{op_name: class}` 映射的宿主集成需要先做转换。
- `workspace/tests/conftest.py` 依赖外部 `data_refiner_api` 包，且 `workspace/tests/` 不在默认 pytest 范围内。
- `api/test_main.py` 与当前 API 模型不同步，因此也不在默认测试范围内。
- `/write-operator-test-code` 当前实现中存在错误的 `mkdir` 关键字参数，修复前不可用。
- API 的路径参数尚未完整限制在 `workspace/` 内，不应暴露给不可信调用方。
- `/sync-workspace` 使用固定版本文件名和固定 WebHDFS 地址，适用于当前部署环境，不是通用发布命令。
- 不要直接运行 `python webhdfs_client.py`；其当前脚本入口会递归删除 HDFS 的 `/workspaces` 目录。
