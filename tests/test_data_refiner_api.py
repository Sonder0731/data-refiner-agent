import unittest
from unittest.mock import patch

import httpx
import pytest

from data_refiner_agent_tools import (
    add_package,
    get_cluster_resources,
    get_operator_docs,
    request_schema,
    run_pipeline,
    sync_conda_env,
    sync_workspace,
)
from data_refiner_agent_tools._http import request_json
from data_refiner_agent_tools.data_refiner_api import _format_size


def test_http_error_includes_service_detail() -> None:
    response = httpx.Response(
        400,
        json={"detail": "Spark configuration cannot be overridden"},
        request=httpx.Request("POST", "http://api/pipelines/run"),
    )
    with (
        patch("data_refiner_agent_tools._http.httpx.request", return_value=response),
        pytest.raises(httpx.HTTPStatusError, match="cannot be overridden"),
    ):
        request_json("http://api/pipelines/run", method="POST", payload={})


@pytest.mark.parametrize(
    ("size_bytes", "expected"),
    [
        (1023, "1023 B"),
        (1024, "1 KB"),
        (1024**2, "1 MB"),
        (1024**3, "1 GB"),
        (1024**4, "1 TB"),
        (2733, "2.67 KB"),
    ],
)
def test_format_size_uses_largest_unit_at_least_one(
    size_bytes: int, expected: str
) -> None:
    assert _format_size(size_bytes) == expected


class DataRefinerApiTest(unittest.TestCase):
    @patch("data_refiner_agent_tools.data_refiner_api.read_hdfs_size")
    @patch("data_refiner_agent_tools.data_refiner_api.read_file_schema")
    def test_request_schema_formats_data_size(self, read_schema, read_size) -> None:
        read_schema.return_value = {"result": "schema"}
        read_size.return_value = {"size_bytes": 1024**3}

        result = request_schema("file", "hdfs:///data", base_url="http://api")

        self.assertEqual(result, "schema\nData size:\n1 GB")

    @patch("data_refiner_agent_tools.data_refiner_api.post_json")
    def test_operator_lookup_uses_name_only_endpoint(self, post_json) -> None:
        post_json.return_value = {"result": "operator docs"}

        result = get_operator_docs(
            "@admin:example.test",
            "length_filter",
            base_url="http://api",
        )

        self.assertEqual(result, {"result": "operator docs"})
        post_json.assert_called_once_with(
            "http://api/operators/processing_operator/length_filter/docs",
            {"user_id": "@admin:example.test"},
        )

    @patch("data_refiner_agent_tools.data_refiner_api.post_json")
    def test_add_package_sends_name_and_version_separately(self, post_json) -> None:
        post_json.return_value = {"status": "succeeded", "exit_code": 0}

        result = add_package(
            "@admin:example.test",
            "pypinyin",
            "0.55.0",
            base_url="http://api",
        )

        self.assertEqual(result["status"], "succeeded")
        post_json.assert_called_once_with(
            "http://api/workspace/add-package",
            {
                "user_id": "@admin:example.test",
                "package_name": "pypinyin",
                "version": "0.55.0",
            },
            timeout=620,
        )

    @patch("data_refiner_agent_tools.data_refiner_api.get_json")
    def test_get_cluster_resources_uses_cluster_endpoint(self, get_json) -> None:
        get_json.return_value = {"total_memory_mb": 16384}

        result = get_cluster_resources(base_url="http://api")

        self.assertEqual(result, {"total_memory_mb": 16384})
        get_json.assert_called_once_with("http://api/resources/cluster")

    @patch("data_refiner_agent_tools.data_refiner_api.post_json")
    def test_run_pipeline_sends_spark_runtime_config(self, post_json) -> None:
        post_json.return_value = {"application_id": "application_1_1"}
        config = ["--num-executors 4", "--executor-memory 2g"]

        result = run_pipeline(
            "@admin:example.test",
            "pipeline.yaml",
            config,
            base_url="http://api",
        )

        self.assertEqual(result, {"application_id": "application_1_1"})
        post_json.assert_called_once_with(
            "http://api/pipelines/run",
            {
                "user_id": "@admin:example.test",
                "pipeline_name": "pipeline.yaml",
                "spark_runtime_config": config,
            },
        )

    @patch("data_refiner_agent_tools.data_refiner_api.post_json")
    def test_workspace_sync_endpoints(self, post_json) -> None:
        post_json.return_value = {"result": "success"}

        sync_workspace("@admin:example.test", base_url="http://api")
        sync_conda_env("@admin:example.test", base_url="http://api")

        self.assertEqual(
            post_json.call_args_list,
            [
                unittest.mock.call(
                    "http://api/workspace/sync-workspace",
                    {"user_id": "@admin:example.test"},
                ),
                unittest.mock.call(
                    "http://api/workspace/sync-conda-env",
                    {"user_id": "@admin:example.test"},
                    timeout=2000,
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
