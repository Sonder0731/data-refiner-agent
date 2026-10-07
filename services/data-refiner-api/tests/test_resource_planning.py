import io
import json

import pytest

from data_refiner_api.services import resource_planning
from data_refiner_api.services.resource_planning import ResourceInspectionError


def test_hive_statistics_size_prefers_total_size() -> None:
    class Result:
        def collect(self):
            return [
                ("Statistics", "100 rows, 2000 bytes", None),
                ("totalSize", "4096", None),
            ]

    class Spark:
        def sql(self, query: str) -> Result:
            assert query == "DESCRIBE FORMATTED `analytics`.`events`"
            return Result()

    assert resource_planning._hive_statistics_size(
        Spark(), "analytics.events"
    ) == 4096


def test_hive_size_falls_back_to_input_files(monkeypatch) -> None:
    monkeypatch.setattr(
        resource_planning,
        "_hive_statistics_size",
        lambda *_: None,
    )
    monkeypatch.setattr(
        resource_planning,
        "_hive_input_files_size",
        lambda *_: 8192,
    )

    assert resource_planning._hive_size_bytes(object(), "events") == 8192


def test_hive_table_name_rejects_empty_parts() -> None:
    with pytest.raises(ResourceInspectionError, match="Invalid Hive table"):
        resource_planning._quoted_table_name("analytics..events")


def test_get_hdfs_size_stops_owned_spark(monkeypatch) -> None:
    class Spark:
        stopped = False

        def stop(self) -> None:
            self.stopped = True

    spark = Spark()
    monkeypatch.setattr(
        resource_planning,
        "_create_spark",
        lambda _: (spark, True),
    )
    monkeypatch.setattr(
        resource_planning,
        "_hdfs_size_bytes",
        lambda _, path: 1024 if path == "hdfs:///data" else 0,
    )

    assert resource_planning.get_hdfs_size("hdfs:///data") == 1024
    assert spark.stopped


def test_get_cluster_resources(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []
    payload = {
        "clusterMetrics": {
            "totalMB": 131072,
            "availableMB": 65536,
            "allocatedMB": 65536,
            "totalVirtualCores": 64,
            "availableVirtualCores": 32,
            "allocatedVirtualCores": 32,
        }
    }

    def fake_urlopen(url: str, timeout: int) -> io.BytesIO:
        calls.append((url, timeout))
        return io.BytesIO(json.dumps(payload).encode())

    monkeypatch.setenv("YARN_RESOURCE_MANAGER_URL", "http://yarn:8088/")
    monkeypatch.setattr(resource_planning, "urlopen", fake_urlopen)

    assert resource_planning.get_cluster_resources() == {
        "total_memory_mb": 131072,
        "available_memory_mb": 65536,
        "allocated_memory_mb": 65536,
        "total_vcores": 64,
        "available_vcores": 32,
        "allocated_vcores": 32,
    }
    assert calls == [("http://yarn:8088/ws/v1/cluster/metrics", 5)]
