"""Read logical data sizes and YARN cluster resources."""

import json
import os
import re
from urllib.request import urlopen

from pyspark import SparkContext
from pyspark.sql import SparkSession

DEFAULT_YARN_RESOURCE_MANAGER_URL = "http://master:8088"
YARN_CLUSTER_METRICS_PATH = "/ws/v1/cluster/metrics"


class ResourceInspectionError(RuntimeError):
    """Raised when resource information cannot be read."""


def _hdfs_size_bytes(spark: SparkSession, hdfs_path: str) -> int:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    file_system = path.getFileSystem(hadoop_conf)
    if not file_system.exists(path):
        raise ResourceInspectionError(f"HDFS path does not exist: {hdfs_path}")
    return int(file_system.getContentSummary(path).getLength())


def _quoted_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if any(not part for part in parts):
        raise ResourceInspectionError(f"Invalid Hive table name: {table_name}")
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def _hive_statistics_size(spark: SparkSession, table_name: str) -> int | None:
    rows = spark.sql(
        f"DESCRIBE FORMATTED {_quoted_table_name(table_name)}"
    ).collect()
    statistics_size: int | None = None
    for row in rows:
        values = [str(value).strip() for value in row if value is not None]
        if not values:
            continue
        label = values[0].lower()
        joined = " ".join(values)
        if label == "totalsize":
            match = re.search(r"\b(\d+)\b", joined)
            if match:
                return int(match.group(1))
        if label == "statistics":
            match = re.search(r"\b(\d+)\s+bytes\b", joined, re.IGNORECASE)
            if match:
                statistics_size = int(match.group(1))
    return statistics_size


def _hive_input_files_size(spark: SparkSession, table_name: str) -> int:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    total_size = 0
    for input_file in set(spark.table(table_name).inputFiles()):
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(input_file)
        file_system = path.getFileSystem(hadoop_conf)
        if file_system.exists(path):
            total_size += int(file_system.getFileStatus(path).getLen())
    return total_size


def _hive_size_bytes(spark: SparkSession, table_name: str) -> int:
    statistics_size = _hive_statistics_size(spark, table_name)
    if statistics_size is not None:
        return statistics_size
    return _hive_input_files_size(spark, table_name)


def _create_spark(app_name: str) -> tuple[SparkSession, bool]:
    owns_context = SparkContext._active_spark_context is None
    spark = (
        SparkSession.builder.master("yarn")
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark, owns_context


def get_hdfs_size(hdfs_path: str) -> int:
    """Return the logical size of an HDFS file or directory."""
    spark, owns_context = _create_spark("ReadHdfsDataSize")
    try:
        return _hdfs_size_bytes(spark, hdfs_path)
    finally:
        if owns_context:
            spark.stop()


def get_hive_size(table_name: str) -> int:
    """Return the logical size of a Hive table."""
    spark, owns_context = _create_spark("ReadHiveDataSize")
    try:
        return _hive_size_bytes(spark, table_name)
    finally:
        if owns_context:
            spark.stop()


def get_cluster_resources() -> dict[str, int]:
    """Return memory and virtual-core metrics reported by YARN."""
    resource_manager_url = os.getenv(
        "YARN_RESOURCE_MANAGER_URL",
        DEFAULT_YARN_RESOURCE_MANAGER_URL,
    )
    with urlopen(
        f"{resource_manager_url.rstrip('/')}{YARN_CLUSTER_METRICS_PATH}",
        timeout=5,
    ) as response:
        payload = json.load(response)

    try:
        metrics = payload["clusterMetrics"]
        return {
            "total_memory_mb": int(metrics["totalMB"]),
            "available_memory_mb": int(metrics["availableMB"]),
            "allocated_memory_mb": int(metrics["allocatedMB"]),
            "total_vcores": int(metrics["totalVirtualCores"]),
            "available_vcores": int(metrics["availableVirtualCores"]),
            "allocated_vcores": int(metrics["allocatedVirtualCores"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ResourceInspectionError(
            "YARN cluster metrics response is invalid"
        ) from exc
