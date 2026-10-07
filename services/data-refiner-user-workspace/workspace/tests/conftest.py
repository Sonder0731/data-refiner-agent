import os
import sys

import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession

from path_set import LocalPath

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


@pytest.fixture(scope="session")
def spark(request):
    """
    创建一个 SparkSession，整个 pytest 会话共享
    """
    graph_frames_path = LocalPath.workspace_jar_root().joinpath(
        "graphframes-0.8.4-spark3.5-s_2.12.jar"
    )

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    conf = (
        SparkConf()
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.python.worker.memory", "1g")
        .set("spark.driver.memory", "1g")
        .set("spark.default.parallelism", "1")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.ui.enabled", "false")
        .set("spark.jars", str(graph_frames_path.as_uri()))
    )
    spark = (
        SparkSession.builder.master("local[1]")
        .config(conf=conf)
        .appName("pytest-spark-session")
        .enableHiveSupport()
        .getOrCreate()
    )

    # create a test database for testing table something
    test_db = "data_refiner_test_db_by_sonder0731"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    spark.sql(f"USE {test_db}")

    spark.sql("CREATE TABLE IF NOT EXISTS temp (id INT, name STRING)")
    spark.sql("INSERT INTO temp VALUES (1, 'Alice'), (2, 'Bob')")

    sc = spark.sparkContext
    sc.setCheckpointDir(str(LocalPath.workspace_checkpoint_root()))
    yield spark
    spark.sql(f"DROP DATABASE IF EXISTS {test_db} CASCADE")
    spark.stop()
