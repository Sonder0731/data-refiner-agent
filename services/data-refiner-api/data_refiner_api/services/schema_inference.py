"""Spark-backed file and table schema inference."""

from pathlib import PurePosixPath
from urllib.parse import urlsplit

from pyspark.sql import SparkSession


def _print_schema(schema: list[tuple[str, str]]) -> str:
    return "".join(f"- name={name}, type={data_type}\n" for name, data_type in schema)


def _first_file_name(spark: SparkSession, file_path: str) -> tuple[bool, str | None]:
    """Return whether the Hadoop path exists and the first file name below it."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(file_path)
    file_system = hadoop_path.getFileSystem(hadoop_conf)

    if not file_system.exists(hadoop_path):
        return False, None

    status = file_system.getFileStatus(hadoop_path)
    if status.isFile():
        return True, status.getPath().getName()

    files = file_system.listFiles(hadoop_path, True)
    while files.hasNext():
        name = files.next().getPath().getName()
        # Hadoop output directories commonly contain markers such as _SUCCESS.
        # They are not data files and must not determine the reader format.
        if not name.startswith(("_", ".")):
            return True, name
    return True, None


def _file_suffix(file_name: str) -> str:
    # urlsplit handles a complete hdfs:// URI without treating its authority as
    # part of the file name.
    return PurePosixPath(urlsplit(file_name).path).suffix.lower()


def infer_file_schema(file_path: str) -> str:
    """Read a file or directory with Spark and return its schema and one row."""
    spark = SparkSession.builder.appName("InferSchema").getOrCreate()
    try:
        exists, first_file_name = _first_file_name(spark, file_path)
        if not exists:
            return f"File not found: {file_path}"
        if first_file_name is None:
            return f"No file found in directory: {file_path}"

        suffix = _file_suffix(first_file_name)
        text = f"File extension: {suffix}\n"

        if suffix == ".csv":
            df = spark.read.csv(file_path, header=True, inferSchema=True)
        elif suffix in {".json", ".jsonl"}:
            df = spark.read.json(file_path)
        elif suffix == ".parquet":
            df = spark.read.parquet(file_path)
        elif suffix == ".orc":
            df = spark.read.orc(file_path)
        elif suffix in {".txt", ".html"}:
            df = spark.read.text(file_path)
        elif suffix == ".avro":
            df = spark.read.format("avro").load(file_path)
        else:
            return text + f"Unsupported file extension: {suffix or '<none>'}"

        schema = [
            (field.name, field.dataType.simpleString()) for field in df.schema.fields
        ]
        text += "Data schema:\n" + _print_schema(schema)

        show_string = df._show_string(n=5, truncate=True)
        text += "Data example:\n" + show_string
        return text
    finally:
        spark.stop()


def infer_table_schema(table_name: str) -> str:
    """Read a Hive-compatible table and return its schema and bounded examples."""
    spark = (
        SparkSession.builder.appName("ReadTableSchema")
        .enableHiveSupport()
        .getOrCreate()
    )
    try:
        df = spark.table(table_name)
        text = f"Table: {table_name}\nSchema:\n"
        for field in df.schema.fields:
            text += (
                f"- name={field.name}, "
                f"type={field.dataType.simpleString()}, "
                f"nullable={field.nullable}\n"
            )
        text += "Data example:\n" + df._show_string(n=5, truncate=True)
        return text
    finally:
        spark.stop()
