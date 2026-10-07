"""Insert configured pipeline cases into PostgreSQL."""

import os

import psycopg

from db import DEFAULT_DATABASE_DSN


PIPELINE_CASES: list[dict[str, str | None]] = [
    # {
    #     "original_user_query": "用户提出的原始需求",
    #     "belong": "data-refiner",  # data-refiner or user
    #     "user_id": None,  # belong=user 时填写对应用户 ID
    #     "input_data_desc": "输入数据描述",
    #     "pipeline": "pipeline 字符串",
    #     "task_summary": "任务摘要",
    #     "processing_steps": "处理步骤",
    #     "output_data_description": "输出数据描述",
    #     "spark_runtime_config": "Spark runtime 配置",
    # },
]

# PIPELINE_CASES: list[dict[str, str | None]] = [
#     {
#         "original_user_query": "将hdfs:///data/chinese_news下的数据的text字段进行字符标准化",
#         "belong": "data-refiner",  # data-refiner or user
#         "user_id": None,  # belong=user 时填写对应用户 ID
#         "input_data_desc": """""",
#         "task_summary": "将字符串中的字符进行标准化",
#         "processing_steps": "1. 使用regular_path_reader算子进行数据读写 2. 使用character_normalization_mapper算子对字符串进行标准化",
#         "output_data_description": "输出数据描述",
#         "spark_runtime_config": "Spark runtime 配置",
#     },
# ]

INSERT_SQL = """
    INSERT INTO pipeline_case (
        original_user_query,
        belong,
        user_id,
        input_data_desc,
        pipeline,
        task_summary,
        processing_steps,
        output_data_description,
        spark_runtime_config
    )
    SELECT
        %(original_user_query)s,
        %(belong)s,
        %(user_id)s,
        %(input_data_desc)s,
        %(pipeline)s,
        %(task_summary)s,
        %(processing_steps)s,
        %(output_data_description)s,
        %(spark_runtime_config)s
    WHERE NOT EXISTS (
        SELECT 1
        FROM pipeline_case
        WHERE original_user_query = %(original_user_query)s
          AND belong = %(belong)s
          AND user_id IS NOT DISTINCT FROM %(user_id)s
          AND input_data_desc = %(input_data_desc)s
          AND pipeline = %(pipeline)s
          AND task_summary = %(task_summary)s
          AND processing_steps = %(processing_steps)s
          AND output_data_description = %(output_data_description)s
          AND spark_runtime_config = %(spark_runtime_config)s
    )
"""


def sync_pipeline_cases() -> None:
    if not PIPELINE_CASES:
        return

    # ponytail: for a small, single-process seed list; add a unique case key
    # before syncing concurrently from multiple API workers.
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(INSERT_SQL, PIPELINE_CASES)


def main() -> None:
    sync_pipeline_cases()
    print(f"Synchronized {len(PIPELINE_CASES)} configured pipeline case(s).")


if __name__ == "__main__":
    main()
