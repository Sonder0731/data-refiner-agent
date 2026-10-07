"""Insert known HDFS datasets into the base dataset catalog."""

import os
from typing import Any

import psycopg

from db import DEFAULT_DATABASE_DSN
from data_refiner_api.services import base_datasets as base_dataset_service


BASE_DATASETS: list[dict[str, Any]] = [
    {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/cc",
        "data_name": "Common Crawl 英文网页语料",
        "data_description": (
            "Parquet 格式的 Common Crawl CC-MAIN-2018-30 英文网页正文语料。"
            "每行记录网页抽取文本、WARC 文档标识、来源 URL、抓取时间、"
            "原始 WARC 文件位置、语言识别结果和词元数量，适用于网页文本"
            "清洗、语言分析和自然语言处理。目录名 cc 表示 Common Crawl，"
            "不是信用卡数据。"
        ),
        "field_descriptions": {
            "text": "从网页中抽取的正文文本。",
            "id": "WARC 记录标识，格式通常为 urn:uuid。",
            "dump": "Common Crawl 抓取批次标识，例如 CC-MAIN-2018-30。",
            "url": "被抓取网页的原始 URL。",
            "date": "网页抓取时间，ISO 8601 UTC 时间字符串。",
            "file_path": "原始 Common Crawl WARC 文件的 S3 路径。",
            "language": "自动识别的网页文本语言代码。",
            "language_score": "语言识别置信度，Parquet double 类型。",
            "token_count": "正文的词元数量，Parquet long 类型。",
        },
        "examples": [
            {
                "dump": "CC-MAIN-2018-30",
                "language": "en",
                "language_score": 0.8801718354225159,
                "token_count": 1111,
                "url": "http://10-health.com/2017/11/01/winter-skincare-strategies/",
            }
        ],
    },
    {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/btc_ohlcv_dataset",
        "data_name": "BTC/USD 分钟级 OHLCV 行情",
        "data_description": (
            "CSV 格式的 BTC/USD 一分钟粒度历史行情。每行是一分钟 K 线，"
            "包含 Unix 时间戳、开盘价、最高价、最低价、收盘价和成交量。"
            "CSV 带表头，原始字段由 Spark 默认读取时均为 string，需要按"
            "业务类型将时间戳转换为时间、价格和成交量转换为数值。"
        ),
        "field_descriptions": {
            "Timestamp": "该分钟 K 线的 Unix 时间戳，单位为秒。",
            "Open": "该分钟的开盘价格。",
            "High": "该分钟的最高价格。",
            "Low": "该分钟的最低价格。",
            "Close": "该分钟的收盘价格。",
            "Volume": "该分钟记录的成交量，具体单位取决于数据来源。",
        },
        "examples": [
            {
                "Timestamp": "1325376060",
                "Open": "4.58",
                "High": "4.58",
                "Low": "4.58",
                "Close": "4.58",
                "Volume": "0.0",
            }
        ],
    },
    {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/chinese_news",
        "data_name": "中文新闻语料",
        "data_description": (
            "CSV 格式的中文新闻文章数据，包含发布日期、标签、标题和新闻"
            "正文。content 字段可能包含换行符并使用双引号包裹，Spark 读取"
            "时需要设置 header=true 和 multiLine=true，避免正文的后续行被"
            "错误解析成新记录。"
        ),
        "field_descriptions": {
            "date": "新闻发布日期，格式为 YYYY-MM-DD。",
            "tag": "新闻的栏目、分类或内容标签。",
            "headline": "新闻标题。",
            "content": "新闻正文，可能包含逗号、双引号和多行文本。",
        },
        "examples": [
            {
                "date": "2016-01-01",
                "tag": "详细全文",
                "headline": (
                    "陆军领导机构火箭军战略支援部队成立大会在京举行"
                ),
                "content": "中国人民解放军陆军领导机构成立大会在京举行。",
            }
        ],
    },
    {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/douban_movies",
        "data_name": "豆瓣电影短评数据",
        "data_description": (
            "CSV 格式的豆瓣电影短评数据。每行是一条电影用户短评，包含电影"
            "中英文名称、采集日期、用户、评论日期、星级、评论正文和点赞数。"
            "文件表头带 UTF-8 BOM，评论文本可能包含中文标点和逗号。"
        ),
        "field_descriptions": {
            "ID": "CSV 中的全局行索引。",
            "Movie_Name_EN": "电影英文名称。",
            "Movie_Name_CN": "电影中文名称。",
            "Crawl_Date": "该评论被采集的日期。",
            "Number": "评论在对应电影采集结果中的顺序编号。",
            "Username": "发表评论的豆瓣用户名，可能属于个人信息。",
            "Date": "评论发布日期。",
            "Star": "用户给出的电影星级评分。",
            "Comment": "用户发表的电影短评正文。",
            "Like": "该短评获得的有用或点赞数量。",
        },
        "examples": [
            {
                "Movie_Name_EN": "Avengers Age of Ultron",
                "Movie_Name_CN": "复仇者联盟2",
                "Crawl_Date": "2017-01-22",
                "Date": "2015-05-13",
                "Star": "3",
                "Comment": "连奥创都知道整容要去韩国。",
                "Like": "2404",
            }
        ],
    },
    {
        "source_type": "hdfs",
        "source_identifier": "hdfs:///data/job_postings",
        "data_name": "在线职位发布数据",
        "data_description": (
            "CSV 格式的在线职位发布数据。每行是一条招聘职位，包含公司、"
            "职位名称和描述、工作地点、薪资范围和周期、工作类型、经验级别、"
            "远程标记、申请方式、发布时间、到期时间以及浏览和申请数量。"
            "description 和 skills_desc 是可能包含长文本的字段，读取时应正确"
            "处理被双引号包裹的内容。"
        ),
        "field_descriptions": {
            "job_id": "职位发布记录的唯一标识。",
            "company_name": "招聘公司名称。",
            "title": "职位名称。",
            "description": "完整职位描述，可能包含职责和任职要求等长文本。",
            "max_salary": "职位薪资范围的最大值。",
            "pay_period": "薪资周期，例如 HOURLY 或 YEARLY。",
            "location": "职位所在地的展示文本。",
            "company_id": "招聘公司的唯一标识。",
            "views": "职位页面的浏览次数。",
            "med_salary": "职位薪资范围的中位值。",
            "min_salary": "职位薪资范围的最小值。",
            "formatted_work_type": "格式化后的工作类型，例如 Full-time。",
            "applies": "通过平台产生的申请数量。",
            "original_listed_time": "职位最初发布的时间戳。",
            "remote_allowed": "是否允许远程工作，1 表示允许。",
            "job_posting_url": "职位发布页面 URL。",
            "application_url": "外部职位申请页面 URL。",
            "application_type": "职位申请方式或渠道类型。",
            "expiry": "职位发布的到期时间戳。",
            "closed_time": "职位关闭时间戳，未关闭时为空。",
            "formatted_experience_level": "格式化后的职位经验级别。",
            "skills_desc": "职位要求的技能说明文本。",
            "listed_time": "当前职位记录的发布时间戳。",
            "posting_domain": "职位发布或申请网站的域名。",
            "sponsored": "是否为推广职位，1 表示推广。",
            "work_type": "工作类型的原始代码。",
            "currency": "薪资使用的货币代码，例如 USD。",
            "compensation_type": "薪资报酬类型。",
            "normalized_salary": "标准化后用于比较的薪资数值。",
            "zip_code": "职位所在地邮政编码。",
            "fips": "职位所在地对应的 FIPS 地理区域代码。",
        },
        "examples": [
            {
                "job_id": "921716",
                "company_name": "Corcoran Sawyer Smith",
                "title": "Marketing Coordinator",
                "max_salary": "20.0",
                "pay_period": "HOURLY",
                "location": "Princeton, NJ",
                "formatted_work_type": "Full-time",
                "currency": "USD",
                "normalized_salary": "38480.0",
            }
        ],
    },
]


def sync_base_datasets() -> int:
    """Insert configured datasets that are not already in the catalog."""
    missing: list[dict[str, Any]] = []
    dsn = os.getenv("DATABASE_URL", DEFAULT_DATABASE_DSN)
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            for dataset in BASE_DATASETS:
                cursor.execute(
                    """
                    SELECT 1
                    FROM base_dataset
                    WHERE source_type = %s AND source_identifier = %s
                    """,
                    (dataset["source_type"], dataset["source_identifier"]),
                )
                if cursor.fetchone() is None:
                    missing.append(dataset)

    inserted = 0
    for dataset in missing:
        try:
            base_dataset_service.create_base_dataset(**dataset)
        except base_dataset_service.BaseDatasetAlreadyExistsError:
            continue
        inserted += 1
    return inserted


def main() -> None:
    inserted = sync_base_datasets()
    print(f"Synchronized {inserted} base dataset(s).")


if __name__ == "__main__":
    main()
