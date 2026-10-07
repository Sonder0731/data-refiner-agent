import sync_base_dataset


def test_sync_base_datasets_only_inserts_missing_records(monkeypatch) -> None:
    checked: list[tuple[str, str]] = []
    created: list[dict] = []

    class Cursor:
        current_identifier = ""

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def execute(self, statement, parameters) -> None:
            self.current_identifier = parameters[1]
            checked.append(parameters)

        def fetchone(self):
            if self.current_identifier == "hdfs:///data/cc":
                return (1,)
            return None

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        sync_base_dataset.psycopg,
        "connect",
        lambda dsn, connect_timeout: Connection(),
    )
    monkeypatch.setattr(
        sync_base_dataset.base_dataset_service,
        "create_base_dataset",
        lambda **dataset: created.append(dataset),
    )

    assert sync_base_dataset.sync_base_datasets() == 4
    assert len(checked) == 5
    assert len(created) == 4
    assert all(item["source_identifier"] != "hdfs:///data/cc" for item in created)


def test_seed_describes_all_inspected_hdfs_paths() -> None:
    assert {
        dataset["source_identifier"]
        for dataset in sync_base_dataset.BASE_DATASETS
    } == {
        "hdfs:///data/cc",
        "hdfs:///data/btc_ohlcv_dataset",
        "hdfs:///data/chinese_news",
        "hdfs:///data/douban_movies",
        "hdfs:///data/job_postings",
    }
