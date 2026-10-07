from api import main


def test_write_pipeline_to_user_hdfs(monkeypatch) -> None:
    writes = []
    monkeypatch.setenv("USER_NAME", "alice")

    class Storage:
        def __init__(self, **kwargs):
            assert kwargs["root_dir"] == "/workspaces/alice"

        def write_text(self, hdfs_path, content):
            writes.append((hdfs_path, content))

    monkeypatch.setattr(main, "WebHDFSStorage", Storage)

    result = main.write_pipeline(
        main.PipelineRequest(
            pipeline_config_yaml_string="node:\n  op_name: sample\n",
        )
    )

    assert result["hdfs_path"].startswith(
        "/workspaces/alice/pipelines/"
    )
    assert result["hdfs_path"].endswith(".yaml")
    assert writes == [(result["hdfs_path"], "node:\n  op_name: sample\n")]


def test_pipeline_validation_rejects_unsupported_operator_parameter(monkeypatch) -> None:
    monkeypatch.setattr(main, "_workspace_ops_mapping", lambda: {})

    result = main.validate_pipeline(
        "sql:\n"
        "  op_name: spark_sql_executor\n"
        "  input_df: source\n"
        "  output_df: result\n"
        "  sql_query: SELECT 1\n"
    )

    assert result["success"] is False
    assert "input_df" in result["error"]


def test_pipeline_validation_checks_inherited_required_parameters(monkeypatch) -> None:
    monkeypatch.setattr(main, "_workspace_ops_mapping", lambda: {})

    result = main.validate_pipeline(
        "writer:\n"
        "  op_name: path_writer\n"
        "  path: hdfs:///tmp/output\n"
        "  format: parquet\n"
        "  mode: overwrite\n"
    )

    assert result["success"] is False
    assert "input_df" in result["error"]


def test_pipeline_validation_applies_operator_specific_rules(monkeypatch) -> None:
    monkeypatch.setattr(main, "_workspace_ops_mapping", lambda: {})

    result = main.validate_pipeline(
        "mask:\n"
        "  op_name: mask_pii_mapper\n"
        "  input_df: source\n"
        "  output_df: masked\n"
        "  field: text\n"
        "  output_field: masked_text\n"
        "  mask_map:\n"
        "    PHONE_NUMBER:\n"
        "      operator_name: replace\n"
        "      params:\n"
        "        new_value: <PHONE>\n"
    )

    assert result["success"] is False
    assert "type" in result["error"]
