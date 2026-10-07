from types import SimpleNamespace

from data_refiner_api.services import operator_catalog, pipeline_operations


def test_builtin_operator_document_can_be_read() -> None:
    result = operator_catalog.get_processing_operator_docs(
        "deduplicator",
        "exact_deduplicator",
    )

    assert "exact" in result.lower()


def test_builtin_operator_document_can_be_read_by_name() -> None:
    result = operator_catalog.get_processing_operator_docs_by_name(
        "exact_deduplicator"
    )

    assert "exact" in result.lower()


def test_missing_operator_document_returns_expected_message() -> None:
    result = operator_catalog.get_processing_operator_docs(
        "mapper",
        "does_not_exist",
    )

    assert result == operator_catalog.DOCUMENT_NOT_FOUND


def test_operator_market_contains_main_market() -> None:
    result = operator_catalog.get_ops_market()

    assert "operator market" in result.lower()
    assert "Sub Operator Market" not in result


def test_pipeline_example_can_be_read() -> None:
    result = pipeline_operations.get_pipeline_example()

    assert "language_identification_mapper" in result


def test_validate_workspace_pipeline_posts_yaml(monkeypatch) -> None:
    calls = []

    def fake_post_url(api_url, path, payload):
        calls.append((api_url, path, payload))
        return {"success": True, "message": "Pipeline configuration is valid."}

    monkeypatch.setattr(
        pipeline_operations.workspace_proxy,
        "post_url",
        fake_post_url,
    )

    result = pipeline_operations.validate_workspace_pipeline(
        "http://workspace/",
        "node: {}",
    )

    assert result.success is True
    assert calls == [
        (
            "http://workspace/",
            "/pipeline-validation",
            {"pipeline_config_yaml_string": "node: {}"},
        )
    ]


def test_submit_command_uses_user_workspace_paths() -> None:
    command = pipeline_operations.submit_command("admin", "xxx.yaml")

    archives = command[command.index("--archives") + 1]
    assert "data_refiner_env.tar.gz#PYTHON_ENV" in archives
    assert "data-refiner-runtime-resources.zip#data-refiner-runtime-resources" in archives
    assert "hdfs:///workspaces/admin/pipelines/xxx.yaml" in command
    assert command[-2:] == ["--pipeline_name", "xxx.yaml"]


def test_submit_command_appends_extra_files() -> None:
    extra_file = "hdfs:///workspaces/admin/resources/custom.txt"

    command = pipeline_operations.submit_command(
        "admin",
        "xxx.yaml",
        [f"--files {extra_file}"],
    )

    files = command[command.index("--files") + 1]
    assert files == (
        "hdfs:///workspaces/admin/pipelines/xxx.yaml,"
        "hdfs:///workspaces/admin/resources/custom.txt"
    )


def test_submit_command_rejects_extra_files_outside_workspace() -> None:
    try:
        pipeline_operations.submit_command(
            "admin",
            "xxx.yaml",
            ["--files hdfs:///workspaces/other/resources/custom.txt"],
        )
    except ValueError as exc:
        assert "user's workspace" in str(exc)
    else:
        raise AssertionError("files outside the user's workspace must be rejected")


def test_submit_command_injects_litellm_api_key(monkeypatch) -> None:
    monkeypatch.setenv("LITELLM_API_KEY", "secret")

    command = pipeline_operations.submit_command("admin", "xxx.yaml")

    assert "spark.yarn.appMasterEnv.LITELLM_API_KEY=secret" in command
    assert "spark.executorEnv.LITELLM_API_KEY=secret" in command


def test_submit_command_rejects_pipeline_paths() -> None:
    try:
        pipeline_operations.submit_command("admin", "../xxx.yaml")
    except ValueError as exc:
        assert "pipeline_name" in str(exc)
    else:
        raise AssertionError("pipeline paths must be rejected")


def test_run_pipeline_normalizes_user_id_and_returns_application_id(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], bool, bool, bool]] = []

    def fake_run(command, *, check, capture_output, text):
        calls.append((command, check, capture_output, text))
        return SimpleNamespace(
            stdout="Submitted application application_1740000000000_0042",
            stderr="",
        )

    monkeypatch.setattr(pipeline_operations.subprocess, "run", fake_run)

    result = pipeline_operations.run_pipeline(
        "@admin:matrix-local.agentteams.io",
        "xxx.yaml",
    )

    assert result == "application_1740000000000_0042"
    assert calls == [
        (
            [
                str(argument)
                for argument in pipeline_operations.submit_command(
                    "admin", "xxx.yaml"
                )
            ],
            True,
            True,
            True,
        )
    ]
