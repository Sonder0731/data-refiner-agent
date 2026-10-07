import json
import sys
from io import StringIO
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from data_refiner_agent import cli
from data_refiner_agent.cli import ConsoleProgressHandler
from data_refiner_agent.workflow_types import WorkflowStage


def test_progress_logs_steps_without_inputs_or_outputs() -> None:
    stream = StringIO()
    handler = ConsoleProgressHandler(stream)
    run_id = uuid4()

    handler.on_tool_start(
        {"name": "read_data"},
        "secret input",
        run_id=run_id,
        metadata={"agent_name": "data-profile"},
        inputs={"data_source": "hdfs:///secret"},
    )
    handler.on_tool_end("secret output", run_id=run_id)
    handler.on_custom_event(
        "progress",
        "Synchronizing",
        run_id=run_id,
        metadata={"agent_name": "workflow"},
    )
    handler.on_custom_event(
        "completed",
        "Synchronization completed",
        run_id=run_id,
        metadata={"agent_name": "workflow"},
    )

    output = stream.getvalue()
    assert "[START] [data-profile] Calling tool read_data" in output
    assert "[DONE ] [data-profile] Tool read_data completed" in output
    assert "[WAIT ] [workflow] Synchronizing" in output
    assert "[DONE ] [workflow] Synchronization completed" in output
    assert "secret" not in output


def test_cli_runs_outer_workflow_with_postgres_checkpoint(
    monkeypatch, capsys, tmp_path
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("DATA_REFINER_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "data-refiner-agent",
            "--quiet",
            "--thread-id",
            "thread-1",
            "preview data",
        ],
    )
    saver = MagicMock()
    saver_context = MagicMock()
    saver_context.__enter__.return_value = saver
    workflow = MagicMock()
    workflow.invoke.return_value = {"final_answer": "done"}
    postgres_factory = MagicMock(return_value=saver_context)
    monkeypatch.setattr(cli.PostgresSaver, "from_conn_string", postgres_factory)
    create_workflow = MagicMock(return_value=workflow)
    monkeypatch.setattr(cli, "create_workflow", create_workflow)

    assert cli.main() == 0

    postgres_factory.assert_called_once_with("postgresql://test")
    saver.setup.assert_called_once_with()
    create_workflow.assert_called_once_with(checkpointer=saver)
    (state,) = workflow.invoke.call_args.args
    config = workflow.invoke.call_args.kwargs["config"]
    assert state == {
        "query": "preview data",
        "user_id": "@local:local",
        "room_id": "local",
    }
    assert config["configurable"] == {"thread_id": "thread-1"}
    assert len(config["callbacks"]) == 1
    events = [
        json.loads(line)
        for line in (tmp_path / "thread-1.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert [event["event"] for event in events] == [
        "run.started",
        "workflow.initialized",
        "run.completed",
    ]
    assert events[-1]["data"]["result"] == {"final_answer": "done"}
    assert capsys.readouterr().out == "done\n"


@pytest.mark.parametrize(
    ("result", "expected_exit_code"),
    [
        ({"stage": WorkflowStage.FAILED, "final_answer": "task failed"}, 1),
        ({"__interrupt__": ({"question": "need input"},)}, 2),
    ],
)
def test_cli_returns_nonzero_for_business_failure_or_interrupt(
    monkeypatch, tmp_path, result, expected_exit_code
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("DATA_REFINER_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        sys,
        "argv",
        ["data-refiner-agent", "--quiet", "--thread-id", "not-complete", "request"],
    )
    saver_context = MagicMock()
    saver_context.__enter__.return_value = MagicMock()
    monkeypatch.setattr(
        cli.PostgresSaver,
        "from_conn_string",
        MagicMock(return_value=saver_context),
    )
    workflow = MagicMock()
    workflow.invoke.return_value = result
    monkeypatch.setattr(cli, "create_workflow", MagicMock(return_value=workflow))

    assert cli.main() == expected_exit_code


def test_cli_records_failure_traceback(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://test")
    monkeypatch.setenv("DATA_REFINER_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        sys,
        "argv",
        ["data-refiner-agent", "--quiet", "--thread-id", "failed", "request"],
    )
    saver_context = MagicMock()
    saver_context.__enter__.return_value = MagicMock()
    monkeypatch.setattr(
        cli.PostgresSaver,
        "from_conn_string",
        MagicMock(return_value=saver_context),
    )
    workflow = MagicMock()
    workflow.invoke.side_effect = RuntimeError("boom")
    monkeypatch.setattr(cli, "create_workflow", MagicMock(return_value=workflow))

    with pytest.raises(SystemExit):
        cli.main()

    events = [
        json.loads(line)
        for line in (tmp_path / "failed.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert events[-1]["event"] == "run.failed"
    assert events[-1]["data"]["error"] == "boom"
    assert "RuntimeError: boom" in events[-1]["data"]["traceback"]
