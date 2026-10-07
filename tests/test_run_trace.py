import json
from uuid import uuid4

from data_refiner_agent.run_trace import RunTraceHandler


def _read_events(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_trace_records_tool_result_and_redacts_credentials(tmp_path) -> None:
    path = tmp_path / "run.jsonl"
    handler = RunTraceHandler(path, "thread-1", invocation_id="invocation-1")
    run_id = uuid4()

    handler.on_tool_start(
        {"name": "install_workspace_package"},
        "ignored",
        run_id=run_id,
        metadata={"agent_name": "operator-builder-agent"},
        inputs={"package_name": "pypinyin", "token": "secret-token"},
    )
    handler.on_tool_end(
        {"status": "succeeded", "stdout": "installed"},
        run_id=run_id,
    )
    handler.close()

    events = _read_events(path)
    assert [event["event"] for event in events] == [
        "tool.started",
        "tool.completed",
    ]
    assert events[0]["data"]["input"] == {
        "package_name": "pypinyin",
        "token": "<REDACTED>",
    }
    assert events[1]["data"]["output"] == {
        "status": "succeeded",
        "stdout": "installed",
    }


def test_trace_records_pipeline_agent_node_decision(tmp_path) -> None:
    path = tmp_path / "run.jsonl"
    handler = RunTraceHandler(path, "thread-1", invocation_id="invocation-1")
    run_id = uuid4()

    handler.on_chain_start(
        None,
        {"stage": "PLANNING_TEST_PIPELINE"},
        run_id=run_id,
        metadata={
            "agent_name": "workflow",
            "langgraph_node": "pipeline-agent",
        },
        name="pipeline-agent",
    )
    handler.on_chain_end(
        {
            "pipeline_agent_result": {
                "action": "submit-test-pipeline",
                "pipeline_yaml": "reader:\n  op_name: jsonl_reader\n",
            }
        },
        run_id=run_id,
    )
    handler.close()

    events = _read_events(path)
    completed = events[-1]
    assert completed["event"] == "node.completed"
    assert completed["node"] == "pipeline-agent"
    assert completed["data"]["output"]["pipeline_agent_result"] == {
        "action": "submit-test-pipeline",
        "pipeline_yaml": "reader:\n  op_name: jsonl_reader\n",
    }
