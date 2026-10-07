import argparse
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import quote
from uuid import uuid4

from langchain_core.callbacks import BaseCallbackHandler
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.types import Command

from data_refiner_agent.run_trace import RunTraceHandler
from data_refiner_agent.workflow import create_workflow
from data_refiner_agent.workflow_types import WorkflowStage


class ConsoleProgressHandler(BaseCallbackHandler):
    def __init__(self, stream: TextIO = sys.stderr) -> None:
        self.stream = stream
        self.model_runs: dict[Any, tuple[str, float]] = {}
        self.tool_runs: dict[Any, tuple[str, str, float]] = {}

    def log(self, level: str, agent: str, message: str) -> None:
        timestamp = datetime.now().astimezone().strftime("%H:%M:%S")
        print(
            f"[{timestamp}] [{level:<5}] [{agent}] {message}",
            file=self.stream,
            flush=True,
        )

    @staticmethod
    def _agent(metadata: dict[str, Any] | None) -> str:
        return str((metadata or {}).get("agent_name", "agent"))

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[Any]],
        *,
        run_id: Any,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        agent = self._agent(metadata)
        self.model_runs[run_id] = (agent, time.monotonic())
        self.log("THINK", agent, "Analyzing the next step")

    def on_llm_end(self, response: Any, *, run_id: Any, **kwargs: Any) -> None:
        if run := self.model_runs.pop(run_id, None):
            agent, started = run
            self.log(
                "DONE",
                agent,
                f"Analysis completed in {time.monotonic() - started:.1f}s",
            )

    def on_llm_error(self, error: BaseException, *, run_id: Any, **kwargs: Any) -> None:
        agent, _ = self.model_runs.pop(run_id, ("agent", 0.0))
        self.log("ERROR", agent, f"Model call failed: {type(error).__name__}")

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: Any,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        agent = self._agent(metadata)
        tool = str(serialized.get("name", "unknown_tool"))
        self.tool_runs[run_id] = (agent, tool, time.monotonic())
        self.log("START", agent, f"Calling tool {tool}")

    def on_tool_end(self, output: Any, *, run_id: Any, **kwargs: Any) -> None:
        if run := self.tool_runs.pop(run_id, None):
            agent, tool, started = run
            self.log(
                "DONE",
                agent,
                f"Tool {tool} completed in {time.monotonic() - started:.1f}s",
            )

    def on_tool_error(
        self, error: BaseException, *, run_id: Any, **kwargs: Any
    ) -> None:
        agent, tool, _ = self.tool_runs.pop(run_id, ("agent", "unknown_tool", 0.0))
        self.log("ERROR", agent, f"Tool {tool} failed: {type(error).__name__}")

    def on_custom_event(
        self,
        name: str,
        data: Any,
        *,
        run_id: Any,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        levels = {"progress": "WAIT", "completed": "DONE"}
        if name in levels and isinstance(data, str):
            self.log(levels[name], self._agent(metadata), data)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Data Refiner workflow")
    parser.add_argument("query", nargs="?", help="Data preview or processing request")
    parser.add_argument(
        "--user-id",
        default=os.getenv("DATA_REFINER_USER_ID", "@local:local"),
        help="User identity used for workspace and request persistence",
    )
    parser.add_argument(
        "--room-id",
        default=os.getenv("DATA_REFINER_ROOM_ID", "local"),
        help="Conversation identifier persisted with the request",
    )
    parser.add_argument("--thread-id", default=None)
    parser.add_argument(
        "--resume",
        help="Resume a paused workflow with this clarification answer",
    )
    parser.add_argument("--quiet", action="store_true", help="Only print the answer")
    parser.add_argument(
        "--debug", action="store_true", help="Show a traceback when execution fails"
    )
    parser.add_argument(
        "--log-dir",
        default=os.getenv("DATA_REFINER_LOG_DIR", "logs"),
        help="Directory for per-thread JSONL workflow logs",
    )
    return parser


def _config(
    thread_id: str,
    callbacks: list[BaseCallbackHandler],
    agent_name: str = "workflow",
) -> dict[str, Any]:
    return {
        "configurable": {"thread_id": thread_id},
        "callbacks": callbacks,
        "metadata": {"agent_name": agent_name},
    }


def _answer(result: dict[str, Any]) -> str:
    if answer := result.get("final_answer"):
        return str(answer)
    structured = result.get("structured_response")
    if structured is not None:
        for field in ("final_answer", "summary", "error"):
            if value := getattr(structured, field, None):
                return str(value)
    messages = result.get("messages", [])
    return str(messages[-1].content) if messages else str(result)


def _interrupt_question(result: dict[str, Any]) -> str | None:
    interrupts = result.get("__interrupt__", ())
    if not interrupts:
        return None
    value = getattr(interrupts[0], "value", interrupts[0])
    if isinstance(value, dict):
        return str(value.get("question", value))
    return str(value)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.resume is not None and args.query:
        parser.error("--resume cannot be combined with query")
    if args.resume is not None and not args.thread_id:
        parser.error("--thread-id is required with --resume")
    if args.resume is None and not args.query:
        parser.error("query is required unless --resume is used")

    progress = None if args.quiet else ConsoleProgressHandler()
    thread_id = args.thread_id or str(uuid4())
    log_path = Path(args.log_dir).expanduser() / f"{quote(thread_id, safe='')}.jsonl"
    trace = RunTraceHandler(log_path, thread_id)
    try:
        trace.record(
            "run.started",
            data={
                "query": args.query,
                "resume": args.resume,
                "user_id": args.user_id,
                "room_id": args.room_id,
            },
        )
        started = time.monotonic()
        if progress:
            progress.log(
                "INFO", "system", f"Initializing workflow, thread_id={thread_id}"
            )
            progress.log("INFO", "system", f"Workflow trace: {log_path}")
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        with PostgresSaver.from_conn_string(database_url) as checkpointer:
            checkpointer.setup()
            workflow = create_workflow(checkpointer=checkpointer)
            if progress:
                progress.log(
                    "DONE",
                    "system",
                    f"Workflow initialized in {time.monotonic() - started:.1f}s",
                )
            trace.record(
                "workflow.initialized",
                duration_ms=round((time.monotonic() - started) * 1000, 3),
            )
            workflow_input: dict[str, Any] | Command
            if args.resume is not None:
                workflow_input = Command(resume=args.resume)
            else:
                workflow_input = {
                    "query": args.query,
                    "user_id": args.user_id,
                    "room_id": args.room_id,
                }
            result = workflow.invoke(
                workflow_input,
                config=_config(
                    thread_id,
                    [trace, *([progress] if progress else [])],
                ),
            )

        if question := _interrupt_question(result):
            trace.record(
                "run.interrupted",
                data={"question": question, "result": result},
            )
            if progress:
                progress.log(
                    "WAIT",
                    "system",
                    "Waiting for user input; resume with "
                    f"--thread-id {thread_id} --resume <answer>",
                )
            else:
                print(f"thread_id={thread_id}", file=sys.stderr, flush=True)
            print(question)
            return 2
        answer = _answer(result)
        trace.record("run.completed", data={"result": result})
        print(answer)
        return 1 if result.get("stage") == WorkflowStage.FAILED else 0
    except Exception as exc:
        trace.record(
            "run.failed",
            data={
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": "".join(traceback.format_exception(exc)),
            },
        )
        if progress:
            progress.log(
                "ERROR", "system", f"Execution failed: {type(exc).__name__}: {exc}"
            )
        if args.debug:
            raise
        raise SystemExit(1) from None
    finally:
        trace.close()


if __name__ == "__main__":
    raise SystemExit(main())
