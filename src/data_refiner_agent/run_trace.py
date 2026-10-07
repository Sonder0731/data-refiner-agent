import json
import os
import re
import time
import traceback
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from langchain_core.callbacks import BaseCallbackHandler

REDACTED = "<REDACTED>"
SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "cookie",
    "password",
    "secret",
    "set_cookie",
    "token",
}
URL_CREDENTIALS = re.compile(
    r"([a-z][a-z0-9+.-]*://[^:/\s]+:)[^@\s]+(@)", re.IGNORECASE
)
BEARER_TOKEN = re.compile(r"\bBearer\s+\S+", re.IGNORECASE)
SECRET_ASSIGNMENT = re.compile(
    r"\b(password|api[_-]?key|access[_-]?token|refresh[_-]?token|secret)="
    r"([^\s&]+)",
    re.IGNORECASE,
)


def _is_sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return normalized in SENSITIVE_KEYS or normalized.endswith(
        ("_password", "_secret", "_token")
    )


def _redact_text(value: str) -> str:
    value = URL_CREDENTIALS.sub(r"\1<REDACTED>\2", value)
    value = BEARER_TOKEN.sub("Bearer <REDACTED>", value)
    return SECRET_ASSIGNMENT.sub(r"\1=<REDACTED>", value)


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, bytes):
        return _redact_text(value.decode(errors="replace"))
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if hasattr(value, "model_dump"):
        try:
            return _jsonable(value.model_dump(mode="json"))
        except TypeError:
            return _jsonable(value.model_dump())
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            string_key = str(key)
            result[string_key] = (
                REDACTED if _is_sensitive_key(string_key) else _jsonable(item)
            )
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    return _redact_text(str(value))


class RunTraceHandler(BaseCallbackHandler):
    """Append one structured event per line for a complete workflow run."""

    raise_error = True
    run_inline = True

    def __init__(
        self,
        path: str | Path,
        thread_id: str,
        *,
        invocation_id: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            self.path,
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o600,
        )
        self._stream = os.fdopen(descriptor, "a", encoding="utf-8")
        self._lock = Lock()
        self.thread_id = thread_id
        self.invocation_id = invocation_id or str(uuid4())
        self._model_runs: dict[Any, tuple[str, str | None, float, Any]] = {}
        self._tool_runs: dict[Any, tuple[str, str | None, str, float, Any]] = {}
        self._node_runs: dict[Any, tuple[str, str, float, Any]] = {}

    @staticmethod
    def _agent(metadata: dict[str, Any] | None) -> str:
        return str((metadata or {}).get("agent_name", "agent"))

    @staticmethod
    def _node(metadata: dict[str, Any] | None) -> str | None:
        node = (metadata or {}).get("langgraph_node")
        return str(node) if node else None

    @staticmethod
    def _duration(started: float) -> float:
        return round((time.monotonic() - started) * 1000, 3)

    def record(
        self,
        event: str,
        *,
        agent: str = "system",
        node: str | None = None,
        run_id: Any = None,
        parent_run_id: Any = None,
        duration_ms: float | None = None,
        data: Any = None,
    ) -> None:
        item = {
            "timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "thread_id": self.thread_id,
            "invocation_id": self.invocation_id,
            "event": event,
            "agent": agent,
            "data": _jsonable(data),
        }
        if node is not None:
            item["node"] = node
        if run_id is not None:
            item["run_id"] = str(run_id)
        if parent_run_id is not None:
            item["parent_run_id"] = str(parent_run_id)
        if duration_ms is not None:
            item["duration_ms"] = duration_ms
        line = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()

    def close(self) -> None:
        with self._lock:
            if not self._stream.closed:
                self._stream.close()

    def on_chain_start(
        self,
        serialized: dict[str, Any] | None,
        inputs: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        node = self._node(metadata)
        if node is None or kwargs.get("name") != node:
            return
        agent = self._agent(metadata)
        self._node_runs[run_id] = (node, agent, time.monotonic(), parent_run_id)
        self.record(
            "node.started",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id,
            data={"input": inputs},
        )

    def on_chain_end(
        self,
        outputs: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._node_runs.pop(run_id, None)
        if run is None:
            return
        node, agent, started, stored_parent = run
        self.record(
            "node.completed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={"output": outputs},
        )

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._node_runs.pop(run_id, None)
        if run is None:
            return
        node, agent, started, stored_parent = run
        self.record(
            "node.failed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={
                "error_type": type(error).__name__,
                "error": str(error),
                "traceback": "".join(traceback.format_exception(error)),
            },
        )

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[Any]],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        agent = self._agent(metadata)
        node = self._node(metadata)
        self._model_runs[run_id] = (
            agent,
            node,
            time.monotonic(),
            parent_run_id,
        )
        self.record(
            "model.started",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id,
            data={"model": serialized, "messages": messages},
        )

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._model_runs.pop(run_id, None)
        if run is None:
            return
        agent, node, started, stored_parent = run
        self.record(
            "model.completed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={"response": response},
        )

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._model_runs.pop(run_id, None)
        if run is None:
            return
        agent, node, started, stored_parent = run
        self.record(
            "model.failed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={
                "error_type": type(error).__name__,
                "error": str(error),
                "traceback": "".join(traceback.format_exception(error)),
            },
        )

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        agent = self._agent(metadata)
        node = self._node(metadata)
        tool = str(serialized.get("name", "unknown_tool"))
        self._tool_runs[run_id] = (
            agent,
            node,
            tool,
            time.monotonic(),
            parent_run_id,
        )
        self.record(
            "tool.started",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id,
            data={"tool": tool, "input": kwargs.get("inputs", input_str)},
        )

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._tool_runs.pop(run_id, None)
        if run is None:
            return
        agent, node, tool, started, stored_parent = run
        self.record(
            "tool.completed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={"tool": tool, "output": output},
        )

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        run = self._tool_runs.pop(run_id, None)
        if run is None:
            return
        agent, node, tool, started, stored_parent = run
        self.record(
            "tool.failed",
            agent=agent,
            node=node,
            run_id=run_id,
            parent_run_id=parent_run_id or stored_parent,
            duration_ms=self._duration(started),
            data={
                "tool": tool,
                "error_type": type(error).__name__,
                "error": str(error),
                "traceback": "".join(traceback.format_exception(error)),
            },
        )

    def on_custom_event(
        self,
        name: str,
        data: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.record(
            "custom",
            agent=self._agent(metadata),
            node=self._node(metadata),
            run_id=run_id,
            parent_run_id=parent_run_id,
            data={"name": name, "value": data},
        )


__all__ = ["RunTraceHandler"]
