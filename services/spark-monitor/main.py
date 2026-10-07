from __future__ import annotations

import asyncio
import json
import logging
import os
from html.parser import HTMLParser
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field


app = FastAPI(title="Spark Monitor")
logger = logging.getLogger(__name__)

HISTORY_URL = os.getenv("SPARK_HISTORY_URL", "http://127.0.0.1:18080")
RESOURCE_MANAGER_URL = os.getenv("YARN_RESOURCE_MANAGER_URL", "http://127.0.0.1:8088")

TERMINAL_FINAL_STATUSES = {"SUCCEEDED", "FAILED", "KILLED"}
MONITOR_POLL_INTERVAL_SECONDS = 5
EXECUTION_LOG_RETRY_ATTEMPTS = 12
LOG_TAIL_BYTES = 64 * 1024
active_monitor_tasks: dict[str, asyncio.Task[None]] = {}
monitor_task_lock = asyncio.Lock()


class MonitorTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    application_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9._-]+$")


class ExecutionLog(BaseModel):
    executor_id: str
    container_id: str | None = None
    host: str | None = None
    stdout: str = ""
    stdout_truncated: bool = False
    stderr: str = ""
    stderr_truncated: bool = False


class ExecutionLogs(BaseModel):
    status: str
    spark_attempt_id: str | None = None
    containers: list[ExecutionLog] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class MonitorTaskResponse(BaseModel):
    application_id: str
    status: str


class ApplicationTaskStatus(BaseModel):
    application_id: str
    status: str
    diagnostics: str | None = None
    attempts: list[dict[str, Any]] = Field(default_factory=list)
    logs: ExecutionLogs | None = None


monitored_applications: dict[str, ApplicationTaskStatus] = {}


def _json(url: str) -> Any:
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise RuntimeError(f"GET {url} returned HTTP {exc.code}") from exc
    except OSError as exc:
        raise RuntimeError(f"GET {url} failed: {exc}") from exc


class _LogParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_pre = False
        self.found_pre = False
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "pre":
            self.in_pre = True
            self.found_pre = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "pre":
            self.in_pre = False

    def handle_data(self, data: str) -> None:
        if self.in_pre:
            self.parts.append(data)


def _tail_log_url(url: str, limit: int) -> str:
    parsed = urlsplit(url)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key not in {"start", "end"}]
    query.append(("start", f"-{limit}"))
    return urlunsplit(parsed._replace(query=urlencode(query)))


def _read_log(url: str, limit: int) -> tuple[str, bool]:
    request = Request(_tail_log_url(url, limit), headers={"Accept": "text/html, text/plain"})
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read()
    except HTTPError as exc:
        raise RuntimeError(f"GET {url} returned HTTP {exc.code}") from exc
    except OSError as exc:
        raise RuntimeError(f"GET {url} failed: {exc}") from exc

    raw_text = body.decode("utf-8", errors="replace")
    parser = _LogParser()
    parser.feed(raw_text)
    content = "".join(parser.parts) if parser.found_pre else raw_text
    return content, len(content.encode("utf-8")) >= limit


async def _read_log_value(url: Any) -> tuple[str, bool, str | None]:
    if not isinstance(url, str) or not url:
        return "", False, "log URL is missing"
    try:
        content, truncated = await asyncio.to_thread(_read_log, url, LOG_TAIL_BYTES)
        return content, truncated, None
    except RuntimeError as exc:
        return "", False, str(exc)


async def fetch_execution_logs(application_id: str) -> ExecutionLogs:
    encoded_id = quote(application_id, safe="")
    try:
        history = await asyncio.to_thread(
            _json,
            f"{HISTORY_URL.rstrip('/')}/api/v1/applications/{encoded_id}",
        )
    except RuntimeError as exc:
        return ExecutionLogs(status="unavailable", errors=[str(exc)])

    if not isinstance(history, dict):
        return ExecutionLogs(status="unavailable", errors=["application history was not found"])

    attempts = [item for item in history.get("attempts", []) if isinstance(item, dict)]
    attempt = max(attempts, key=lambda item: item.get("lastUpdatedEpoch") or 0, default={})
    spark_attempt_id = attempt.get("attemptId")
    if spark_attempt_id is None:
        return ExecutionLogs(status="unavailable", errors=["application attempt was not found"])

    try:
        executors = await asyncio.to_thread(
            _json,
            f"{HISTORY_URL.rstrip('/')}/api/v1/applications/{encoded_id}/{quote(str(spark_attempt_id), safe='')}/executors",
        )
    except RuntimeError as exc:
        return ExecutionLogs(
            status="unavailable",
            spark_attempt_id=str(spark_attempt_id),
            errors=[str(exc)],
        )

    if not isinstance(executors, list) or not executors:
        return ExecutionLogs(
            status="unavailable",
            spark_attempt_id=str(spark_attempt_id),
            errors=["executor log metadata was not found"],
        )

    errors: list[str] = []

    async def load_executor(executor: dict[str, Any]) -> ExecutionLog:
        executor_id = str(executor.get("id", "unknown"))
        attributes = executor.get("attributes") if isinstance(executor.get("attributes"), dict) else {}
        executor_logs = executor.get("executorLogs")
        if not isinstance(executor_logs, dict):
            errors.append(f"{executor_id}: executor log URLs are unavailable")
            return ExecutionLog(
                executor_id=executor_id,
                container_id=attributes.get("CONTAINER_ID"),
                host=attributes.get("NM_HTTP_ADDRESS"),
            )

        (stdout, stdout_truncated, stdout_error), (
            stderr,
            stderr_truncated,
            stderr_error,
        ) = await asyncio.gather(
            _read_log_value(executor_logs.get("stdout")),
            _read_log_value(executor_logs.get("stderr")),
        )
        if stdout_error:
            errors.append(f"{executor_id} stdout: {stdout_error}")
        if stderr_error:
            errors.append(f"{executor_id} stderr: {stderr_error}")
        return ExecutionLog(
            executor_id=executor_id,
            container_id=attributes.get("CONTAINER_ID"),
            host=attributes.get("NM_HTTP_ADDRESS"),
            stdout=stdout,
            stdout_truncated=stdout_truncated,
            stderr=stderr,
            stderr_truncated=stderr_truncated,
        )

    records = await asyncio.gather(
        *(load_executor(executor) for executor in executors if isinstance(executor, dict))
    )
    if not records:
        return ExecutionLogs(
            status="unavailable",
            spark_attempt_id=str(spark_attempt_id),
            errors=["executor log metadata was empty"],
        )
    records.sort(key=lambda item: (item.executor_id != "driver", item.executor_id))
    return ExecutionLogs(
        status="partial" if errors else "available",
        spark_attempt_id=str(spark_attempt_id),
        containers=records,
        errors=errors,
    )


def terminal_final_status(status: dict[str, Any]) -> str | None:
    final_status = str(status.get("final_status") or "").upper()
    if final_status in TERMINAL_FINAL_STATUSES:
        return final_status
    state = str(status.get("state") or "").upper()
    return state if state in {"FINISHED", "FAILED", "KILLED"} else None


async def fetch_application(application_id: str) -> dict[str, Any]:
    encoded_id = quote(application_id, safe="")
    errors: list[str] = []

    try:
        yarn = await asyncio.to_thread(
            _json,
            f"{RESOURCE_MANAGER_URL.rstrip('/')}/ws/v1/cluster/apps/{encoded_id}",
        )
        if isinstance(yarn, dict) and isinstance(yarn.get("app"), dict):
            app_data = yarn["app"]
            state = str(app_data.get("state", "UNKNOWN")).upper()
            attempts: list[dict[str, Any]] = []
            try:
                yarn_attempts = await asyncio.to_thread(
                    _json,
                    f"{RESOURCE_MANAGER_URL.rstrip('/')}/ws/v1/cluster/apps/"
                    f"{encoded_id}/appattempts",
                )
                raw_attempts = (
                    yarn_attempts.get("appAttempts", {}).get("appAttempt", [])
                    if isinstance(yarn_attempts, dict)
                    else []
                )
                attempts = [item for item in raw_attempts if isinstance(item, dict)]
            except RuntimeError:
                pass
            return {
                "application_id": application_id,
                "state": state,
                "final_status": app_data.get("finalStatus"),
                "source": "yarn",
                "name": app_data.get("name"),
                "diagnostics": app_data.get("diagnostics"),
                "attempts": attempts,
            }
    except RuntimeError as exc:
        errors.append(str(exc))

    try:
        history = await asyncio.to_thread(
            _json,
            f"{HISTORY_URL.rstrip('/')}/api/v1/applications/{encoded_id}",
        )
        if isinstance(history, dict):
            attempts = history.get("attempts") or []
            attempt = max(attempts, key=lambda item: item.get("lastUpdatedEpoch") or 0) if attempts else {}
            completed = bool(attempt.get("completed"))
            return {
                "application_id": application_id,
                "state": "FINISHED" if completed else "RUNNING",
                "final_status": None,
                "source": "history",
                "name": history.get("name"),
            }
    except RuntimeError as exc:
        errors.append(str(exc))

    if errors:
        raise HTTPException(status_code=502, detail="Spark status service unavailable")
    raise HTTPException(status_code=404, detail=f"Application {application_id} was not found")


async def _fetch_execution_logs_when_ready(application_id: str) -> ExecutionLogs:
    logs = await fetch_execution_logs(application_id)
    for _ in range(EXECUTION_LOG_RETRY_ATTEMPTS - 1):
        if logs.containers:
            return logs
        await asyncio.sleep(MONITOR_POLL_INTERVAL_SECONDS)
        logs = await fetch_execution_logs(application_id)
    return logs


async def _watch_application(application_id: str) -> None:
    try:
        while True:
            try:
                status = await fetch_application(application_id)
            except Exception as exc:
                # ponytail: retry in memory forever; add persistence/timeout only when abandoned jobs matter.
                logger.warning("Monitor poll failed for %s: %s", application_id, exc)
                await asyncio.sleep(MONITOR_POLL_INTERVAL_SECONDS)
                continue

            final_status = terminal_final_status(status)
            if final_status is None:
                async with monitor_task_lock:
                    monitored_applications[application_id] = ApplicationTaskStatus(
                        application_id=application_id,
                        status=str(status.get("state") or "UNKNOWN"),
                        diagnostics=status.get("diagnostics"),
                        attempts=status.get("attempts") or [],
                    )
                await asyncio.sleep(MONITOR_POLL_INTERVAL_SECONDS)
                continue

            execution_logs = await _fetch_execution_logs_when_ready(application_id)
            async with monitor_task_lock:
                monitored_applications[application_id] = ApplicationTaskStatus(
                    application_id=application_id,
                    status=final_status,
                    diagnostics=status.get("diagnostics"),
                    attempts=status.get("attempts") or [],
                    logs=execution_logs,
                )
            return
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Monitor task failed for %s", application_id)
    finally:
        async with monitor_task_lock:
            active_monitor_tasks.pop(application_id, None)


async def _create_monitor_task(request: MonitorTaskRequest) -> MonitorTaskResponse:
    async with monitor_task_lock:
        current = monitored_applications.get(request.application_id)
        if request.application_id in active_monitor_tasks:
            status = "already_watching"
        elif current is not None and current.logs is not None:
            status = "already_finished"
        else:
            monitored_applications[request.application_id] = ApplicationTaskStatus(
                application_id=request.application_id,
                status="WATCHING",
            )
            active_monitor_tasks[request.application_id] = asyncio.create_task(
                _watch_application(request.application_id)
            )
            status = "watching"
    return MonitorTaskResponse(
        application_id=request.application_id,
        status=status,
    )


async def _get_monitor_task(application_id: str) -> ApplicationTaskStatus:
    async with monitor_task_lock:
        status = monitored_applications.get(application_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Application {application_id} is not monitored")
    return status


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/monitor/tasks", response_model=MonitorTaskResponse, status_code=202)
async def create_monitor_task(request: MonitorTaskRequest) -> MonitorTaskResponse:
    return await _create_monitor_task(request)


@app.get(
    "/monitor/tasks/{application_id}",
    response_model=ApplicationTaskStatus,
    response_model_exclude_none=True,
)
async def get_monitor_task(application_id: str) -> ApplicationTaskStatus:
    return await _get_monitor_task(application_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8010, reload=False)
