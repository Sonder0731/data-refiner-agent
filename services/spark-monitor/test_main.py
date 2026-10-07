import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from pydantic import ValidationError

from main import (
    ApplicationTaskStatus,
    ExecutionLog,
    ExecutionLogs,
    MonitorTaskRequest,
    _LogParser,
    _create_monitor_task,
    _fetch_execution_logs_when_ready,
    _get_monitor_task,
    _tail_log_url,
    _watch_application,
    active_monitor_tasks,
    app,
    fetch_execution_logs,
    fetch_application,
    monitored_applications,
    terminal_final_status,
)


class StatusTest(unittest.TestCase):
    def test_structured_terminal_status_is_authoritative(self) -> None:
        self.assertEqual(
            "SUCCEEDED",
            terminal_final_status({"state": "FINISHED", "final_status": "SUCCEEDED"}),
        )
        self.assertEqual("FAILED", terminal_final_status({"state": "FAILED"}))
        self.assertEqual("KILLED", terminal_final_status({"state": "KILLED"}))
        self.assertEqual("FINISHED", terminal_final_status({"state": "FINISHED"}))
        self.assertIsNone(terminal_final_status({"state": "RUNNING"}))

    def test_request_only_accepts_application_id(self) -> None:
        request = MonitorTaskRequest(application_id="application_1")
        self.assertEqual({"application_id": "application_1"}, request.model_dump())

        with self.assertRaises(ValidationError):
            MonitorTaskRequest(
                application_id="application_1",
                request_id="550e8400-e29b-41d4-a716-446655440000",
            )

    def test_monitor_and_query_endpoints_are_exposed(self) -> None:
        routes = {route.path: route for route in app.routes}
        self.assertIn("/monitor/tasks", routes)
        query_route = routes["/monitor/tasks/{application_id}"]
        self.assertTrue(query_route.response_model_exclude_none)


class MonitorTaskTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        active_monitor_tasks.clear()
        monitored_applications.clear()

    def tearDown(self) -> None:
        active_monitor_tasks.clear()
        monitored_applications.clear()

    def _request(self) -> MonitorTaskRequest:
        return MonitorTaskRequest(application_id="application_1")

    async def test_running_status_contains_no_log_information(self) -> None:
        with (
            patch("main.fetch_application", new_callable=AsyncMock) as application_mock,
            patch("main.asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
        ):
            application_mock.return_value = {"state": "RUNNING", "final_status": "UNDEFINED"}
            sleep_mock.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await _watch_application("application_1")

        result = await _get_monitor_task("application_1")
        self.assertEqual("RUNNING", result.status)
        self.assertNotIn("logs", result.model_dump(exclude_none=True))

    async def test_terminal_status_contains_execution_logs(self) -> None:
        logs = ExecutionLogs(
            status="available",
            spark_attempt_id="1",
            containers=[ExecutionLog(executor_id="driver", stdout="driver output")],
        )
        with (
            patch("main.fetch_application", new_callable=AsyncMock) as application_mock,
            patch("main.fetch_execution_logs", new_callable=AsyncMock) as logs_mock,
            patch("main.asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
        ):
            application_mock.side_effect = [
                {"state": "RUNNING", "final_status": "UNDEFINED"},
                {"state": "FINISHED", "final_status": "FAILED"},
            ]
            logs_mock.return_value = logs
            await _watch_application("application_1")

        result = await _get_monitor_task("application_1")
        self.assertEqual("FAILED", result.status)
        self.assertEqual(logs, result.logs)
        sleep_mock.assert_awaited_once_with(5)

    async def test_duplicate_application_is_not_started_twice(self) -> None:
        with patch("main.asyncio.create_task") as create_task:
            def create_task_without_running(coro):
                coro.close()
                return object()

            create_task.side_effect = create_task_without_running
            first = await _create_monitor_task(self._request())
            second = await _create_monitor_task(self._request())

        self.assertEqual("watching", first.status)
        self.assertEqual("already_watching", second.status)
        create_task.assert_called_once()

    async def test_unknown_application_returns_404(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            await _get_monitor_task("application_missing")
        self.assertEqual(404, raised.exception.status_code)

    async def test_retries_until_history_has_executor_metadata(self) -> None:
        unavailable = ExecutionLogs(
            status="unavailable", errors=["executor log metadata was not found"]
        )
        ready = ExecutionLogs(
            status="available",
            containers=[ExecutionLog(executor_id="driver", stdout="driver output")],
        )
        with (
            patch("main.fetch_execution_logs", new_callable=AsyncMock) as logs_mock,
            patch("main.asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
        ):
            logs_mock.side_effect = [unavailable, ready]
            result = await _fetch_execution_logs_when_ready("application_1")

        self.assertEqual(ready, result)
        self.assertEqual(2, logs_mock.await_count)
        sleep_mock.assert_awaited_once_with(5)


class ExecutionLogFetchTest(unittest.IsolatedAsyncioTestCase):
    @patch("main._read_log")
    @patch("main._json")
    @patch("main.asyncio.to_thread", new_callable=AsyncMock)
    async def test_fetches_driver_and_all_executors(
        self, to_thread_mock, json_mock, read_mock
    ) -> None:
        to_thread_mock.side_effect = lambda function, *args: function(*args)
        json_mock.side_effect = [
            {"attempts": [{"attemptId": "1", "lastUpdatedEpoch": 2}]},
            [
                {
                    "id": "driver",
                    "attributes": {
                        "CONTAINER_ID": "container_1",
                        "NM_HTTP_ADDRESS": "worker1:8042",
                    },
                    "executorLogs": {
                        "stdout": "http://worker1/stdout",
                        "stderr": "http://worker1/stderr",
                    },
                },
                {
                    "id": "1",
                    "attributes": {
                        "CONTAINER_ID": "container_2",
                        "NM_HTTP_ADDRESS": "worker2:8042",
                    },
                    "executorLogs": {
                        "stdout": "http://worker2/stdout",
                        "stderr": "http://worker2/stderr",
                    },
                },
            ],
        ]
        read_mock.side_effect = lambda url, limit: (url, False)

        result = await fetch_execution_logs("application_1")

        self.assertEqual("available", result.status)
        self.assertEqual("1", result.spark_attempt_id)
        self.assertEqual(["driver", "1"], [item.executor_id for item in result.containers])
        self.assertEqual("http://worker1/stderr", result.containers[0].stderr)
        self.assertEqual(4, read_mock.call_count)

    @patch("main._json")
    @patch("main.asyncio.to_thread", new_callable=AsyncMock)
    async def test_fetches_yarn_diagnostics_and_attempts(self, to_thread_mock, json_mock) -> None:
        to_thread_mock.side_effect = lambda function, *args: function(*args)
        json_mock.side_effect = [
            {
                "app": {
                    "state": "FAILED",
                    "finalStatus": "FAILED",
                    "name": "pipeline",
                    "diagnostics": "executor failed",
                }
            },
            {"appAttempts": {"appAttempt": [{"appAttemptId": "appattempt_1"}]}},
        ]

        result = await fetch_application("application_1")

        self.assertEqual("executor failed", result["diagnostics"])
        self.assertEqual([{"appAttemptId": "appattempt_1"}], result["attempts"])


class LogParsingTest(unittest.TestCase):
    def test_extracts_preformatted_log_text(self) -> None:
        parser = _LogParser()
        parser.feed("<html><pre>line &lt;1&gt;\nline 2</pre></html>")
        self.assertEqual("line <1>\nline 2", "".join(parser.parts))

    def test_tail_url_replaces_existing_range(self) -> None:
        url = _tail_log_url("http://worker/stdout?start=-4096&end=10", 65536)
        self.assertEqual("http://worker/stdout?start=-65536", url)


if __name__ == "__main__":
    unittest.main()
