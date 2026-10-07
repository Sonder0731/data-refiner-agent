from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from psycopg.rows import dict_row

from data_refiner_agent_tools.database.connection import connect_database
from data_refiner_agent_tools.database.history import History
from data_refiner_agent_tools.database.request_state import RequestState


class WorkflowStage(StrEnum):
    RECEIVED = "RECEIVED"
    WORKSPACE_PREPARING = "WORKSPACE_PREPARING"
    PROFILING_INPUT = "PROFILING_INPUT"
    EVALUATING_OPERATORS = "EVALUATING_OPERATORS"
    BUILDING_OPERATOR = "BUILDING_OPERATOR"
    PLANNING_TEST_PIPELINE = "PLANNING_TEST_PIPELINE"
    SUBMITTING_TEST_PIPELINE = "SUBMITTING_TEST_PIPELINE"
    MONITORING_TEST_PIPELINE = "MONITORING_TEST_PIPELINE"
    PROFILING_TEST_OUTPUT = "PROFILING_TEST_OUTPUT"
    VERIFYING_TEST_OUTPUT = "VERIFYING_TEST_OUTPUT"
    PLANNING_PRODUCTION_PIPELINE = "PLANNING_PRODUCTION_PIPELINE"
    SIZING_PRODUCTION = "SIZING_PRODUCTION"
    SUBMITTING_PRODUCTION_PIPELINE = "SUBMITTING_PRODUCTION_PIPELINE"
    MONITORING_PRODUCTION_PIPELINE = "MONITORING_PRODUCTION_PIPELINE"
    PROFILING_OUTPUT = "PROFILING_OUTPUT"
    FINALIZING = "FINALIZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


ALLOWED_TRANSITIONS: dict[WorkflowStage, frozenset[WorkflowStage]] = {
    WorkflowStage.RECEIVED: frozenset({WorkflowStage.WORKSPACE_PREPARING}),
    WorkflowStage.WORKSPACE_PREPARING: frozenset({WorkflowStage.PROFILING_INPUT}),
    WorkflowStage.PROFILING_INPUT: frozenset({WorkflowStage.EVALUATING_OPERATORS}),
    WorkflowStage.EVALUATING_OPERATORS: frozenset(
        {
            WorkflowStage.BUILDING_OPERATOR,
            WorkflowStage.PLANNING_TEST_PIPELINE,
        }
    ),
    WorkflowStage.BUILDING_OPERATOR: frozenset({WorkflowStage.EVALUATING_OPERATORS}),
    WorkflowStage.PLANNING_TEST_PIPELINE: frozenset(
        {WorkflowStage.SUBMITTING_TEST_PIPELINE}
    ),
    WorkflowStage.SUBMITTING_TEST_PIPELINE: frozenset(
        {WorkflowStage.MONITORING_TEST_PIPELINE}
    ),
    WorkflowStage.MONITORING_TEST_PIPELINE: frozenset(
        {
            WorkflowStage.BUILDING_OPERATOR,
            WorkflowStage.PLANNING_TEST_PIPELINE,
            WorkflowStage.PROFILING_TEST_OUTPUT,
        }
    ),
    WorkflowStage.PROFILING_TEST_OUTPUT: frozenset(
        {
            WorkflowStage.VERIFYING_TEST_OUTPUT,
            WorkflowStage.PLANNING_TEST_PIPELINE,
        }
    ),
    WorkflowStage.VERIFYING_TEST_OUTPUT: frozenset(
        {
            WorkflowStage.PLANNING_TEST_PIPELINE,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
        }
    ),
    WorkflowStage.PLANNING_PRODUCTION_PIPELINE: frozenset(
        {WorkflowStage.SIZING_PRODUCTION}
    ),
    WorkflowStage.SIZING_PRODUCTION: frozenset(
        {WorkflowStage.SUBMITTING_PRODUCTION_PIPELINE}
    ),
    WorkflowStage.SUBMITTING_PRODUCTION_PIPELINE: frozenset(
        {WorkflowStage.MONITORING_PRODUCTION_PIPELINE}
    ),
    WorkflowStage.MONITORING_PRODUCTION_PIPELINE: frozenset(
        {
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
            WorkflowStage.SIZING_PRODUCTION,
            WorkflowStage.PROFILING_OUTPUT,
        }
    ),
    WorkflowStage.PROFILING_OUTPUT: frozenset(
        {
            WorkflowStage.FINALIZING,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
        }
    ),
    WorkflowStage.FINALIZING: frozenset({WorkflowStage.COMPLETED}),
    WorkflowStage.COMPLETED: frozenset(),
    WorkflowStage.FAILED: frozenset(),
}

for _stage in tuple(ALLOWED_TRANSITIONS):
    if _stage not in (WorkflowStage.COMPLETED, WorkflowStage.FAILED):
        ALLOWED_TRANSITIONS[_stage] = ALLOWED_TRANSITIONS[_stage] | {
            WorkflowStage.FAILED
        }


def extract_username(matrix_id: str) -> str:
    username, separator, _ = matrix_id.removeprefix("@").partition(":")
    if not separator or not username:
        raise ValueError(f"Invalid Matrix ID: {matrix_id}")
    return username


def _active_handoff(
    event: str,
    assignee: str,
) -> dict[str, Any]:
    next_stages = sorted(
        stage.value for stage in ALLOWED_TRANSITIONS[WorkflowStage(event)]
    )
    return {
        "event": event,
        "expected_next_stage": next_stages[0] if len(next_stages) == 1 else None,
        "expected_next_stages": next_stages,
        "assignee": assignee,
        "sent_at": datetime.now(UTC).isoformat(),
    }


class RequestWorkflow:
    @staticmethod
    def validate_transition(
        current_stage: str | WorkflowStage,
        next_stage: str | WorkflowStage,
    ) -> None:
        current = WorkflowStage(current_stage)
        target = WorkflowStage(next_stage)
        if target not in ALLOWED_TRANSITIONS[current]:
            raise ValueError(f"Illegal workflow transition: {current} -> {target}")

    @staticmethod
    def create(
        user_id: str,
        room_id: str,
        current_stage: str,
        user_request: str,
        expected_next_stage: str,
        assignee: str,
        *,
        request_id: str | UUID | None = None,
        dsn: str | None = None,
    ) -> dict[str, Any]:
        request_uuid = UUID(str(request_id)) if request_id else uuid4()
        RequestWorkflow.validate_transition(current_stage, expected_next_stage)
        handoff = _active_handoff(
            current_stage,
            assignee,
        )
        history_content = [{"stage": current_stage, "at": handoff["sent_at"]}]

        with connect_database(dsn, row_factory=dict_row) as connection:
            history = History.create(
                request_uuid,
                history_content,
                connection=connection,
            )
            return RequestState.create(
                request_id=request_uuid,
                user_id=user_id,
                user_name=extract_username(user_id),
                room_id=room_id,
                current_stage=current_stage,
                user_request=user_request,
                history_id=history["id"],
                active_handoff=handoff,
                connection=connection,
            )

    @staticmethod
    def transition(
        request_id: str | UUID,
        next_stage: str | WorkflowStage,
        assignee: str,
        *,
        dsn: str | None = None,
    ) -> dict[str, Any]:
        with connect_database(dsn, row_factory=dict_row) as connection:
            state = RequestState.get(request_id, connection=connection)
            if state is None:
                raise LookupError("request_state was not found")
            current_stage = WorkflowStage(state["current_stage"])
            target = WorkflowStage(next_stage)
            RequestWorkflow.validate_transition(current_stage, target)
            handoff = _active_handoff(target, assignee)
            history_item = {"stage": target, "at": handoff["sent_at"]}
            RequestState.compare_and_set_current_stage(
                request_id,
                current_stage,
                target,
                connection=connection,
            )
            state = RequestState.update_active_handoff(
                request_id,
                handoff,
                connection=connection,
            )
            History.append(
                request_id,
                history_item,
                connection=connection,
            )
            return state
