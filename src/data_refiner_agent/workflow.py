import json
from dataclasses import replace
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

from langchain_core.callbacks.manager import CallbackManager, dispatch_custom_event
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from pydantic import ValidationError

from data_refiner_agent import tools as operations
from data_refiner_agent.agents import (
    create_operator_builder_agent,
    create_pipeline_agent,
)
from data_refiner_agent.model import invoke_structured, resolve_model
from data_refiner_agent.subgraphs import (
    create_application_monitor_subgraph,
    create_data_profile_subgraph,
    create_spark_sizing_subgraph,
)
from data_refiner_agent.workflow_types import (
    FinalReport,
    IntakeResult,
    IntentRoute,
    MonitorOutcome,
    OperatorBuilderContext,
    OperatorBuildResult,
    OutputSpec,
    PipelineAction,
    PipelineAgentContext,
    PipelineAgentResult,
    PipelineRun,
    PipelineTask,
    WorkflowError,
    WorkflowStage,
    WorkflowState,
)
from data_refiner_agent_tools import RequestState, RequestWorkflow

MAX_OPERATOR_ROUNDS = 5
MAX_PIPELINE_ATTEMPTS = 3
MAX_PIPELINE_REVISIONS = 3

PIPELINE_ACTIONS_BY_TASK = {
    PipelineTask.EVALUATE_OPERATORS: {
        PipelineAction.BUILD_OPERATOR,
        PipelineAction.PLAN_TEST_PIPELINE,
        PipelineAction.FAIL,
    },
    PipelineTask.PLAN_TEST: {
        PipelineAction.SUBMIT_TEST_PIPELINE,
        PipelineAction.FAIL,
    },
    PipelineTask.VERIFY_TEST_OUTPUT: {
        PipelineAction.PLAN_PRODUCTION_PIPELINE,
        PipelineAction.RETRY_TEST_PIPELINE,
        PipelineAction.FAIL,
    },
    PipelineTask.PLAN_PRODUCTION: {
        PipelineAction.SIZE_PRODUCTION,
        PipelineAction.FAIL,
    },
    PipelineTask.ANALYZE_TEST_FAILURE: {
        PipelineAction.BUILD_OPERATOR,
        PipelineAction.RETRY_TEST_PIPELINE,
        PipelineAction.FAIL,
    },
    PipelineTask.ANALYZE_PRODUCTION_FAILURE: {
        PipelineAction.RETRY_PRODUCTION_PIPELINE,
        PipelineAction.RESIZE_PRODUCTION,
        PipelineAction.FAIL,
    },
}

STAGE_ASSIGNEES = {
    WorkflowStage.RECEIVED: "intake",
    WorkflowStage.WORKSPACE_PREPARING: "prepare-workspace",
    WorkflowStage.PROFILING_INPUT: "data-profile",
    WorkflowStage.EVALUATING_OPERATORS: "pipeline-agent",
    WorkflowStage.BUILDING_OPERATOR: "operator-builder-agent",
    WorkflowStage.PLANNING_TEST_PIPELINE: "pipeline-agent",
    WorkflowStage.SUBMITTING_TEST_PIPELINE: "submit-test",
    WorkflowStage.MONITORING_TEST_PIPELINE: "application-monitor",
    WorkflowStage.PROFILING_TEST_OUTPUT: "data-profile",
    WorkflowStage.VERIFYING_TEST_OUTPUT: "pipeline-agent",
    WorkflowStage.PLANNING_PRODUCTION_PIPELINE: "pipeline-agent",
    WorkflowStage.SIZING_PRODUCTION: "spark-sizing",
    WorkflowStage.SUBMITTING_PRODUCTION_PIPELINE: "submit-production",
    WorkflowStage.MONITORING_PRODUCTION_PIPELINE: "application-monitor",
    WorkflowStage.PROFILING_OUTPUT: "data-profile",
    WorkflowStage.FINALIZING: "archive",
    WorkflowStage.COMPLETED: "final-report",
    WorkflowStage.FAILED: "final-report",
}


def _test_output_target(
    data_type: Literal["file", "table"], output_target: str, request_id: str
) -> str:
    suffix = request_id.replace("-", "")[:12]
    separator = ".__test_" if data_type == "file" else "__test_"
    return f"{output_target.rstrip('/')}{separator}{suffix}"


def _state_outputs(state: WorkflowState) -> list[OutputSpec]:
    raw_outputs = state.get("outputs")
    if raw_outputs:
        return [OutputSpec.model_validate(output) for output in raw_outputs]
    legacy_target = state.get("output_target")
    if not legacy_target:
        return []
    return [
        OutputSpec(
            name="result",
            data_type=state.get("output_data_type", state.get("data_type", "table")),
            target=legacy_target,
            test_target=state.get("test_output_target"),
        )
    ]


def _resolve_output_targets(
    outputs: list[OutputSpec], request_id: str
) -> list[OutputSpec]:
    return [
        output.model_copy(
            update={
                "test_target": _test_output_target(
                    output.data_type, output.target, request_id
                )
            }
        )
        for output in outputs
    ]


def _outputs_payload(outputs: list[OutputSpec]) -> list[dict[str, Any]]:
    return [output.model_dump(mode="json") for output in outputs]


def _pipeline_feedback_history(state: WorkflowState) -> list[str]:
    history = list(state.get("pipeline_feedback_history", []))
    current = state.get("pipeline_feedback")
    if current and (not history or history[-1] != current):
        history.append(current)
    return history


def _record_pipeline_feedback(state: WorkflowState, message: str) -> dict[str, Any]:
    history = _pipeline_feedback_history(state)
    if not history or history[-1] != message:
        history.append(message)
    return {"pipeline_feedback": message, "pipeline_feedback_history": history}


def _format_pipeline_feedback(state: WorkflowState) -> str | None:
    history = _pipeline_feedback_history(state)
    if not history:
        return None
    previous = "\n".join(
        f"{index}. {message}" for index, message in enumerate(history[:-1], 1)
    )
    current = history[-1]
    if not previous:
        return f"Primary correction for this round:\n{current}"
    return (
        "Previously confirmed constraints that must not regress:\n"
        f"{previous}\nPrimary correction for this round:\n{current}"
    )


def _model_config(config: RunnableConfig, name: str) -> RunnableConfig:
    return {
        **config,
        "metadata": {**config.get("metadata", {}), "agent_name": name},
    }


def _current_stage(request_id: str) -> WorkflowStage:
    state = RequestState.get(request_id)
    if state is None:
        raise LookupError(f"request_id={request_id} was not found")
    return WorkflowStage(state["current_stage"])


def _move(request_id: str, target: WorkflowStage) -> dict[str, Any]:
    current = _current_stage(request_id)
    if current == target:
        state = RequestState.get(request_id)
        if state is None:
            raise LookupError(f"request_id={request_id} was not found")
        return state
    return RequestWorkflow.transition(request_id, target, STAGE_ASSIGNEES[target])


def _run(value: PipelineRun | dict[str, Any] | None) -> PipelineRun:
    return PipelineRun.model_validate(value or {})


def _fail(state: WorkflowState, message: str) -> dict[str, Any]:
    request_id = state.get("request_id")
    stage = WorkflowStage(state["stage"]) if state.get("stage") else None
    if request_id and stage not in {WorkflowStage.COMPLETED, WorkflowStage.FAILED}:
        _move(request_id, WorkflowStage.FAILED)
    return {
        "stage": WorkflowStage.FAILED.value,
        "error": WorkflowError(stage=stage, message=message).model_dump(mode="json"),
    }


def create_workflow(
    model: str | Any | None = None,
    *,
    pipeline_agent: Any = None,
    operator_builder: Any = None,
    checkpointer: Any = None,
):
    llm = resolve_model(model)
    pipeline = pipeline_agent or create_pipeline_agent(llm)
    builder = operator_builder or create_operator_builder_agent(llm)

    data_profile = create_data_profile_subgraph(llm)
    spark_sizing = create_spark_sizing_subgraph(llm)
    application_monitor = create_application_monitor_subgraph()

    def intake(state: WorkflowState, config: RunnableConfig) -> dict[str, Any]:
        existing_outputs = _state_outputs(state)
        result = invoke_structured(
            llm,
            IntakeResult,
            "Classify the user request. Use preview for read-only inspection and "
            "process for any transformation or persisted output. This is the only "
            "stage that may clarify requirements with the user. Use clarify only "
            "when information required to complete the task is missing and cannot "
            "reasonably be inferred from context, and identify all missing "
            "information at once. file means an HDFS file and table means a Hive "
            "table. process must identify the input source and processing target. "
            "When the user does not specify an output target, leave outputs empty; "
            "the workflow will assign a unique temp Hive table, so this does not "
            "require clarification. When the user specifies multiple outputs, "
            "outputs must contain multiple elements, each with a unique, concise "
            "logical name. A target may contain only one HDFS path or Hive table. "
            "After entering process, downstream agents must make reasonable "
            "assumptions about any remaining ambiguity and continue.\n"
            f"Existing data type: {state.get('data_type')}\n"
            f"Existing input: {state.get('data_source')}\n"
            "Existing outputs: "
            f"{json.dumps(_outputs_payload(existing_outputs), ensure_ascii=False)}\n"
            f"User request: {state['query']}",
            _model_config(config, "intake-llm"),
        )
        values = {
            field: getattr(result, field) or state.get(field)
            for field in ("data_type", "data_source")
        }
        outputs = result.outputs or existing_outputs
        outputs = [
            output.model_copy(update={"test_target": None}) for output in outputs
        ]
        missing = []
        if result.route in {IntentRoute.PREVIEW, IntentRoute.PROCESS}:
            if not values["data_type"]:
                missing.append("data type (HDFS file or Hive table)")
            if not values["data_source"]:
                missing.append("input data source")
        route = IntentRoute.CLARIFY if missing else result.route
        question = result.clarification_question
        if missing:
            question = "Please provide: " + ", ".join(missing) + "."
        update: dict[str, Any] = {"intent": route.value}
        update.update({key: value for key, value in values.items() if value})
        if outputs:
            update["outputs"] = _outputs_payload(outputs)
        if question:
            update["clarification_question"] = question
        if route == IntentRoute.FAIL:
            update["error"] = WorkflowError(
                message=result.summary or "The request cannot be processed"
            ).model_dump(mode="json")
        return update

    def route_intake(
        state: WorkflowState,
    ) -> Literal["clarify", "preview", "process", "fail"]:
        return IntentRoute(state["intent"]).value

    def clarify(state: WorkflowState) -> dict[str, Any]:
        answer = interrupt(
            {
                "question": state.get(
                    "clarification_question",
                    "Please provide the information required to complete the task.",
                )
            }
        )
        return {
            "query": f"{state['query']}\nAdditional user information: {answer}",
            "clarification_question": "",
        }

    def initialize_request(
        state: WorkflowState, config: RunnableConfig
    ) -> dict[str, Any]:
        dispatch_custom_event(
            "progress", "Initializing persistent request", config=config
        )
        thread_id = str(config.get("configurable", {}).get("thread_id", ""))
        if not thread_id:
            raise ValueError("thread_id is required")
        request_id = str(uuid5(NAMESPACE_URL, f"data-refiner:{thread_id}"))
        row = RequestState.get(request_id)
        if row is None:
            RequestWorkflow.create(
                user_id=state["user_id"],
                room_id=state["room_id"],
                current_stage=WorkflowStage.RECEIVED,
                user_request=state["query"],
                expected_next_stage=WorkflowStage.WORKSPACE_PREPARING,
                assignee=STAGE_ASSIGNEES[WorkflowStage.RECEIVED],
                request_id=request_id,
            )
        elif (
            row["user_id"] != state["user_id"] or row["user_request"] != state["query"]
        ):
            raise ValueError("thread_id already belongs to a different request")
        _move(request_id, WorkflowStage.WORKSPACE_PREPARING)
        outputs = _state_outputs(state)
        if not outputs:
            outputs = [
                OutputSpec(
                    name="result",
                    data_type="table",
                    target=f"temp.data_refiner_{request_id.replace('-', '')}",
                )
            ]
        outputs = _resolve_output_targets(outputs, request_id)
        return {
            "request_id": request_id,
            "stage": WorkflowStage.WORKSPACE_PREPARING.value,
            "operator_round": 1,
            "pipeline_feedback": "",
            "pipeline_feedback_history": [],
            "pipeline_revision": 0,
            "outputs": _outputs_payload(outputs),
            "test_run": PipelineRun().model_dump(mode="json"),
            "production_run": PipelineRun().model_dump(mode="json"),
        }

    def prepare_workspace(
        state: WorkflowState, config: RunnableConfig
    ) -> dict[str, Any]:
        dispatch_custom_event("progress", "Preparing user workspace", config=config)
        request_id = state["request_id"]
        if _current_stage(request_id) != WorkflowStage.PROFILING_INPUT:
            operations.prepare_user_workspace(
                state["user_id"],
                callbacks=CallbackManager.configure(config.get("callbacks")),
            )
            _move(request_id, WorkflowStage.PROFILING_INPUT)
        return {"stage": WorkflowStage.PROFILING_INPUT.value}

    def advance_profile(state: WorkflowState) -> dict[str, Any]:
        request_id = state.get("request_id")
        if not request_id:
            return {}
        stage = WorkflowStage(state["stage"])
        profile_error = state.get("profile_error")
        if profile_error:
            if stage == WorkflowStage.PROFILING_INPUT:
                return _fail(state, profile_error)
            environment = (
                "test" if stage == WorkflowStage.PROFILING_TEST_OUTPUT else "production"
            )
            feedback = _record_pipeline_feedback(state, profile_error)
            if (
                _run(state.get(f"{environment}_run")).submit_attempts
                >= MAX_PIPELINE_ATTEMPTS
            ):
                return {
                    **feedback,
                    **_fail(
                        state,
                        f"{environment} pipeline attempt limit exceeded; "
                        f"last profile error: {profile_error}",
                    ),
                }
            target = (
                WorkflowStage.PLANNING_TEST_PIPELINE
                if environment == "test"
                else WorkflowStage.PLANNING_PRODUCTION_PIPELINE
            )
            _move(request_id, target)
            return {
                "stage": target.value,
                **feedback,
                "pipeline_revision": 0,
                "failure_status": profile_error,
            }
        if stage == WorkflowStage.PROFILING_INPUT:
            if not state.get("input_metadata_id"):
                return _fail(state, "input metadata was not persisted")
            target = WorkflowStage.EVALUATING_OPERATORS
        elif stage == WorkflowStage.PROFILING_TEST_OUTPUT:
            if not state.get("test_output_metadata_id"):
                return _fail(state, "test output metadata was not persisted")
            target = WorkflowStage.VERIFYING_TEST_OUTPUT
        elif stage == WorkflowStage.PROFILING_OUTPUT:
            if not state.get("output_metadata_id"):
                return _fail(state, "output metadata was not persisted")
            target = WorkflowStage.FINALIZING
        else:
            return _fail(state, f"unexpected profile stage: {stage}")
        _move(request_id, target)
        return {"stage": target.value, "failure_status": ""}

    def route_profile(
        state: WorkflowState,
    ) -> Literal["report", "pipeline", "archive"]:
        if not state.get("request_id") or state.get("stage") == WorkflowStage.FAILED:
            return "report"
        if state["stage"] == WorkflowStage.EVALUATING_OPERATORS:
            return "pipeline"
        if state["stage"] == WorkflowStage.VERIFYING_TEST_OUTPUT:
            return "pipeline"
        if state["stage"] in {
            WorkflowStage.PLANNING_TEST_PIPELINE,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
        }:
            return "pipeline"
        return "archive"

    def run_pipeline_agent(
        state: WorkflowState, config: RunnableConfig
    ) -> dict[str, Any]:
        stage = WorkflowStage(state["stage"])
        task_by_stage = {
            WorkflowStage.EVALUATING_OPERATORS: PipelineTask.EVALUATE_OPERATORS,
            WorkflowStage.PLANNING_TEST_PIPELINE: PipelineTask.PLAN_TEST,
            WorkflowStage.VERIFYING_TEST_OUTPUT: PipelineTask.VERIFY_TEST_OUTPUT,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE: PipelineTask.PLAN_PRODUCTION,
            WorkflowStage.MONITORING_TEST_PIPELINE: PipelineTask.ANALYZE_TEST_FAILURE,
            WorkflowStage.MONITORING_PRODUCTION_PIPELINE: (
                PipelineTask.ANALYZE_PRODUCTION_FAILURE
            ),
        }
        task = task_by_stage.get(stage)
        if task is None:
            return _fail(state, f"Pipeline Agent cannot handle stage {stage}")
        environment = (
            "test"
            if task
            in {
                PipelineTask.PLAN_TEST,
                PipelineTask.VERIFY_TEST_OUTPUT,
                PipelineTask.ANALYZE_TEST_FAILURE,
            }
            else "production"
            if task
            in {
                PipelineTask.PLAN_PRODUCTION,
                PipelineTask.ANALYZE_PRODUCTION_FAILURE,
            }
            else None
        )
        run = _run(state.get(f"{environment}_run")) if environment else PipelineRun()
        round_number = state.get("operator_round", 1)
        if task == PipelineTask.ANALYZE_TEST_FAILURE:
            round_number += 1
        outputs = _state_outputs(state)
        context = PipelineAgentContext(
            user_id=state["user_id"],
            request_id=state["request_id"],
            task=task,
            operator_round=round_number,
            pipeline_category=environment,
            application_id=run.application_id,
            failure_status=state.get("failure_status") or None,
            validation_feedback=_format_pipeline_feedback(state),
            outputs=tuple(outputs),
        )
        message = (
            f"Run Pipeline Agent task: {task.value}\n"
            f"Round {round_number}\nUser request: {state['query']}"
            "\nStructured outputs: "
            f"{json.dumps(_outputs_payload(outputs), ensure_ascii=False)}"
        )
        if context.failure_status:
            message += f"\nFailure evidence: {context.failure_status}"
        if context.validation_feedback:
            message += f"\nMandatory planning feedback: {context.validation_feedback}"
        messages: list[Any] = [{"role": "user", "content": message}]
        attempt_context = context
        last_issue = ""
        for _attempt in range(2):
            result = pipeline.invoke(
                {"messages": messages},
                config=_model_config(config, "pipeline-agent"),
                context=attempt_context,
            )
            structured_response = result.get("structured_response")
            if structured_response is not None:
                try:
                    outcome = PipelineAgentResult.model_validate(structured_response)
                except ValidationError as exc:
                    last_issue = f"Structured result validation failed: {exc}"
                else:
                    allowed = PIPELINE_ACTIONS_BY_TASK[task]
                    if outcome.action in allowed:
                        return {
                            "pipeline_agent_result": outcome.model_dump(mode="json")
                        }
                    expected = ", ".join(sorted(action.value for action in allowed))
                    last_issue = (
                        f"Task {task.value} does not allow action "
                        f"{outcome.action.value}; allowed actions: {expected}"
                    )
            else:
                last_issue = "The previous call returned no structured result"
            messages = list(result.get("messages") or messages)
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"{last_issue}. All previously collected evidence is "
                        "preserved. Do not call any retrieval or subagent tools. "
                        "You must now call the PipelineAgentResult structured-result "
                        "tool exactly once."
                    ),
                }
            )
            attempt_context = replace(context, finalize_only=True)

        return {
            "pipeline_agent_result": PipelineAgentResult(
                action=PipelineAction.FAIL,
                summary=(
                    "Pipeline Agent returned no structured result twice in a row"
                    if last_issue == "The previous call returned no structured result"
                    else "Pipeline Agent returned no valid result twice in a row; "
                    f"{last_issue}"
                ),
            ).model_dump(mode="json")
        }

    def apply_pipeline_result(state: WorkflowState) -> dict[str, Any]:
        stage = WorkflowStage(state["stage"])
        result = PipelineAgentResult.model_validate(state["pipeline_agent_result"])
        request_id = state["request_id"]
        if result.action == PipelineAction.FAIL:
            return _fail(state, result.summary)

        if stage == WorkflowStage.EVALUATING_OPERATORS:
            if result.action not in {
                PipelineAction.PLAN_TEST_PIPELINE,
                PipelineAction.BUILD_OPERATOR,
            }:
                return _fail(state, f"invalid evaluation action: {result.action}")
            round_number = state.get("operator_round", 1)
            evaluation = operations.save_operator_evaluation(
                request_id, round_number, result.evaluation_report or ""
            )
            update: dict[str, Any] = {
                "operator_evaluation_id": evaluation["id"],
                "pipeline_feedback": "",
                "pipeline_revision": 0,
            }
            if result.action == PipelineAction.PLAN_TEST_PIPELINE:
                target = WorkflowStage.PLANNING_TEST_PIPELINE
            else:
                if round_number > MAX_OPERATOR_ROUNDS:
                    return {
                        **update,
                        **_fail(state, "operator build round limit exceeded"),
                    }
                reference = operations.save_operator_build_reference(
                    request_id, round_number, result.build_reference or ""
                )
                update["operator_build_reference_id"] = reference["id"]
                target = WorkflowStage.BUILDING_OPERATOR
            _move(request_id, target)
            update["stage"] = target.value
            return update

        if stage in {
            WorkflowStage.PLANNING_TEST_PIPELINE,
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE,
        }:
            test = stage == WorkflowStage.PLANNING_TEST_PIPELINE
            expected = (
                PipelineAction.SUBMIT_TEST_PIPELINE
                if test
                else PipelineAction.SIZE_PRODUCTION
            )
            if result.action != expected:
                return _fail(
                    state, f"invalid pipeline planning action: {result.action}"
                )
            validation = operations.validate_pipeline_config(
                request_id, result.pipeline_yaml or ""
            )
            environment: Literal["test", "production"] = (
                "test" if test else "production"
            )
            if validation.get("success"):
                validation = operations.validate_pipeline_outputs(
                    result.pipeline_yaml or "",
                    _outputs_payload(_state_outputs(state)),
                    environment,
                )
            if not validation.get("success"):
                revision = state.get("pipeline_revision", 0) + 1
                error = str(validation.get("error") or validation)
                feedback = _record_pipeline_feedback(state, error)
                if revision >= MAX_PIPELINE_REVISIONS:
                    return {
                        **feedback,
                        **_fail(state, f"pipeline validation failed: {error}"),
                    }
                return {**feedback, "pipeline_revision": revision}
            saved = operations.save_pipeline_config(
                request_id, environment, result.pipeline_yaml or ""
            )
            workspace = saved.get("workspace", {})
            database = saved.get("database", {})
            path = workspace.get("hdfs_path") or workspace.get("path")
            if not path:
                return _fail(state, "saved pipeline returned no HDFS path")
            run = _run(state.get(f"{environment}_run")).model_copy(
                update={"pipeline_id": database.get("id"), "pipeline_path": path}
            )
            target = (
                WorkflowStage.SUBMITTING_TEST_PIPELINE
                if test
                else WorkflowStage.SIZING_PRODUCTION
            )
            _move(request_id, target)
            return {
                f"{environment}_run": run.model_dump(mode="json"),
                "stage": target.value,
                "pipeline_feedback": "",
                "pipeline_revision": 0,
            }

        if stage == WorkflowStage.VERIFYING_TEST_OUTPUT:
            if result.action == PipelineAction.PLAN_PRODUCTION_PIPELINE:
                target = WorkflowStage.PLANNING_PRODUCTION_PIPELINE
                feedback: dict[str, Any] = {"pipeline_feedback": ""}
            elif result.action == PipelineAction.RETRY_TEST_PIPELINE:
                feedback = _record_pipeline_feedback(state, result.summary)
                if _run(state.get("test_run")).submit_attempts >= MAX_PIPELINE_ATTEMPTS:
                    return {
                        **feedback,
                        **_fail(
                            state,
                            "test pipeline attempt limit exceeded; "
                            f"last rejection: {result.summary}",
                        ),
                    }
                target = WorkflowStage.PLANNING_TEST_PIPELINE
            else:
                return _fail(
                    state, f"invalid test output validation action: {result.action}"
                )
            _move(request_id, target)
            return {
                "stage": target.value,
                **feedback,
                "pipeline_revision": 0,
            }

        if stage == WorkflowStage.MONITORING_TEST_PIPELINE:
            if result.action == PipelineAction.BUILD_OPERATOR:
                round_number = state.get("operator_round", 1) + 1
                evaluation = operations.save_operator_evaluation(
                    request_id, round_number, result.evaluation_report or ""
                )
                update = {
                    "operator_round": round_number,
                    "operator_evaluation_id": evaluation["id"],
                }
                if round_number > MAX_OPERATOR_ROUNDS:
                    return {
                        **update,
                        **_fail(state, "operator build round limit exceeded"),
                    }
                reference = operations.save_operator_build_reference(
                    request_id, round_number, result.build_reference or ""
                )
                _move(request_id, WorkflowStage.BUILDING_OPERATOR)
                return {
                    **update,
                    **_record_pipeline_feedback(state, result.summary),
                    "operator_build_reference_id": reference["id"],
                    "stage": WorkflowStage.BUILDING_OPERATOR.value,
                    "failure_status": "",
                }
            if result.action == PipelineAction.RETRY_TEST_PIPELINE:
                feedback = _record_pipeline_feedback(state, result.summary)
                if _run(state.get("test_run")).submit_attempts >= MAX_PIPELINE_ATTEMPTS:
                    failure = state.get("failure_status") or result.summary
                    return {
                        **feedback,
                        **_fail(
                            state,
                            "test pipeline attempt limit exceeded; "
                            f"last failure evidence: {failure}; "
                            f"agent analysis: {result.summary}",
                        ),
                    }
                _move(request_id, WorkflowStage.PLANNING_TEST_PIPELINE)
                return {
                    "stage": WorkflowStage.PLANNING_TEST_PIPELINE.value,
                    **feedback,
                    "pipeline_revision": 0,
                    "failure_status": "",
                }
            return _fail(state, f"invalid test failure action: {result.action}")

        if stage == WorkflowStage.MONITORING_PRODUCTION_PIPELINE:
            if result.action == PipelineAction.RETRY_PRODUCTION_PIPELINE:
                target = WorkflowStage.PLANNING_PRODUCTION_PIPELINE
            elif result.action == PipelineAction.RESIZE_PRODUCTION:
                target = WorkflowStage.SIZING_PRODUCTION
            else:
                return _fail(
                    state, f"invalid production failure action: {result.action}"
                )
            feedback = _record_pipeline_feedback(state, result.summary)
            if (
                _run(state.get("production_run")).submit_attempts
                >= MAX_PIPELINE_ATTEMPTS
            ):
                failure = state.get("failure_status") or result.summary
                return {
                    **feedback,
                    **_fail(
                        state,
                        "production pipeline attempt limit exceeded; "
                        f"last failure evidence: {failure}; "
                        f"agent analysis: {result.summary}",
                    ),
                }
            _move(request_id, target)
            return {
                "stage": target.value,
                **feedback,
                "pipeline_revision": 0,
                "failure_status": "",
            }

        return _fail(state, f"cannot apply Pipeline Agent result at stage {stage}")

    def route_pipeline_result(
        state: WorkflowState,
    ) -> Literal["pipeline", "build", "submit-test", "size", "report"]:
        routes = {
            WorkflowStage.EVALUATING_OPERATORS: "pipeline",
            WorkflowStage.PLANNING_TEST_PIPELINE: "pipeline",
            WorkflowStage.PLANNING_PRODUCTION_PIPELINE: "pipeline",
            WorkflowStage.VERIFYING_TEST_OUTPUT: "pipeline",
            WorkflowStage.BUILDING_OPERATOR: "build",
            WorkflowStage.SUBMITTING_TEST_PIPELINE: "submit-test",
            WorkflowStage.SIZING_PRODUCTION: "size",
            WorkflowStage.FAILED: "report",
        }
        return routes[WorkflowStage(state["stage"])]

    def build_operator(state: WorkflowState, config: RunnableConfig) -> dict[str, Any]:
        round_number = state.get("operator_round", 1)
        if round_number > MAX_OPERATOR_ROUNDS:
            return _fail(state, "operator build round limit exceeded")
        try:
            evaluation = operations.get_operator_evaluation_for_round(
                state["request_id"], round_number
            )
            reference = operations.get_operator_build_reference_for_round(
                state["request_id"], round_number
            )
        except ValueError as exc:
            return _fail(state, str(exc))
        if str(evaluation["id"]) != str(state.get("operator_evaluation_id")):
            return _fail(state, "current operator evaluation reference does not match")
        if str(reference["id"]) != str(state.get("operator_build_reference_id")):
            return _fail(state, "current operator build reference does not match")
        context = OperatorBuilderContext(
            user_id=state["user_id"],
            request_id=state["request_id"],
            operator_round=round_number,
            build_reference_id=reference["id"],
        )
        result = builder.invoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            "Build, test, and synchronize the single operator "
                            "specified by the current round's build reference."
                        ),
                    }
                ]
            },
            config=_model_config(config, "operator-builder-agent"),
            context=context,
        )
        outcome = OperatorBuildResult.model_validate(result.get("structured_response"))
        if (
            outcome.status != "succeeded"
            or outcome.test_exit_code != 0
            or not outcome.docs_synced
        ):
            return _fail(state, outcome.error or outcome.summary)
        try:
            dispatch_custom_event(
                "progress", "Synchronizing operator code to HDFS", config=config
            )
            operations.sync_workspace_for_request(state["request_id"])
            dispatch_custom_event(
                "completed", "Operator code synchronized to HDFS", config=config
            )
        except Exception as exc:
            return {
                **_fail(state, f"workspace synchronization failed: {exc}"),
                "workspace_synced": False,
            }
        try:
            dispatch_custom_event(
                "progress", "Synchronizing Python environment to HDFS", config=config
            )
            operations.sync_conda_env_for_request(state["request_id"])
            dispatch_custom_event(
                "completed", "Python environment synchronized to HDFS", config=config
            )
        except Exception as exc:
            return {
                **_fail(state, f"conda environment synchronization failed: {exc}"),
                "workspace_synced": True,
                "conda_env_synced": False,
            }
        _move(state["request_id"], WorkflowStage.EVALUATING_OPERATORS)
        return {
            "stage": WorkflowStage.EVALUATING_OPERATORS.value,
            "operator_round": round_number + 1,
            "workspace_synced": True,
            "conda_env_synced": True,
        }

    def route_builder(state: WorkflowState) -> Literal["pipeline", "report"]:
        return "report" if state["stage"] == WorkflowStage.FAILED else "pipeline"

    def route_submit(state: WorkflowState) -> Literal["monitor", "report"]:
        return "report" if state["stage"] == WorkflowStage.FAILED else "monitor"

    def submit(
        state: WorkflowState,
        config: RunnableConfig,
        environment: Literal["test", "production"],
    ) -> dict[str, Any]:
        run = _run(state.get(f"{environment}_run"))
        if not run.pipeline_path:
            return _fail(state, f"{environment} pipeline path is missing")
        dispatch_custom_event(
            "progress", f"Submitting {environment} pipeline", config=config
        )
        runtime_args = (
            operations.get_spark_runtime_config_for_request(state["request_id"])
            if environment == "production"
            else []
        )
        result = operations.submit_pipeline(
            state["request_id"], run.pipeline_path, runtime_args
        )
        application_id = result.get("application_id")
        if not application_id:
            return _fail(state, "pipeline submission returned no application_id")
        updated = run.model_copy(
            update={
                "application_id": application_id,
                "application_status": None,
                "submit_attempts": run.submit_attempts + 1,
                "metrics": [],
                "metrics_error": None,
            }
        )
        target = (
            WorkflowStage.MONITORING_TEST_PIPELINE
            if environment == "test"
            else WorkflowStage.MONITORING_PRODUCTION_PIPELINE
        )
        _move(state["request_id"], target)
        return {
            f"{environment}_run": updated.model_dump(mode="json"),
            "stage": target.value,
        }

    def submit_test(state: WorkflowState, config: RunnableConfig) -> dict[str, Any]:
        return submit(state, config, "test")

    def advance_sizing(state: WorkflowState) -> dict[str, Any]:
        if not state.get("spark_runtime_config_id"):
            return _fail(state, "Spark runtime configuration was not persisted")
        _move(state["request_id"], WorkflowStage.SUBMITTING_PRODUCTION_PIPELINE)
        return {"stage": WorkflowStage.SUBMITTING_PRODUCTION_PIPELINE.value}

    def route_sizing(state: WorkflowState) -> Literal["submit", "report"]:
        return "report" if state["stage"] == WorkflowStage.FAILED else "submit"

    def submit_production(
        state: WorkflowState, config: RunnableConfig
    ) -> dict[str, Any]:
        return submit(state, config, "production")

    def advance_monitor(state: WorkflowState) -> dict[str, Any]:
        outcome = MonitorOutcome(state["monitor_outcome"])
        if outcome == MonitorOutcome.TIMEOUT:
            return _fail(state, "Spark application monitoring timed out")
        if outcome == MonitorOutcome.FAILED:
            return {}
        target = (
            WorkflowStage.PROFILING_TEST_OUTPUT
            if state["stage"] == WorkflowStage.MONITORING_TEST_PIPELINE
            else WorkflowStage.PROFILING_OUTPUT
        )
        _move(state["request_id"], target)
        return {"stage": target.value}

    def route_monitor(
        state: WorkflowState,
    ) -> Literal["pipeline", "profile", "report"]:
        stage = WorkflowStage(state["stage"])
        if stage == WorkflowStage.FAILED:
            return "report"
        if stage in {
            WorkflowStage.PROFILING_TEST_OUTPUT,
            WorkflowStage.PROFILING_OUTPUT,
        }:
            return "profile"
        return "pipeline"

    def archive(state: WorkflowState, config: RunnableConfig) -> dict[str, Any]:
        dispatch_custom_event(
            "progress", "Archiving successful pipeline", config=config
        )
        operations.save_pipeline_case(
            state["request_id"],
            "user",
            task_summary=state["query"],
            processing_steps="Validated test and production workflow completed.",
        )
        _move(state["request_id"], WorkflowStage.COMPLETED)
        return {"stage": WorkflowStage.COMPLETED.value}

    def final_report(state: WorkflowState, config: RunnableConfig) -> dict[str, Any]:
        error = state.get("error")
        if error:
            error = WorkflowError.model_validate(error).message
        output_metadata = (
            operations.get_output_metadata_for_request(state["request_id"])
            if state.get("request_id") and state.get("output_metadata_id")
            else None
        )
        test_run = _run(state.get("test_run"))
        production_run = _run(state.get("production_run"))
        report = invoke_structured(
            llm,
            FinalReport,
            "Give the user a concise, accurate final result from the completed "
            "workflow state below. Do not claim artifacts that do not exist. On "
            "failure, state the failed stage and cause.\n"
            f"Request: {state['query']}\nStage: {state.get('stage', 'preview')}\n"
            f"Preview: {state.get('preview_result')}\n"
            f"request_id: {state.get('request_id')}\n"
            f"Input metadata: {state.get('input_metadata_id')}\n"
            f"Operator evaluation: {state.get('operator_evaluation_id')}\n"
            f"Operator workspace synchronized: {state.get('workspace_synced')}\n"
            f"Operator conda environment synchronized: "
            f"{state.get('conda_env_synced')}\n"
            f"Test run: {test_run.model_dump()}\n"
            f"Test run metrics: {[item.model_dump() for item in test_run.metrics]}\n"
            f"Test metric extraction error: {test_run.metrics_error}\n"
            f"Production run: {production_run.model_dump()}\n"
            "Production run metrics: "
            f"{[item.model_dump() for item in production_run.metrics]}\n"
            f"Production metric extraction error: {production_run.metrics_error}\n"
            "Production output targets: "
            f"{json.dumps(_outputs_payload(_state_outputs(state)), ensure_ascii=False)}"
            "\n"
            f"Output metadata ID: {state.get('output_metadata_id')}\n"
            f"Production output metadata: {output_metadata}\n"
            f"Pipeline feedback history: {_pipeline_feedback_history(state)}\n"
            f"Error: {error}\n"
            "When a synchronization status is True, do not claim that its "
            "environment was not synchronized. None means that synchronization was "
            "not performed in this run. When the user requests row counts or changes "
            "at each stage and production metrics exist, report those metrics; do "
            "not claim that row counts were unavailable. row_count_delta is the "
            "change relative to previous_step_name: a negative value is a decrease "
            "and a positive value is an increase. When uncounted steps occur between "
            "two count checkpoints, report only the cumulative change and do not "
            "attribute it to any single uncounted step.",
            _model_config(config, "final-report-llm"),
        )
        return {"final_answer": report.answer}

    graph = StateGraph(WorkflowState)
    graph.add_node("intake", intake)
    graph.add_node("clarify", clarify)
    graph.add_node("initialize-request", initialize_request)
    graph.add_node("prepare-workspace", prepare_workspace)
    graph.add_node("data-profile", data_profile)
    graph.add_node("advance-profile", advance_profile)
    graph.add_node("pipeline-agent", run_pipeline_agent)
    graph.add_node("apply-pipeline-result", apply_pipeline_result)
    graph.add_node("operator-builder-agent", build_operator)
    graph.add_node("submit-test", submit_test)
    graph.add_node("spark-sizing", spark_sizing)
    graph.add_node("advance-sizing", advance_sizing)
    graph.add_node("submit-production", submit_production)
    graph.add_node("application-monitor", application_monitor)
    graph.add_node("advance-monitor", advance_monitor)
    graph.add_node("archive", archive)
    graph.add_node("final-report", final_report)

    graph.add_edge(START, "intake")
    graph.add_conditional_edges(
        "intake",
        route_intake,
        {
            "clarify": "clarify",
            "preview": "data-profile",
            "process": "initialize-request",
            "fail": "final-report",
        },
    )
    graph.add_edge("clarify", "intake")
    graph.add_edge("initialize-request", "prepare-workspace")
    graph.add_edge("prepare-workspace", "data-profile")
    graph.add_edge("data-profile", "advance-profile")
    graph.add_conditional_edges(
        "advance-profile",
        route_profile,
        {
            "report": "final-report",
            "pipeline": "pipeline-agent",
            "archive": "archive",
        },
    )
    graph.add_edge("pipeline-agent", "apply-pipeline-result")
    graph.add_conditional_edges(
        "apply-pipeline-result",
        route_pipeline_result,
        {
            "pipeline": "pipeline-agent",
            "build": "operator-builder-agent",
            "submit-test": "submit-test",
            "size": "spark-sizing",
            "report": "final-report",
        },
    )
    graph.add_conditional_edges(
        "operator-builder-agent",
        route_builder,
        {"pipeline": "pipeline-agent", "report": "final-report"},
    )
    graph.add_conditional_edges(
        "submit-test",
        route_submit,
        {"monitor": "application-monitor", "report": "final-report"},
    )
    graph.add_edge("spark-sizing", "advance-sizing")
    graph.add_conditional_edges(
        "advance-sizing",
        route_sizing,
        {"submit": "submit-production", "report": "final-report"},
    )
    graph.add_conditional_edges(
        "submit-production",
        route_submit,
        {"monitor": "application-monitor", "report": "final-report"},
    )
    graph.add_edge("application-monitor", "advance-monitor")
    graph.add_conditional_edges(
        "advance-monitor",
        route_monitor,
        {
            "pipeline": "pipeline-agent",
            "profile": "data-profile",
            "report": "final-report",
        },
    )
    graph.add_edge("archive", "final-report")
    graph.add_edge("final-report", END)
    app = graph.compile(checkpointer=checkpointer)
    return app


__all__ = ["create_workflow"]
