from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, Field, model_validator

from data_refiner_agent_tools import WorkflowStage

OperatorType = Literal[
    "builtin",
    "deduplicator",
    "filter",
    "mapper",
    "other",
    "reader",
    "reducer",
    "sampler",
    "writer",
]


class IntentRoute(StrEnum):
    CLARIFY = "clarify"
    PREVIEW = "preview"
    PROCESS = "process"
    FAIL = "fail"


class MonitorOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMEOUT = "timeout"


class PipelineTask(StrEnum):
    EVALUATE_OPERATORS = "evaluate-operators"
    PLAN_TEST = "plan-test"
    VERIFY_TEST_OUTPUT = "verify-test-output"
    PLAN_PRODUCTION = "plan-production"
    ANALYZE_TEST_FAILURE = "analyze-test-failure"
    ANALYZE_PRODUCTION_FAILURE = "analyze-production-failure"


class PipelineAction(StrEnum):
    BUILD_OPERATOR = "build-operator"
    PLAN_TEST_PIPELINE = "plan-test-pipeline"
    SUBMIT_TEST_PIPELINE = "submit-test-pipeline"
    PLAN_PRODUCTION_PIPELINE = "plan-production-pipeline"
    SIZE_PRODUCTION = "size-production"
    RETRY_TEST_PIPELINE = "retry-test-pipeline"
    RETRY_PRODUCTION_PIPELINE = "retry-production-pipeline"
    RESIZE_PRODUCTION = "resize-production"
    FAIL = "fail"


class OutputSpec(BaseModel):
    name: str = Field(min_length=1)
    data_type: Literal["file", "table"]
    target: str = Field(min_length=1)
    test_target: str | None = None

    @model_validator(mode="after")
    def require_one_target(self) -> "OutputSpec":
        for value in (self.target, self.test_target):
            if value and (";" in value or "；" in value or value.count("://") > 1):
                raise ValueError("an output target must contain exactly one location")
        if self.data_type == "file" and not self.target.startswith("hdfs://"):
            raise ValueError("file output targets must be HDFS URIs")
        return self


@dataclass(frozen=True)
class PipelineAgentContext:
    user_id: str
    request_id: str
    task: PipelineTask
    operator_round: int = 1
    pipeline_category: Literal["test", "production"] | None = None
    application_id: str | None = None
    failure_status: str | None = None
    validation_feedback: str | None = None
    outputs: tuple[OutputSpec, ...] = ()
    finalize_only: bool = False

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ValueError("request_id is required")
        if self.operator_round < 1:
            raise ValueError("operator_round must be positive")
        if self.task == PipelineTask.PLAN_TEST and self.pipeline_category != "test":
            raise ValueError("plan-test requires pipeline_category=test")
        if (
            self.task == PipelineTask.VERIFY_TEST_OUTPUT
            and self.pipeline_category != "test"
        ):
            raise ValueError("verify-test-output requires pipeline_category=test")
        if (
            self.task == PipelineTask.PLAN_PRODUCTION
            and self.pipeline_category != "production"
        ):
            raise ValueError("plan-production requires pipeline_category=production")
        if (
            self.task
            in {
                PipelineTask.ANALYZE_TEST_FAILURE,
                PipelineTask.ANALYZE_PRODUCTION_FAILURE,
            }
            and not self.application_id
        ):
            raise ValueError("failure analysis requires application_id")

    def require_request_id(self) -> str:
        return self.request_id

    def require_application_id(self) -> str:
        if not self.application_id:
            raise ValueError("application_id is required")
        return self.application_id


@dataclass(frozen=True)
class OperatorBuilderContext:
    user_id: str
    request_id: str
    operator_round: int = 1
    build_reference_id: int | str | None = None

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ValueError("request_id is required")
        if self.operator_round < 1:
            raise ValueError("operator_round must be positive")

    def require_request_id(self) -> str:
        return self.request_id


class IntakeResult(BaseModel):
    route: IntentRoute
    summary: str = ""
    clarification_question: str | None = None
    data_type: Literal["file", "table"] | None = None
    data_source: str | None = None
    outputs: list[OutputSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_distinct_outputs(self) -> "IntakeResult":
        names = [output.name for output in self.outputs]
        targets = [(output.data_type, output.target) for output in self.outputs]
        if len(names) != len(set(names)):
            raise ValueError("output names must be unique")
        if len(targets) != len(set(targets)):
            raise ValueError("output targets must be unique")
        return self


class FinalReport(BaseModel):
    answer: str


class DataDescription(BaseModel):
    description: str


class PipelineAgentResult(BaseModel):
    action: PipelineAction
    summary: str
    evaluation_report: str | None = None
    build_reference: str | None = None
    pipeline_yaml: str | None = None

    @model_validator(mode="after")
    def require_action_artifacts(self) -> "PipelineAgentResult":
        if self.action == PipelineAction.BUILD_OPERATOR:
            if not self.evaluation_report or not self.build_reference:
                raise ValueError(
                    "build-operator requires evaluation_report and build_reference"
                )
        elif (
            self.action == PipelineAction.PLAN_TEST_PIPELINE
            and not self.evaluation_report
        ):
            raise ValueError("plan-test-pipeline requires evaluation_report")
        elif (
            self.action
            in {
                PipelineAction.SUBMIT_TEST_PIPELINE,
                PipelineAction.SIZE_PRODUCTION,
            }
            and not self.pipeline_yaml
        ):
            raise ValueError(f"{self.action} requires pipeline_yaml")
        return self


class SparkSizingResult(BaseModel):
    runtime_args: list[str]
    rationale: str = ""


class OperatorBuildResult(BaseModel):
    status: Literal["succeeded", "failed"]
    summary: str
    operator_name: str | None = None
    operator_type: OperatorType | None = None
    implementation_path: str | None = None
    test_path: str | None = None
    test_exit_code: int | None = None
    docs_synced: bool = False
    error: str | None = None


class PipelineMetric(BaseModel):
    step_index: int = Field(ge=0)
    step_name: str
    operator_name: str
    row_count: int = Field(ge=0)
    previous_step_name: str | None = None
    row_count_delta: int | None = None


class PipelineRun(BaseModel):
    pipeline_id: int | str | None = None
    pipeline_path: str | None = None
    application_id: str | None = None
    application_status: str | None = None
    submit_attempts: int = Field(default=0, ge=0)
    metrics: list[PipelineMetric] = Field(default_factory=list)
    metrics_error: str | None = None


class PipelineRunState(TypedDict, total=False):
    pipeline_id: int | str | None
    pipeline_path: str | None
    application_id: str | None
    application_status: str | None
    submit_attempts: int
    metrics: list[dict[str, Any]]
    metrics_error: str | None


class WorkflowError(BaseModel):
    stage: WorkflowStage | None = None
    message: str


class WorkflowState(TypedDict, total=False):
    query: str
    user_id: str
    room_id: str
    request_id: str
    stage: WorkflowStage
    intent: IntentRoute
    data_type: Literal["file", "table"]
    data_source: str
    outputs: list[dict[str, Any]]
    # Legacy checkpoint fields. New workflow logic reads outputs instead.
    output_data_type: Literal["file", "table"]
    output_target: str
    test_output_target: str
    clarification_question: str
    preview_result: str
    input_metadata_id: int | str
    test_output_metadata_id: int | str
    output_metadata_id: int | str
    operator_round: int
    operator_evaluation_id: int | str
    operator_build_reference_id: int | str
    workspace_synced: bool
    conda_env_synced: bool
    pipeline_agent_result: dict[str, Any]
    pipeline_feedback: str
    pipeline_feedback_history: list[str]
    pipeline_revision: int
    test_run: PipelineRunState
    production_run: PipelineRunState
    spark_runtime_config_id: int | str
    monitor_outcome: MonitorOutcome
    failure_status: str
    error: dict[str, Any]
    final_answer: str
    profile_error: str


class DataProfileState(WorkflowState, total=False):
    raw_profile: str
    profile_description: str


class SparkSizingState(WorkflowState, total=False):
    sizing_context: str
    cluster_resources: dict[str, Any]
    runtime_args: list[str]
    sizing_rationale: str
    sizing_error: str
    sizing_attempt: int


class MonitorState(WorkflowState, total=False):
    monitor_environment: Literal["test", "production"]
    monitored_application_id: str


__all__ = [
    "DataDescription",
    "DataProfileState",
    "FinalReport",
    "IntakeResult",
    "IntentRoute",
    "MonitorOutcome",
    "MonitorState",
    "OperatorBuildResult",
    "OperatorBuilderContext",
    "OutputSpec",
    "PipelineAction",
    "PipelineAgentContext",
    "PipelineAgentResult",
    "PipelineMetric",
    "PipelineRun",
    "PipelineRunState",
    "PipelineTask",
    "SparkSizingResult",
    "SparkSizingState",
    "WorkflowError",
    "WorkflowStage",
    "WorkflowState",
]
