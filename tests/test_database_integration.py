import os
import unittest
from uuid import uuid4

from psycopg.rows import dict_row

from data_refiner_agent_tools import (
    DataMetadata,
    History,
    NewOperatorBuildReference,
    NewOperatorDocs,
    OperatorsEvaluation,
    Pipeline,
    PipelineCase,
    PipelineRetrievalIndex,
    RequestState,
    RequestWorkflow,
    SparkRuntimeConfig,
    WorkflowStage,
    WorkspaceMapping,
    connect_database,
)


@unittest.skipUnless(
    os.getenv("DATABASE_INTEGRATION_TEST") == "1",
    "set DATABASE_INTEGRATION_TEST=1 to run PostgreSQL integration checks",
)
class DatabaseIntegrationTest(unittest.TestCase):
    def test_table_classes_crud_in_rolled_back_transaction(self) -> None:
        request_id = uuid4()
        user_id = f"@test-{request_id}:example.test"
        pipeline_case_id = None

        class RollbackTest(Exception):
            pass

        with connect_database(row_factory=dict_row) as connection:
            try:
                with connection.transaction():
                    history = History.create(
                        request_id,
                        [{"stage": "created"}],
                        connection=connection,
                    )
                    RequestState.create(
                        request_id,
                        user_id,
                        f"test-{request_id}",
                        "room",
                        "created",
                        "request",
                        history_id=history["id"],
                        connection=connection,
                    )
                    RequestState.update_current_stage(
                        request_id,
                        "updated",
                        connection=connection,
                    )

                    metadata = DataMetadata.create(
                        request_id,
                        "input",
                        "before",
                        connection=connection,
                    )
                    DataMetadata.update_content(
                        request_id,
                        "input",
                        "after",
                        connection=connection,
                    )
                    RequestState.update_input_data_metadata_id(
                        request_id,
                        metadata["id"],
                        connection=connection,
                    )

                    evaluation = OperatorsEvaluation.create(
                        request_id,
                        1,
                        "evaluation",
                        connection=connection,
                    )
                    OperatorsEvaluation.update_content(
                        request_id,
                        1,
                        "updated evaluation",
                        connection=connection,
                    )
                    RequestState.update_operators_evaluation_id(
                        request_id,
                        evaluation["id"],
                        connection=connection,
                    )

                    reference = NewOperatorBuildReference.create(
                        request_id,
                        1,
                        "reference",
                        connection=connection,
                    )
                    NewOperatorBuildReference.update_content(
                        request_id,
                        1,
                        "updated reference",
                        connection=connection,
                    )
                    RequestState.update_new_operator_build_reference_id(
                        request_id,
                        reference["id"],
                        connection=connection,
                    )

                    docs = NewOperatorDocs.create(
                        request_id,
                        "docs",
                        connection=connection,
                    )
                    NewOperatorDocs.update_content(
                        docs["id"],
                        "updated docs",
                        connection=connection,
                    )
                    RequestState.update_new_operator_docs(
                        request_id,
                        [docs["id"]],
                        connection=connection,
                    )

                    pipeline = Pipeline.create(
                        request_id,
                        "test",
                        "pipeline",
                        connection=connection,
                    )
                    Pipeline.update_content(
                        request_id,
                        "test",
                        "updated pipeline",
                        connection=connection,
                    )
                    RequestState.update_test_pipeline_id(
                        request_id,
                        pipeline["id"],
                        connection=connection,
                    )

                    pipeline_case = PipelineCase.create(
                        "original",
                        "data-refiner",
                        None,
                        "input",
                        "pipeline",
                        "summary",
                        "steps",
                        "output",
                        "config",
                        connection=connection,
                    )
                    pipeline_case_id = pipeline_case["id"]
                    PipelineCase.update_task_summary(
                        pipeline_case_id,
                        "updated summary",
                        connection=connection,
                    )
                    retrieval = PipelineRetrievalIndex.create(
                        pipeline_case_id,
                        "query",
                        [1.0] + [0.0] * 383,
                        connection=connection,
                    )
                    similar_cases = PipelineRetrievalIndex.search_similar(
                        [1.0] + [0.0] * 383,
                        connection=connection,
                    )
                    self.assertEqual(
                        similar_cases[0]["pipeline_case_id"],
                        pipeline_case_id,
                    )
                    PipelineRetrievalIndex.update_retrieval_text(
                        retrieval["id"],
                        "updated query",
                        connection=connection,
                    )

                    runtime_config_id = f"runtime-{request_id}"
                    SparkRuntimeConfig.create(
                        runtime_config_id,
                        str(request_id),
                        "spark.executor.instances=2",
                        connection=connection,
                    )
                    SparkRuntimeConfig.update_config(
                        runtime_config_id,
                        "spark.executor.instances=4",
                        connection=connection,
                    )

                    WorkspaceMapping.create(
                        user_id,
                        f"test-{request_id}",
                        f"container-{request_id}",
                        "http://workspace:8000",
                        connection=connection,
                    )
                    WorkspaceMapping.update_api_url(
                        user_id,
                        "http://updated-workspace:8000",
                        connection=connection,
                    )

                    self.assertEqual(
                        RequestState.get(request_id, connection=connection)[
                            "current_stage"
                        ],
                        "updated",
                    )
                    self.assertEqual(
                        DataMetadata.get(
                            request_id,
                            "input",
                            connection=connection,
                        )["content"],
                        "after",
                    )

                    PipelineRetrievalIndex.delete(
                        retrieval["id"],
                        connection=connection,
                    )
                    SparkRuntimeConfig.delete(
                        runtime_config_id,
                        connection=connection,
                    )
                    PipelineCase.delete(
                        pipeline_case_id,
                        connection=connection,
                    )
                    WorkspaceMapping.delete(user_id, connection=connection)
                    RequestState.delete(request_id, connection=connection)
                    History.delete(request_id, connection=connection)
                    DataMetadata.delete(
                        request_id,
                        "input",
                        connection=connection,
                    )
                    OperatorsEvaluation.delete(
                        request_id,
                        1,
                        connection=connection,
                    )
                    NewOperatorBuildReference.delete(
                        request_id,
                        1,
                        connection=connection,
                    )
                    NewOperatorDocs.delete(docs["id"], connection=connection)
                    Pipeline.delete(
                        request_id,
                        "test",
                        connection=connection,
                    )
                    self.assertIsNone(
                        RequestState.get(request_id, connection=connection)
                    )
                    raise RollbackTest
            except RollbackTest:
                pass

        self.assertIsNone(RequestState.get(request_id))
        if pipeline_case_id is not None:
            self.assertIsNone(PipelineCase.get(pipeline_case_id))

    def test_request_workflow_create_and_transition(self) -> None:
        request_id = uuid4()
        try:
            created = RequestWorkflow.create(
                user_id=f"@workflow-{request_id}:example.test",
                room_id="room",
                current_stage=WorkflowStage.RECEIVED,
                user_request="request",
                expected_next_stage=WorkflowStage.WORKSPACE_PREPARING,
                assignee="worker",
                request_id=request_id,
            )
            transitioned = RequestWorkflow.transition(
                request_id=request_id,
                next_stage=WorkflowStage.WORKSPACE_PREPARING,
                assignee="worker",
            )

            self.assertEqual(created["current_stage"], WorkflowStage.RECEIVED)
            self.assertEqual(
                transitioned["current_stage"], WorkflowStage.WORKSPACE_PREPARING
            )
            self.assertEqual(
                History.get(request_id)["content"][-1]["stage"],
                WorkflowStage.WORKSPACE_PREPARING,
            )
        finally:
            with connect_database(row_factory=dict_row) as connection:
                if RequestState.get(request_id, connection=connection):
                    RequestState.delete(request_id, connection=connection)
                if History.get(request_id, connection=connection):
                    History.delete(request_id, connection=connection)
