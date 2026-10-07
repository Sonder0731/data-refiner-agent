import ast
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

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
    SparkRuntimeConfig,
    WorkspaceMapping,
)

CLASS_METHODS = {
    DataMetadata: {
        "create",
        "upsert",
        "get",
        "list_by_request_id",
        "update_category",
        "update_content",
        "delete",
    },
    History: {"create", "get", "update_content", "append", "delete"},
    NewOperatorBuildReference: {
        "create",
        "upsert",
        "get",
        "get_latest",
        "list_by_request_id",
        "update_rounds",
        "update_content",
        "delete",
    },
    NewOperatorDocs: {
        "create",
        "get",
        "list_by_request_id",
        "update_content",
        "delete",
    },
    OperatorsEvaluation: {
        "create",
        "upsert",
        "get",
        "list_by_request_id",
        "update_rounds",
        "update_content",
        "delete",
    },
    Pipeline: {
        "create",
        "upsert",
        "get",
        "list_by_request_id",
        "update_category",
        "update_content",
        "delete",
    },
    PipelineCase: {
        "create",
        "get",
        "list_by_belong",
        "list_by_user_id",
        "update_original_user_query",
        "update_belong",
        "update_user_id",
        "update_owner",
        "update_input_data_desc",
        "update_pipeline",
        "update_task_summary",
        "update_processing_steps",
        "update_output_data_description",
        "update_spark_runtime_config",
        "delete",
    },
    PipelineRetrievalIndex: {
        "create",
        "get",
        "list_by_pipeline_case_id",
        "search_similar",
        "update_pipeline_case_id",
        "update_retrieval_text",
        "update_embedding",
        "delete",
    },
    RequestState: {
        "create",
        "get",
        "get_user_id",
        "update_user_id",
        "update_user_name",
        "update_room_id",
        "update_current_stage",
        "compare_and_set_current_stage",
        "update_user_request",
        "update_input_data_metadata_id",
        "update_operators_evaluation_id",
        "update_new_operator_build_reference_id",
        "update_new_operator_docs",
        "update_test_pipeline_id",
        "update_pipeline_id",
        "update_test_output_data_metadata_id",
        "update_output_data_metadata_id",
        "update_history_id",
        "update_active_handoff",
        "delete",
    },
    SparkRuntimeConfig: {
        "create",
        "get",
        "list_by_request_id",
        "update_request_id",
        "update_config",
        "delete",
    },
    WorkspaceMapping: {
        "create",
        "upsert",
        "get",
        "get_by_user_name",
        "update_user_name",
        "update_container_name",
        "update_api_url",
        "delete",
    },
}


class DatabaseClassContractTest(unittest.TestCase):
    def test_data_metadata_supports_test_output(self) -> None:
        self.assertIn("test_output", DataMetadata.CATEGORIES)

    def test_every_table_class_has_expected_methods(self) -> None:
        for table_class, methods in CLASS_METHODS.items():
            with self.subTest(table_class=table_class.__name__):
                self.assertEqual(
                    {name for name in methods if hasattr(table_class, name)},
                    methods,
                )

    @patch("data_refiner_agent_tools.database.data_metadata.fetch_one")
    def test_data_metadata_update_content_uses_full_unique_key(
        self,
        fetch_one,
    ) -> None:
        fetch_one.return_value = {"id": 1, "content": "updated"}
        request_id = UUID("a3988658-859c-4545-8467-26e12d6547d0")

        row = DataMetadata.update_content(
            request_id=request_id,
            category="input",
            content="updated",
        )

        parameters = fetch_one.call_args.args[1]
        self.assertEqual(parameters, ("updated", request_id, "input"))
        self.assertEqual(row["content"], "updated")

    @patch("data_refiner_agent_tools.database.data_metadata.fetch_one")
    def test_update_missing_row_raises_lookup_error(self, fetch_one) -> None:
        fetch_one.return_value = None
        with self.assertRaises(LookupError):
            DataMetadata.update_content(
                request_id="a3988658-859c-4545-8467-26e12d6547d0",
                category="input",
                content="updated",
            )

    def test_embedding_dimension_is_validated(self) -> None:
        with self.assertRaises(ValueError):
            PipelineRetrievalIndex.create(1, "query", [0.0])

    @patch("data_refiner_agent_tools.database.pipeline_retrieval_index.fetch_all")
    def test_pipeline_retrieval_index_search_returns_top_five(
        self,
        fetch_all,
    ) -> None:
        fetch_all.return_value = [{"pipeline_case_id": 1, "similarity": 1.0}]
        embedding = [0.0] * 384

        result = PipelineRetrievalIndex.search_similar(embedding)

        query, parameters = fetch_all.call_args.args
        self.assertIn("ORDER BY similarity DESC", query)
        self.assertEqual(parameters, (json.dumps(embedding), 5))
        self.assertEqual(result, fetch_all.return_value)

    def test_package_has_no_relative_imports(self) -> None:
        package = Path(__file__).parents[1] / "src" / "data_refiner_agent_tools"
        relative_imports = []
        for path in package.rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            relative_imports.extend(
                (path.name, node.lineno)
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.level
            )
        self.assertEqual(relative_imports, [])


if __name__ == "__main__":
    unittest.main()
