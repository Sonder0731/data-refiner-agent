---
name: inspect-task-context
description: Read the current input metadata, operator catalog and documentation, evaluation report, examples, pipelines, and failure evidence required for a Pipeline Agent task.
---

# Inspect Task Context

Read only the evidence required for the current task:

1. When evaluating operators, call `get_input_metadata_for_request` and
   `get_operator_market_for_request`, then use `get_operator_document` to read the
   relevant operator documentation.
2. When planning a pipeline, also call `get_latest_operator_evaluation` and
   `get_pipeline_example`. Test planning must call
   `search_similar_pipeline_cases`; production planning must call
   `get_test_pipeline`.
3. When validating test output, call `get_input_metadata_for_request`,
   `get_test_pipeline`, and `get_test_output_metadata_for_request`. Compare the
   actual output with every part of the original user request. Read the relevant
   operator documentation only when needed to explain a step's behavior.
4. When analyzing a failure, use the monitor response, failure logs, and production
   Spark arguments in the task message, and read the failed pipeline.

Do not speculate about an operator interface or failure cause when a tool can read
the relevant evidence.
