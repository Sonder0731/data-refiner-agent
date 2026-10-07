---
name: failure-analysis
description: Diagnose a failed test or production Spark application and select the smallest evidence-supported recovery action.
---

# Analyze a Pipeline Failure

Use `inspect-task-context`. Treat the monitor response, failure logs, and production
Spark arguments in the task message as failure evidence. A terminal status name
alone is not enough for a diagnosis.

For test failures:

- Return `retry-test-pipeline` when existing operators can correct the YAML,
  parameters, step order, or operator selection.
- By default, treat `UNRESOLVED_COLUMN`, `TABLE_OR_VIEW_NOT_FOUND`, mismatched SQL
  `UNION` column counts, a node missing `op_name`, missing required parameters, and
  null values passed to a downstream operator as issues that pipeline
  configuration, step order, or an earlier filter can correct. Use the input
  metadata, failed pipeline, and operator documentation to give a specific
  correction and return `retry-test-pipeline`. Build an operator only when the
  evidence clearly shows that existing operators cannot express the correction.
- Return `build-operator` only when the failure evidence proves an operator
  capability or compatibility gap. Include a new evaluation report and a complete
  build reference.
- Use `fail` only when an external resource or service is permanently unavailable,
  required credentials are truly missing and cannot be injected at runtime, or the
  evidence proves that neither replanning nor building an operator can recover.
  Do not use `fail` merely because the correction must preserve input fields, add
  null filtering, align a SQL projection, or move temporary-view registration.

For production failures:

- Return `retry-production-pipeline` when the pipeline configuration is defective.
- Return `resize-production` when resources are exhausted or the Spark resource
  configuration is clearly insufficient.
- Return `fail` for other cases.

Do not reuse a stale build reference or infer an operator gap from an unexplained
application status. Before returning a retry, check the failed pipeline and all
accumulated feedback and identify at least one concrete configuration change. Do
not resubmit unchanged YAML that caused the same error.
