---
name: test-output-validation
description: Determine from the actual test output profile whether the pipeline satisfies the original user request, then approve production, replan the test, or fail.
---

# Validate Test Output

Use `inspect-task-context` first to read the input metadata, test pipeline, and
test output metadata. Use the original user request as the acceptance criteria.
Check the actual output fields, types, sample values, filtering or transformation
results, deduplication or aggregation effects, and any row limit explicitly
requested by the user. A successful Spark application or static YAML validation
does not constitute business acceptance.

Validation covers the bounded sample and processing logic of the test pipeline.
Judge it from the deterministic test pipeline configuration, actual output schema,
and available samples. A profile showing only a subset of samples does not mean
the output is invalid. Unless the user explicitly requests full-data statistics
during testing, do not require evidence such as null counts or constraint-violation
counts over the full source dataset, and do not stop because those statistics are missing.

- Return `plan-production-pipeline` when the available evidence shows no actual
  conflict and the pipeline configuration, output schema, and samples jointly
  support the request. When evidence is limited, make the most reasonable
  judgment, record key assumptions in `summary`, and continue without asking the
  user for more information.
- Return `retry-test-pipeline` when the actual output does not satisfy the request
  but changing YAML, operator parameters, operator order, or operator selection can
  correct it. `summary` must list the actual mismatch, the corresponding user
  requirement, and a specific correction for the next `plan-test` task.
- Return `fail` only for a clear technical or capability blocker that replanning
  cannot recover from, and explain the blocker.

Do not infer output correctness from Spark success alone. Do not require full-data
proof that a bounded sample profile cannot provide.
