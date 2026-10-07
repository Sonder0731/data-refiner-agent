---
name: pipeline-planning
description: Plan a Data Refiner test or production pipeline for workflow validation using only confirmed operators.
---

# Plan a Pipeline

Use `inspect-task-context` first. Before drafting, call `get_operator_document`
during this invocation for every operator confirmed by the latest evaluation. The
documentation set is complete only when every operator planned for the YAML has a
corresponding tool result. Do not retrieve the same operator document again after
a successful result. Once the documentation set is complete, return the structured
result on the next turn.

Draft the smallest configuration that satisfies the user request using the input
metadata and retrieved documentation. Prefer operator-specific and format-specific
parameters that directly express the required behavior. Configure CSV parsing
behavior such as headers through the reader's `options`. Use `renames` only when
source column names must actually change.

Accumulated planning feedback in the task message is an ordered set of hard
constraints. Before planning, check every item under "Previously confirmed
constraints that must not regress" and "Primary correction for this round." The
new version must satisfy all items. It is complete only when the current issue is
fixed and no previously fixed issue has returned.

When the user explicitly asks to receive a scalar or small aggregate result such
as a count, mean, or maximum directly, set `show: true` on the processing operator
that produces that final result. Preserve it in both test and production
pipelines. Display only bounded results. Persist unbounded detail data and use the
output profile to provide bounded samples.

When the user asks for filtering, deduplication, retained-row counts, or row-count
changes at each stage, set `count: true` on the read step and every processing step
whose effect must be attributed independently. Preserve these settings in both
test and production pipelines. `count` may appear anywhere in the pipeline. Steps
without `count` between adjacent count checkpoints yield only the cumulative
change between those checkpoints; do not attribute that change to any single
step. Count the last processing step before the writer when its row count must be
reported. The writer itself does not need a duplicate count.

The structured `outputs` from runtime context are authoritative. Each element
represents one independent output and requires exactly one writer matching its
data type. Do not reconstruct or rewrite targets from the original request. For a
table target, use the Hive table writer confirmed by retrieved documentation. Use
overwrite mode for targets matching `temp.data_refiner_*` so retries are
idempotent.

For a test pipeline:

1. Call `search_similar_pipeline_cases` before drafting.
2. Limit the reader to at most 100 records unless the user explicitly requests a
   lower limit.
3. Each writer must use its output's `test_target` in overwrite mode. Test output
   planning is complete when every `test_target` appears exactly once.

The test pipeline validates processing logic, output shape, and executability on a
bounded sample. It does not prove that the full source dataset satisfies quality
constraints. The accepted production pipeline handles the full dataset.

For a production pipeline:

1. After the test pipeline succeeds, start from the result of `get_test_pipeline`.
2. Replace each writer target from its output's `test_target` to `target`. Restore
   the user-requested production write mode when overwrite was used only for the
   test. Continue using overwrite for system-assigned `temp.data_refiner_*`
   targets.
3. Remove only artificial test limits. Preserve every limit explicitly requested
   by the user.

When test-output validation feedback is present, correct every reported mismatch.
Do not return the previous test pipeline unchanged.

When the workflow returns validation feedback, correct every item and return the
complete YAML. Before returning, retrieve any operator documentation still missing
for the final YAML and revise the configuration from that evidence. Once the
documentation set is complete, return the YAML. The workflow node validates,
persists, and submits it.

`spark_sql_executor` can read only tables or temporary views that already exist.
To query the previous DataFrame, set `temp_view_name` on the preceding producer
node and reference that name in the later SQL. A SQL node's own `temp_view_name`
registers only the SQL result and is unavailable to that same SQL statement. After
validation fails, change the configuration that caused the error. Do not resubmit
the same YAML or the same invalid configuration.
