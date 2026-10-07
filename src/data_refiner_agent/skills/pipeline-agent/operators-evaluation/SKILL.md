---
name: operators-evaluation
description: Evaluate whether the current operator catalog fully covers the requested Data Refiner pipeline and return an evidence-based report.
---

# Evaluate Operators

Use `inspect-task-context` first. Match every required processing step to an
operator and confirm that its documentation proves the required behavior.

The evaluation report must contain:

```text
# Operator Evaluation Report, Round x
Evaluation result: reuse existing operators/operator capability missing
## Required Pipeline
### Step x
Operator name: ...
Exists: yes/no
Documentation evidence and role: ...
## Missing Capability
...
```

When all steps are covered, return action `plan-test-pipeline` with the report.
When a capability is missing, identify only the first missing operator needed in
this round, use `new-operator-reference`, and return action `build-operator` with
the report and build reference.
