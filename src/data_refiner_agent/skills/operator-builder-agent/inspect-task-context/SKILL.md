---
name: inspect-task-context
description: Inspect the build reference, operator catalog, installed packages, and relevant implementation, test, and documentation examples before writing an operator.
---

# Inspect Operator Build Context

1. Call `get_operator_build_reference_for_request`.
2. Call `get_operator_market_for_request` and
   `get_installed_packages_for_request`.
3. Select the existing operator from the same base class that most closely matches
   the requirement.
4. Call `get_operator_implementation`, `get_operator_test_implementation`, and
   `get_operator_document` for that operator.
5. Read additional examples only when the first example does not answer a specific
   interface question.

The inspection is complete only after the retrieved evidence establishes the
required behavior, base class conventions, import style, test style, and available
dependencies.
