---
name: build-operator
description: Implement, test, repair, and document one missing operator in the requesting user's workspace.
---

# Build an Operator

Use `inspect-task-context` first.

1. Use `install_workspace_package` only when the required package is not already
   installed and the build reference explicitly requires it. Pass only the package
   name in `package_name` and pass an exact version separately in `version`.
   Continue only when the tool returns `status=succeeded`.
2. Implement one operator according to the build reference and the retrieved base
   class conventions. Pass the complete source code to
   `save_operator_implementation`.
3. Write focused tests that follow the retrieved test conventions. Pass the
   complete test source to `save_operator_test_implementation`.
4. Call `test_operator_implementation` with the saved test path.
5. If the test fails, correct the implementation or tests from the returned error,
   save the changes, and run the test again. Do not claim success from code
   generation alone.
6. After the test returns exit code zero, call `sync_operator_documentation`.

The task is complete only when the tests pass and documentation synchronization
succeeds. Return the operator name and type, saved paths, test result, and
documentation synchronization result.
