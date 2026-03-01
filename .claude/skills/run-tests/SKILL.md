---
name: run-tests
description: Run the project test suite and report results
disable-model-invocation: true
allowed-tools: Bash
---

Run the test suite for this project:

```bash
uv run pytest $ARGUMENTS -x -q
```

If no arguments are provided, run all tests. If arguments specify a path or test name, run only those.

After running:
1. Report pass/fail count
2. If any tests fail, show the failure details
3. Suggest fixes for failing tests
