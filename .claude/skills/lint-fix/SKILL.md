---
name: lint-fix
description: Run ruff linter with auto-fix and format the codebase
disable-model-invocation: true
allowed-tools: Bash
---

Run the linter with auto-fix and formatter:

```bash
uv run ruff check . --fix && uv run ruff format .
```

After running:
1. Report how many issues were found and fixed
2. If any issues remain unfixed, list them and suggest manual fixes
