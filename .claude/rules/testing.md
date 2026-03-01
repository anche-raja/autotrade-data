---
paths:
  - "tests/**/*.py"
---

# Testing conventions

- Tests use `pytest` with `pythonpath = ["."]` so `marketdata` package imports work directly.
- Test files in `tests/` named `test_<module>.py`, test functions `test_<behavior>`.
- `asyncio_mode = "auto"` in pyproject.toml — no need for manual `@pytest.mark.asyncio`.
- Use `conftest.py` for shared fixtures (sample DataFrames, config helpers, mock clients).
- Use `tmp_path` (pytest built-in) for any file I/O (Parquet, DuckDB).
- Prefer real assertions over mocks; mock only IBKR client and external services (Twitter API).
- Run tests: `uv run pytest` (all), `uv run pytest tests/test_storage.py -x` (one file), `uv run pytest -k test_name` (one test).
- Keep tests fast; avoid network calls in unit tests.
