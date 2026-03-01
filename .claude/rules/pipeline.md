---
paths:
  - "marketdata/**/*.py"
  - "gateway/**/*.py"
---

# Pipeline conventions

- All config via `PipelineConfig` (Pydantic `BaseSettings`, env prefix `MD_`, nested delimiter `__`).
- Never hardcode symbols, ports, or credentials — always read from `load_config()`.
- Hive-partitioned Parquet layout: `dataset=.../symbol=.../bar=.../date=.../part.parquet`.
- Parquet schemas are defined as `pa.schema` constants in `marketdata/pipeline/storage.py`.
- DuckDB (`marketdata/db/duck.py`) tracks ingestion status, checksums, and gaps per partition.
- Use `asyncio.run()` only at CLI entry points; all internal code uses `async/await`.
- Use `from __future__ import annotations` in every file.
- Use `%s` format strings in log calls (not f-strings) to avoid eager evaluation.
- Resource cleanup via `try/finally` blocks for IBKR connections and DuckDB handles.
- Never swallow exceptions silently — always log at minimum `log.debug("...", exc_info=True)`.
