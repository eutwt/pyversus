# Developer Reference

This document captures the essentials of the Python implementation so
future contributors can work without hunting through old context.

## Project layout

- `pyproject.toml` – package metadata. Targets Python ≥3.7 and depends on
  `duckdb>=1.0.0`.
- `python/versus/`
  - `__init__.py` – exports `compare`, `Comparison`,
    `ComparisonError`, and the `examples` module.
  - `comparison.py` – core implementation. Keeps DuckDB relations lazy
    until the concise comparison representation is materialized, then
    stores only metadata and key views in `Comparison`.
  - `examples.py` – exposes the original `example_cars_*` tables as
    DuckDB relations for demos/tests.
- `tests/` – pytest suite mirroring the behavior covered by the R
  testthat files (value diffs, weave helpers, slices, unmatched rows,
  etc.). `tests/conftest.py` adds the `python/` folder to `sys.path` so
  `pytest` can import the package without installation.

## Key concepts

- `compare()` only accepts DuckDB relations or SQL strings. The optional
  `connection` parameter must be provided when the relations were created
  on non-default connections so subsequent helper queries run in the
  correct database.
- The `Comparison` object stores:
  - table metadata (`tables`, `by`, `unmatched_cols`, `intersection`)
  - internal handles to the temp views plus a mapping of column name to
    diff-key relation that is used for `materialize="all"` row helpers;
    other materialization modes run predicates inline instead
  - `Comparison.inputs`, a mapping from table id to the input relations
    for direct querying
  - lookup tables for unmatched rows/diff counts
- Helper methods (`value_diffs`, `slice_diffs`, `weave_diffs_*`,
  `slice_unmatched*`) push their work back into DuckDB and return
  `DuckDBPyRelation` objects, keeping the API fast and memory-light even
  for large tables. The summary relations shown in `Comparison.__repr__`
  are materialized by default so printing is cheap; use
  `materialize="none"` to keep those summary tables lazy until printed.
- Duplicate `by` keys are detected early (`ensure_unique_by`) and raise
  `ComparisonError` listing the conflicting key values.
- Temporary tables/views are created via `CREATE TEMP ...` with unique
  names scoped to the connection, so they never leak outside the current
  DuckDB session.

## Testing & tooling

- Run tests as needed without asking; prefer `uv run pytest` from the
  repo root.
- Use the checked-in `.venv` managed by `uv`:
  1. `uv venv .venv`
  2. `uv pip install -e . pytest`
  3. `uv run pytest`
- There are no extra runtime dependencies beyond DuckDB. For local
  benchmarks you can author separate scripts (outside the package) that
  import `versus.compare`; the repo no longer includes R assets or
  benchmark harnesses.
- Keep the code base Python 3.7-compatible (no pattern matching,
  `str.removeprefix`, `typing.Annotated`, etc.).
- GitHub Actions runs the test suite on Python 3.7. Keep that workflow
  green: run `uv run pytest` locally before pushing and don’t merge
  unless the Actions build is green.
- Static type checking uses Astral's `ty` (configured in `ty.toml`).
  Run `uvx ty check` (or `uv tool install ty` once and use `ty check`)
  from the repo root; ty will report any signature mismatches that could
  regress safety in the DuckDB helpers.
- Before pushing, run `scripts/pre_push.sh` (or symlink it into
  `.git/hooks/pre-push`) so Ruff formatting, `pytest`, and `ty check`
  finish successfully. The script aborts pushes if any step fails.
- The `README.md` Quick Start tables are always programmatically
  generated with `scripts/update_readme.py` (do not edit them by hand).
  Whenever a change affects any helper output (or periodically to keep
  hashes current), run `uv run python scripts/update_readme.py` and
  commit the result so the documentation shows real data.

## Style notes

- When accepting column lists, reuse `resolve_column_list` so empty
  selections raise early and only shared columns are allowed.
- Avoid materializing DuckDB relations into Python data structures
  unless absolutely necessary; keep as much work inside DuckDB as
  possible.
- Prefer returning `DuckDBPyRelation` objects from internal helpers and
  intermediate steps over passing table names or SQL strings around, so
  we keep a consistent relation-first flow.
- Ensure new helpers return relations ordered similarly to the R version
  (by columns first, then the requested fields).

That should be enough context for future maintainers to extend the
package without digging through previous agent transcripts.
