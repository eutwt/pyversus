# Developer Reference

This document captures the essentials of the Python implementation so
future contributors can work without hunting through old context.

## Project layout

- `pyproject.toml` – package metadata. Targets Python ≥3.7 and depends on
  `duckdb>=1.0.0` plus `polars>=0.18` (0.18 is the last release with
  Python 3.7 wheels).
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
  - handles to the temp views plus a `DiffKeyTable` mapping (surfaced to
    users as the `diff_rows` column) so helper methods can fetch diff
    keys without recomputing predicates
  - lookup tables for unmatched rows/diff counts
- Helper methods (`value_diffs`, `slice_diffs`, `weave_diffs_*`,
  `slice_unmatched*`) push their work back into DuckDB and return Polars
  DataFrames, keeping the API fast and memory-light even for large
  tables.
- Duplicate `by` keys are detected early (`_ensure_unique_by`) and raise
  `ComparisonError` listing the conflicting key values.

## Testing & tooling

- Use the checked-in `.venv` managed by `uv` to ensure `pyarrow` stays
  available for DuckDB’s `relation.pl()` path:
  1. `uv venv .venv`
  2. `uv pip install -e . pytest pyarrow`
  3. `uv run pytest`
- There are no extra runtime dependencies beyond DuckDB/Polars/PyArrow.
  For local benchmarks you can author separate scripts (outside the
  package) that import `versus.compare`; the repo no longer includes R
  assets or benchmark harnesses.
- Keep the code base Python 3.7-compatible (no pattern matching,
  `str.removeprefix`, `typing.Annotated`, etc.).

## Style notes

- When accepting column lists, reuse `_resolve_column_list` so empty
  selections raise early and only shared columns are allowed.
- Avoid materializing DuckDB relations unless you must return a Polars
  frame to the user; all computation should happen via SQL.
- Ensure new helpers return Polars DataFrames ordered similarly to the
  R version (by columns first, then the requested fields).

That should be enough context for future maintainers to extend the
package without digging through previous agent transcripts.
