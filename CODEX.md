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
  - handles to the temp views plus a `DiffKeyTable` mapping (available
    via `Comparison.diff_rows` even though the intersection table only
    shows counts) so helper methods can fetch diff keys without
    recomputing predicates
  - lookup tables for unmatched rows/diff counts
- Helper methods (`value_diffs`, `slice_diffs`, `weave_diffs_*`,
  `slice_unmatched*`) push their work back into DuckDB and return
  `DuckDBPyRelation` objects, keeping the API fast and memory-light even
  for large tables. The summary relations shown in `Comparison.__repr__`
  are materialized by default so printing is cheap; pass
  `materialize=False` to `compare()` if you want them lazy instead.
- Duplicate `by` keys are detected early (`_ensure_unique_by`) and raise
  `ComparisonError` listing the conflicting key values.
- Temporary tables/views are created via `CREATE TEMP ...` with unique
  names scoped to the connection, so they never leak outside the current
  DuckDB session.

## Testing & tooling

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
- The Quick Start tables in `README.md` are regenerated with
  `scripts/update_readme.py`. Whenever a change affects any helper
  output (or periodically to keep hashes current), run
  `uv run python scripts/update_readme.py` and commit the result so the
  documentation shows real data.

## Style notes

- When accepting column lists, reuse `_resolve_column_list` so empty
  selections raise early and only shared columns are allowed.
- Avoid materializing DuckDB relations into Python data structures
  unless absolutely necessary; keep as much work inside DuckDB as
  possible.
- Ensure new helpers return relations ordered similarly to the R version
  (by columns first, then the requested fields).

That should be enough context for future maintainers to extend the
package without digging through previous agent transcripts.
