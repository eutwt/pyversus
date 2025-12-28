# Alignment Tracker (Python vs R)

This file tracks user-facing differences between the Python and R
implementations and notes which ones are intentional.

## Intentional differences
- Python adds materialization modes and lazy summary relations; R always
  returns in-memory data frames.
- Input types differ: Python accepts DuckDB relations plus pandas/polars
  DataFrames with string column names; R accepts data frames and uses tidy-select
  semantics.
- Python exposes unmatched row counts in `unmatched_rows` and the row keys in
  `unmatched_keys`, rather than returning full unmatched rows by default.
- `intersection` in Python does not include nested diff row keys; use
  `value_diffs`, `slice_diffs`, or the diff table helpers instead.
- Python summary tables omit the source expression column (`expr`) shown in R.
- Summary column labels differ: Python uses `table_name`, `type_*`, and
  `type_diffs`; R uses `table`, `class_*`, and `class_diffs`.
- Output row ordering in Python follows database ordering (often key-sorted),
  not the original input row order used in R.
- `table_id` values are not normalized in Python (no syntactic name repair).
- Type coercion for stacked outputs relies on DuckDB rather than explicit
  character conversion.
- There is no Python equivalent of `versus.copy_data_table`.
