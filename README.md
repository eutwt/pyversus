
<!-- README.md now describes the Python package -->

# versus (Python)

`versus` is a Python package that mirrors the ergonomics of the original
R library while pushing all heavy work into DuckDB. It compares two
relations (tables, views, or subqueries) and exposes a `Comparison`
object that summarises where the data diverges. The object is
lightweight: it only stores metadata and key locations so that every
follow-up call reuses the original DuckDB relations without copying the
entire dataset into Python.

## Installation

This repository is already a Python project. Install it into your
environment (preferably a virtual environment) with:

```bash
pip install -e .
```

This will pull in the only runtime dependencies, DuckDB and Polars (0.18+).

## Quick start

```python
import duckdb
from versus import compare, examples

con = duckdb.connect()
rel_a = examples.example_cars_a(con)
rel_b = examples.example_cars_b(con)

comparison = compare(rel_a, rel_b, by=["car"], connection=con)
print(comparison.tables)
#   table    source  nrows  ncols
# 0      a  relation      9      9
# 1      b  relation     10      9

print(comparison.intersection.select(["column", "n_diffs"]))
#    column  n_diffs
# 0     mpg        2
# 1     cyl        0
# 2    disp        2
# ...

comparison.value_diffs("disp")
#    disp_a  disp_b           car
# 0     109     108     Datsun 710
# 1     259     258  Hornet 4 Drive

comparison.value_diffs_stacked(["mpg", "disp"])
#   column  val_a  val_b           car
# 0    mpg   14.3   16.3     Duster 360
# 1    mpg   24.4   26.4      Merc 240D
# 2   disp  109.0  108.0     Datsun 710
# 3   disp  259.0  258.0  Hornet 4 Drive

comparison.weave_diffs_wide(["mpg", "disp"])
# merges the rows with differences and keeps both versions side-by-side

comparison.weave_diffs_long(["disp"])
# interleaves the rows from each table so you can review them sequentially

comparison.slice_diffs("a", ["mpg"])
# returns the subset of rows from table "a" that have mpg differences

comparison.slice_unmatched("b")
# the rows present in table "b" but not in table "a"

comparison.slice_unmatched_both()
# stacks the unmatched rows from both tables with a `table` column
```

## Notes

- `compare()` accepts DuckDB relations or SQL strings/views. When you
  pass relations created on a custom DuckDB connection you must supply
  the `connection=` argument so that the comparison can run SQL against
  the same database.
- Columns used in `by` must uniquely identify rows in each table. If a
  duplicate is found the function raises a `ComparisonError` explaining
  which `by` values appear multiple times.
- Only the metadata and row identifiers needed for later operations are
  stored inside the `Comparison` object. Fetching the actual differing
  rows always happens lazily inside DuckDB, so the workflow remains fast
  even for large tables.
- Inputs stay lazy too: `compare()` never pulls the source relations into
  Python objects before producing the final comparison representation,
  and the helper methods return Polars data frames.
- For quick experimentation you can use the built-in example tables
  `versus.examples.example_cars_a()` and `versus.examples.example_cars_b()`
  as shown above.

The package currently exposes the same high-level helpers as the R
version (`value_diffs*`, `weave_diffs*`, `slice_*`) so it should feel
familiar if you already used the original library.
# pyversus
