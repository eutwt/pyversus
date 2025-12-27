# pyversus

DuckDB-powered tools for comparing two relations (tables, views, or SQL queries)
without materializing them in Python.

!!! note "Alpha status"
    This package is still evolving and may change between releases.

## Quick start

```python
import duckdb
from versus import compare, examples

con = duckdb.connect()
rel_a = examples.example_cars_a(con)
rel_b = examples.example_cars_b(con)

comparison = compare(rel_a, rel_b, by=["car"], connection=con)
comparison
```

From the returned `Comparison`, you can ask for specific views of the
differences:

```python
comparison.value_diffs("disp")
comparison.value_diffs_stacked(["mpg", "disp"])
comparison.weave_diffs_wide(["mpg", "disp"])
comparison.weave_diffs_long(["disp"])
comparison.slice_diffs("a", ["mpg"])
comparison.slice_unmatched("b")
comparison.slice_unmatched_both()
comparison.summary()
```
