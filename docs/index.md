# pyversus

DuckDB-powered tools for comparing two relations (tables, views, or SQL queries)
without materializing them in Python.

!!! note "Alpha status"
    This package is still evolving and may change between releases.

## Quick start

The two relations below are used as an example to demonstrate functionality.

```python exec="on" session="quickstart"
--8<-- "quickstart.py:setup"
```

Use `compare()` to create a comparison of two tables.

A comparison contains:

- `comparison.intersection`: columns in both tables and rows with differing values
- `comparison.unmatched_cols`: columns in only one table
- `comparison.unmatched_rows`: rows in only one table

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:comparison"
```

Use `value_diffs()` to see the values that are different.

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:value-diffs"
```

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:value-diffs-stacked"
```

Use `weave_diffs_*()` to see the differing values in context.

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:weave-diffs-wide-disp"
```

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:weave-diffs-wide-mpg-disp"
```

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:weave-diffs-long-disp"
```

Use `slice_diffs()` to get the rows with differing values from one table.

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:slice-diffs"
```

Use `slice_unmatched()` to get the unmatched rows from one or both tables.

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:slice-unmatched"
```

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:slice-unmatched-both"
```

Use `summary()` to see what kind of differences were found.

```python exec="on" result="text" session="quickstart"
--8<-- "quickstart.py:summary"
```
