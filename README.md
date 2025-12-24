
<!-- README.md now describes the Python package -->

# versus (Python)

`versus` is a Python package that mirrors the the original R library while pushing
all heavy work into DuckDB. Use it to compare two duckdb relations (tables, views,
or queries) without materializing them. The `compare()` function gives a `Comparison`
object that shows where the tables disagree, with methods for displaying the
differences.

## Installation

Clone the repo (or add it as a submodule) and install it into your
environment using:

```bash
pip install -e .
```

That command installs DuckDB, the only runtime dependency.

## Quick start

Here is a small interactive session you can paste into a Python REPL:

```python
import duckdb
from versus import compare, examples

con = duckdb.connect()
rel_a = examples.example_cars_a(con)
rel_b = examples.example_cars_b(con)

comparison = compare(rel_a, rel_b, by="car", connection=con)
comparison
# Comparison(tables=
# ┌─────────┬───────────────────────────────────┬───────┬───────┐
# │  table  │              source               │ nrows │ ncols │
# │ varchar │              varchar              │ int64 │ int64 │
# ├─────────┼───────────────────────────────────┼───────┼───────┤
# │ a       │ unnamed_relation_e9b454127dba13b6 │     9 │     9 │
# │ b       │ unnamed_relation_64e4ed070de71ea9 │    10 │     9 │
# └─────────┴───────────────────────────────────┴───────┴───────┘
# 
# by=
# ┌─────────┬─────────┬─────────┐
# │ column  │ class_a │ class_b │
# │ varchar │ varchar │ varchar │
# ├─────────┼─────────┼─────────┤
# │ car     │ VARCHAR │ VARCHAR │
# └─────────┴─────────┴─────────┘
# 
# intersection=
# ┌─────────┬─────────┬──────────────┬──────────────┐
# │ column  │ n_diffs │   class_a    │   class_b    │
# │ varchar │  int64  │   varchar    │   varchar    │
# ├─────────┼─────────┼──────────────┼──────────────┤
# │ mpg     │       2 │ DECIMAL(3,1) │ DECIMAL(3,1) │
# │ cyl     │       0 │ INTEGER      │ INTEGER      │
# │ disp    │       2 │ INTEGER      │ INTEGER      │
# │ hp      │       0 │ INTEGER      │ INTEGER      │
# │ drat    │       0 │ DECIMAL(3,2) │ DECIMAL(3,2) │
# │ wt      │       0 │ DECIMAL(3,2) │ DECIMAL(3,2) │
# │ vs      │       0 │ INTEGER      │ INTEGER      │
# └─────────┴─────────┴──────────────┴──────────────┘
# 
# unmatched_cols=
# ┌─────────┬─────────┬─────────┐
# │  table  │ column  │  class  │
# │ varchar │ varchar │ varchar │
# ├─────────┼─────────┼─────────┤
# │ a       │ am      │ INTEGER │
# │ b       │ carb    │ INTEGER │
# └─────────┴─────────┴─────────┘
# 
# unmatched_rows=
# ┌─────────┬────────────┐
# │  table  │    car     │
# │ varchar │  varchar   │
# ├─────────┼────────────┤
# │ a       │ Mazda RX4  │
# │ b       │ Merc 280C  │
# │ b       │ Merc 450SE │
# └─────────┴────────────┘
# 
# )

comparison.value_diffs("disp")
# ┌────────┬────────┬────────────────┐
# │ disp_a │ disp_b │      car       │
# │ int32  │ int32  │    varchar     │
# ├────────┼────────┼────────────────┤
# │    109 │    108 │ Datsun 710     │
# │    259 │    258 │ Hornet 4 Drive │
# └────────┴────────┴────────────────┘

comparison.value_diffs_stacked(["mpg", "disp"])
# ┌─────────┬───────────────┬───────────────┬────────────────┐
# │ column  │     val_a     │     val_b     │      car       │
# │ varchar │ decimal(11,1) │ decimal(11,1) │    varchar     │
# ├─────────┼───────────────┼───────────────┼────────────────┤
# │ mpg     │          24.4 │          26.4 │ Merc 240D      │
# │ mpg     │          14.3 │          16.3 │ Duster 360     │
# │ disp    │         109.0 │         108.0 │ Datsun 710     │
# │ disp    │         259.0 │         258.0 │ Hornet 4 Drive │
# └─────────┴───────────────┴───────────────┴────────────────┘

comparison.weave_diffs_wide(["mpg", "disp"])
# ┌────────────────┬──────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
# │      car       │    mpg_a     │    mpg_b     │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
# │    varchar     │ decimal(3,1) │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
# ├────────────────┼──────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
# │ Duster 360     │         14.3 │         16.3 │     8 │    360 │    360 │   245 │         3.21 │         3.57 │     0 │
# │ Datsun 710     │         22.8 │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
# │ Merc 240D      │         24.4 │         26.4 │     4 │    147 │    147 │    62 │         3.69 │         3.19 │     1 │
# │ Hornet 4 Drive │         21.4 │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
# └────────────────┴──────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘

comparison.weave_diffs_long("disp")
# ┌─────────┬────────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
# │  table  │      car       │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
# │ varchar │    varchar     │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
# ├─────────┼────────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
# │ a       │ Datsun 710     │         22.8 │  NULL │   109 │    93 │         3.85 │         2.32 │     1 │
# │ b       │ Datsun 710     │         22.8 │  NULL │   108 │    93 │         3.85 │         2.32 │     1 │
# │ a       │ Hornet 4 Drive │         21.4 │     6 │   259 │   110 │         3.08 │         3.22 │     1 │
# │ b       │ Hornet 4 Drive │         21.4 │     6 │   258 │   110 │         3.08 │         3.22 │     1 │
# └─────────┴────────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

comparison.slice_diffs("a", "mpg")
# ┌────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
# │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
# │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
# ├────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
# │ Duster 360 │         14.3 │     8 │   360 │   245 │         3.21 │         3.57 │     0 │     0 │
# │ Merc 240D  │         24.4 │     4 │   147 │    62 │         3.69 │         3.19 │     1 │     0 │
# └────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘

# (The `column` argument only decides which diffs include a row; the returned relation
# always keeps the full schema of the requested table.)

comparison.slice_unmatched("b")
# ┌────────────┬──────────────┬──────────────┬───────┬───────┬───────┬───────┬──────────────┬───────┐
# │    car     │      wt      │     mpg      │  hp   │  cyl  │ disp  │ carb  │     drat     │  vs   │
# │  varchar   │ decimal(3,2) │ decimal(3,1) │ int32 │ int32 │ int32 │ int32 │ decimal(3,2) │ int32 │
# ├────────────┼──────────────┼──────────────┼───────┼───────┼───────┼───────┼──────────────┼───────┤
# │ Merc 280C  │         3.44 │         17.8 │   123 │     6 │   168 │     4 │         3.92 │     1 │
# │ Merc 450SE │         4.07 │         16.4 │   180 │     8 │   276 │     3 │         3.07 │     0 │
# └────────────┴──────────────┴──────────────┴───────┴───────┴───────┴───────┴──────────────┴───────┘

comparison.slice_unmatched_both()
# ┌─────────┬────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
# │  table  │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
# │ varchar │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
# ├─────────┼────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
# │ a       │ Mazda RX4  │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │
# │ b       │ Merc 280C  │         17.8 │     6 │   168 │   123 │         3.92 │         3.44 │     1 │
# │ b       │ Merc 450SE │         16.4 │     8 │   276 │   180 │         3.07 │         4.07 │     0 │
# └─────────┴────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

comparison.summary()
# ┌──────────────┬────────┐
# │ difference   │ found  │
# │ ---          │ ---    │
# │ str          │ bool   │
# ╞══════════════╪════════╡
# │ value_diffs  │ true   │
# │ unmatched... │ true   │
# │ unmatched... │ true   │
# │ class_diffs  │ false  │
# └──────────────┴────────┘
```

## Notes

- Call `compare()` with DuckDB relations or SQL strings/views. If your
  relations live on a custom DuckDB connection, pass it via
  `connection=` so the comparison queries use the same database.
- The `by` columns must uniquely identify rows in each table. When they
  do not, `compare()` raises `ComparisonError` and tells you which key
  values repeat.
- The resulting `Comparison` object stores only metadata and row
  identifiers. Whenever you ask for actual rows (`value_diffs`, slices,
  weave helpers, etc.), the library runs SQL in DuckDB and returns the
  results as DuckDB relations, so you can inspect huge tables without
  blowing up Python memory.
- By default `compare()` materializes the summary relations used in the
  repr (`tables`, `by`, `intersection`, `unmatched_*`). Pass
  `materialize=False` if you prefer to keep those lazy as well (at the
  cost of recomputing them every time you print).
- Inputs stay lazy as well: `compare()` never materialises the full
  source tables in Python.
- Want to kick the tyres quickly? The `versus.examples.example_cars_*`
  helpers used in the quick start are available for ad-hoc testing.

The package exposes the same high-level helpers as the R version
(`value_diffs*`, `weave_diffs*`, `slice_*`), so if you already know the
R API you can continue working the same way here.
# pyversus
