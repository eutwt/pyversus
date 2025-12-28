# pyversus


`pyversus` (imported as `versus`) is a Python package that mirrors the
the original R library while pushing all heavy work into DuckDB. Use it
to compare two duckdb relations (tables or views) or pandas/polars
DataFrames without materializing them. The `compare()` function gives a
`Comparison` object that shows where the tables disagree, with methods
for displaying the differences.

> **Alpha status:** This package is in active development. Backward
> compatibility is not guaranteed between releases yet.

## Installation

Clone the repo (or add it as a submodule) and install it into your
environment using:

``` bash
pip install -e .
```

That command installs DuckDB, the only runtime dependency.

## Quick start

Here is a small interactive session you can paste into a Python REPL:

``` python
from versus import compare, examples

rel_a = examples.example_cars_a()
rel_b = examples.example_cars_b()

comparison = compare(rel_a, rel_b, by="car")
comparison
```

    Comparison(tables=
    ┌────────────┬───────┬───────┐
    │ table_name │ nrow  │ ncol  │
    │  varchar   │ int64 │ int64 │
    ├────────────┼───────┼───────┤
    │ a          │     9 │     9 │
    │ b          │    10 │     9 │
    └────────────┴───────┴───────┘

    by=
    ┌─────────┬─────────┬─────────┐
    │ column  │ type_a  │ type_b  │
    │ varchar │ varchar │ varchar │
    ├─────────┼─────────┼─────────┤
    │ car     │ VARCHAR │ VARCHAR │
    └─────────┴─────────┴─────────┘

    intersection=
    ┌─────────┬─────────┬──────────────┬──────────────┐
    │ column  │ n_diffs │    type_a    │    type_b    │
    │ varchar │  int64  │   varchar    │   varchar    │
    ├─────────┼─────────┼──────────────┼──────────────┤
    │ mpg     │       2 │ DECIMAL(3,1) │ DECIMAL(3,1) │
    │ cyl     │       0 │ INTEGER      │ INTEGER      │
    │ disp    │       2 │ INTEGER      │ INTEGER      │
    │ hp      │       0 │ INTEGER      │ INTEGER      │
    │ drat    │       0 │ DECIMAL(3,2) │ DECIMAL(3,2) │
    │ wt      │       0 │ DECIMAL(3,2) │ DECIMAL(3,2) │
    │ vs      │       0 │ INTEGER      │ INTEGER      │
    └─────────┴─────────┴──────────────┴──────────────┘

    unmatched_cols=
    ┌────────────┬─────────┬─────────┐
    │ table_name │ column  │  type   │
    │  varchar   │ varchar │ varchar │
    ├────────────┼─────────┼─────────┤
    │ a          │ am      │ INTEGER │
    │ b          │ carb    │ INTEGER │
    └────────────┴─────────┴─────────┘

    unmatched_rows=
    ┌────────────┬─────────────┐
    │ table_name │ n_unmatched │
    │  varchar   │    int64    │
    ├────────────┼─────────────┤
    │ a          │           1 │
    │ b          │           2 │
    └────────────┴─────────────┘

    )

``` python
comparison.value_diffs("disp")
```

    ┌────────┬────────┬────────────────┐
    │ disp_a │ disp_b │      car       │
    │ int32  │ int32  │    varchar     │
    ├────────┼────────┼────────────────┤
    │    109 │    108 │ Datsun 710     │
    │    259 │    258 │ Hornet 4 Drive │
    └────────┴────────┴────────────────┘

``` python
comparison.value_diffs_stacked(["mpg", "disp"])
```

    ┌─────────┬───────────────┬───────────────┬────────────────┐
    │ column  │     val_a     │     val_b     │      car       │
    │ varchar │ decimal(11,1) │ decimal(11,1) │    varchar     │
    ├─────────┼───────────────┼───────────────┼────────────────┤
    │ mpg     │          24.4 │          26.4 │ Merc 240D      │
    │ mpg     │          14.3 │          16.3 │ Duster 360     │
    │ disp    │         109.0 │         108.0 │ Datsun 710     │
    │ disp    │         259.0 │         258.0 │ Hornet 4 Drive │
    └─────────┴───────────────┴───────────────┴────────────────┘

``` python
comparison.weave_diffs_wide(["mpg", "disp"])
```

    ┌────────────────┬──────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
    │      car       │    mpg_a     │    mpg_b     │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
    │    varchar     │ decimal(3,1) │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
    ├────────────────┼──────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
    │ Duster 360     │         14.3 │         16.3 │     8 │    360 │    360 │   245 │         3.21 │         3.57 │     0 │
    │ Datsun 710     │         22.8 │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
    │ Merc 240D      │         24.4 │         26.4 │     4 │    147 │    147 │    62 │         3.69 │         3.19 │     1 │
    │ Hornet 4 Drive │         21.4 │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
    └────────────────┴──────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘

``` python
comparison.weave_diffs_long("disp")
```

    ┌────────────┬────────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
    │ table_name │      car       │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
    │  varchar   │    varchar     │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
    ├────────────┼────────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
    │ a          │ Datsun 710     │         22.8 │  NULL │   109 │    93 │         3.85 │         2.32 │     1 │
    │ b          │ Datsun 710     │         22.8 │  NULL │   108 │    93 │         3.85 │         2.32 │     1 │
    │ a          │ Hornet 4 Drive │         21.4 │     6 │   259 │   110 │         3.08 │         3.22 │     1 │
    │ b          │ Hornet 4 Drive │         21.4 │     6 │   258 │   110 │         3.08 │         3.22 │     1 │
    └────────────┴────────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

``` python
comparison.slice_diffs("a", "mpg")
```

    ┌────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
    │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
    │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
    ├────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
    │ Duster 360 │         14.3 │     8 │   360 │   245 │         3.21 │         3.57 │     0 │     0 │
    │ Merc 240D  │         24.4 │     4 │   147 │    62 │         3.69 │         3.19 │     1 │     0 │
    └────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘

> Note: The `column` argument only decides which diffs include a row;
> the returned relation always keeps the full schema of the requested
> table.

``` python
comparison.slice_unmatched("b")
```

    ┌────────────┬──────────────┬──────────────┬───────┬───────┬───────┬───────┬──────────────┬───────┐
    │    car     │      wt      │     mpg      │  hp   │  cyl  │ disp  │ carb  │     drat     │  vs   │
    │  varchar   │ decimal(3,2) │ decimal(3,1) │ int32 │ int32 │ int32 │ int32 │ decimal(3,2) │ int32 │
    ├────────────┼──────────────┼──────────────┼───────┼───────┼───────┼───────┼──────────────┼───────┤
    │ Merc 280C  │         3.44 │         17.8 │   123 │     6 │   168 │     4 │         3.92 │     1 │
    │ Merc 450SE │         4.07 │         16.4 │   180 │     8 │   276 │     3 │         3.07 │     0 │
    └────────────┴──────────────┴──────────────┴───────┴───────┴───────┴───────┴──────────────┴───────┘

``` python
comparison.slice_unmatched_both()
```

    ┌────────────┬────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
    │ table_name │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
    │  varchar   │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
    ├────────────┼────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
    │ a          │ Mazda RX4  │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │
    │ b          │ Merc 280C  │         17.8 │     6 │   168 │   123 │         3.92 │         3.44 │     1 │
    │ b          │ Merc 450SE │         16.4 │     8 │   276 │   180 │         3.07 │         4.07 │     0 │
    └────────────┴────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

``` python
comparison.summary()
```

    ┌────────────────┬─────────┐
    │   difference   │  found  │
    │    varchar     │ boolean │
    ├────────────────┼─────────┤
    │ value_diffs    │ true    │
    │ unmatched_cols │ true    │
    │ unmatched_rows │ true    │
    │ type_diffs     │ false   │
    └────────────────┴─────────┘

## Usage

- Call `compare()` with DuckDB relations or pandas/polars DataFrames. To
  compare a SQL query, build a relation with `connection.sql(...)`
  first. If your relations live on a custom DuckDB connection, pass it
  via `connection=` so the comparison queries use the same database.
- The `by` columns must uniquely identify rows in each table. When they
  do not, `compare()` raises `ComparisonError` and tells you which key
  values repeat.
- The resulting `Comparison` object stores only metadata and row
  identifiers. Whenever you ask for actual rows (`value_diffs`, slices,
  weave helpers, etc.), the library runs SQL in DuckDB and returns the
  results as DuckDB relations, so you can inspect huge tables without
  blowing up Python memory.
- Need insight into the inputs? `comparison.inputs` exposes a mapping
  from table id (e.g., `"a"`, `"b"`) to the input relations.
- Need the row identifiers for unmatched rows?
  `comparison.unmatched_keys` exposes the table id plus `by` columns for
  those keys.
- Inputs stay lazy as well: `compare()` never materialises the full
  source tables in Python.
- Want to kick the tires quickly? The `versus.examples.example_cars_*`
  helpers used in the quick start are available for ad-hoc testing.

### Materialization

When you call `compare()`, Pyversus defines summary tables for the
printed output (`tables`, `by`, `intersection`, `unmatched_cols`,
`unmatched_rows`). These are relation-like wrappers that materialize
themselves on print. The input tables are never materialized by Pyversus
in any mode; they stay as DuckDB relations and are queried lazily.

In full materialization, Pyversus also builds diff key relations:
per-column relations of `by` keys where values differ. Those precomputed
keys let row-level helpers fetch the exact differing rows quickly via
joins, which can be faster when you call multiple helpers. Other modes
skip diff keys and compute diff counts inline.

- `materialize="all"`: store the summary tables and diff key tables as
  temp tables. This is fastest if you will call row-level helpers
  multiple times.
- `materialize="summary"`: store only the summary tables. Row-level
  helpers run inline predicates and return lazy relations.
- `materialize="none"`: do not store anything up front. Printing the
  comparison materializes the summary tables.

Row-level helper outputs are always returned as DuckDB relations and are
never materialized automatically; materialize them explicitly if needed.

The package exposes the same high-level helpers as the R version
(`value_diffs*`, `weave_diffs*`, `slice_*`), so if you already know the
R API you can continue working the same way here.
