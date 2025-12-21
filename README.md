
<!-- README.md now describes the Python package -->

# versus (Python)

`versus` is a Python package that mirrors the the original R library while pushing
all heavy work into DuckDB. Use it to compare two duckdb relations (tables, views,
or queries) without materializing them. The `compare()` function gives a `Comparison`
object that shows where the tables disagree, with methods for displaying the
differences.

## Installation

Clone the repo (or add it as a submodule) and install it into your
environment—ideally a virtual environment—using:

```bash
pip install -e .
```

That command installs the only runtime dependencies, DuckDB and Polars
(0.18+).

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
# shape: (2, 4)
# ┌───────┬─────────────────────────────────┬───────┬───────┐
# │ table ┆ source                          ┆ nrows ┆ ncols │
# │ ---   ┆ ---                             ┆ ---   ┆ ---   │
# │ str   ┆ str                             ┆ i64   ┆ i64   │
# ╞═══════╪═════════════════════════════════╪═══════╪═══════╡
# │ a     ┆ unnamed_relation_c27ef6fa74185… ┆ 9     ┆ 9     │
# │ b     ┆ unnamed_relation_ab7159cdf9ceb… ┆ 10    ┆ 9     │
# └───────┴─────────────────────────────────┴───────┴───────┘
# by=
# shape: (1, 3)
# ┌────────┬─────────┬─────────┐
# │ column ┆ class_a ┆ class_b │
# │ ---    ┆ ---     ┆ ---     │
# │ str    ┆ str     ┆ str     │
# ╞════════╪═════════╪═════════╡
# │ car    ┆ VARCHAR ┆ VARCHAR │
# └────────┴─────────┴─────────┘
# intersection=
# shape: (7, 5)
# ┌────────┬─────────┬──────────────┬──────────────┬───────────┐
# │ column ┆ n_diffs ┆ class_a      ┆ class_b      ┆ diff_rows │
# │ ---    ┆ ---     ┆ ---          ┆ ---          ┆ ---       │
# │ str    ┆ i64     ┆ str          ┆ str          ┆ object    │
# ╞════════╪═════════╪══════════════╪══════════════╪═══════════╡
# │ mpg    ┆ 2       ┆ DECIMAL(3,1) ┆ DECIMAL(3,1) ┆ <2 rows>  │
# │ cyl    ┆ 0       ┆ INTEGER      ┆ INTEGER      ┆ <0 rows>  │
# │ disp   ┆ 2       ┆ INTEGER      ┆ INTEGER      ┆ <2 rows>  │
# │ hp     ┆ 0       ┆ INTEGER      ┆ INTEGER      ┆ <0 rows>  │
# │ drat   ┆ 0       ┆ DECIMAL(3,2) ┆ DECIMAL(3,2) ┆ <0 rows>  │
# │ wt     ┆ 0       ┆ DECIMAL(3,2) ┆ DECIMAL(3,2) ┆ <0 rows>  │
# │ vs     ┆ 0       ┆ INTEGER      ┆ INTEGER      ┆ <0 rows>  │
# └────────┴─────────┴──────────────┴──────────────┴───────────┘
# unmatched_cols=
# shape: (2, 3)
# ┌───────┬────────┬─────────┐
# │ table ┆ column ┆ class   │
# │ ---   ┆ ---    ┆ ---     │
# │ str   ┆ str    ┆ str     │
# ╞═══════╪════════╪═════════╡
# │ a     ┆ am     ┆ INTEGER │
# │ b     ┆ carb   ┆ INTEGER │
# └───────┴────────┴─────────┘
# unmatched_rows=
# shape: (3, 2)
# ┌───────┬────────────┐
# │ table ┆ car        │
# │ ---   ┆ ---        │
# │ str   ┆ str        │
# ╞═══════╪════════════╡
# │ a     ┆ Mazda RX4  │
# │ b     ┆ Merc 280C  │
# │ b     ┆ Merc 450SE │
# └───────┴────────────┘
# )

comparison.value_diffs("disp")
# shape: (2, 3)
# ┌────────┬────────┬────────────────┐
# │ disp_a ┆ disp_b ┆ car            │
# │ ---    ┆ ---    ┆ ---            │
# │ i64    ┆ i64    ┆ str            │
# ╞════════╪════════╪════════════════╡
# │ 259    ┆ 258    ┆ Hornet 4 Drive │
# │ 109    ┆ 108    ┆ Datsun 710     │
# └────────┴────────┴────────────────┘

comparison.value_diffs_stacked(["mpg", "disp"])
# shape: (4, 4)
# ┌────────┬───────┬───────┬────────────────┐
# │ column ┆ val_a ┆ val_b ┆ car            │
# │ ---    ┆ ---   ┆ ---   ┆ ---            │
# │ str    ┆ str   ┆ str   ┆ str            │
# ╞════════╪═══════╪═══════╪════════════════╡
# │ mpg    ┆ 24.4  ┆ 26.4  ┆ Merc 240D      │
# │ mpg    ┆ 14.3  ┆ 16.3  ┆ Duster 360     │
# │ disp   ┆ 109   ┆ 108   ┆ Datsun 710     │
# │ disp   ┆ 259   ┆ 258   ┆ Hornet 4 Drive │
# └────────┴───────┴───────┴────────────────┘

comparison.weave_diffs_wide(["mpg", "disp"])
# shape: (4, 10)
# ┌────────────┬──────────────┬──────────────┬──────┬───┬─────┬──────────────┬──────────────┬─────┐
# │ car        ┆ mpg_a        ┆ mpg_b        ┆ cyl  ┆ … ┆ hp  ┆ drat         ┆ wt           ┆ vs  │
# │ ---        ┆ ---          ┆ ---          ┆ ---  ┆   ┆ --- ┆ ---          ┆ ---          ┆ --- │
# │ str        ┆ decimal[*,1] ┆ decimal[*,1] ┆ i64  ┆   ┆ i64 ┆ decimal[*,2] ┆ decimal[*,2] ┆ i64 │
# ╞════════════╪══════════════╪══════════════╪══════╪═══╪═════╪══════════════╪══════════════╪═════╡
# │ Merc 240D  ┆ 24.4         ┆ 26.4         ┆ 4    ┆ … ┆ 62  ┆ 3.69         ┆ 3.19         ┆ 1   │
# │ Datsun 710 ┆ 22.8         ┆ 22.8         ┆ null ┆ … ┆ 93  ┆ 3.85         ┆ 2.32         ┆ 1   │
# │ Duster 360 ┆ 14.3         ┆ 16.3         ┆ 8    ┆ … ┆ 245 ┆ 3.21         ┆ 3.57         ┆ 0   │
# │ Hornet 4   ┆ 21.4         ┆ 21.4         ┆ 6    ┆ … ┆ 110 ┆ 3.08         ┆ 3.22         ┆ 1   │
# │ Drive      ┆              ┆              ┆      ┆   ┆     ┆              ┆              ┆     │
# └────────────┴──────────────┴──────────────┴──────┴───┴─────┴──────────────┴──────────────┴─────┘

comparison.weave_diffs_long("disp")
# shape: (4, 9)
# ┌───────┬────────────────┬──────────────┬──────┬───┬─────┬──────────────┬──────────────┬─────┐
# │ table ┆ car            ┆ mpg          ┆ cyl  ┆ … ┆ hp  ┆ drat         ┆ wt           ┆ vs  │
# │ ---   ┆ ---            ┆ ---          ┆ ---  ┆   ┆ --- ┆ ---          ┆ ---          ┆ --- │
# │ str   ┆ str            ┆ decimal[*,1] ┆ i64  ┆   ┆ i64 ┆ decimal[*,2] ┆ decimal[*,2] ┆ i64 │
# ╞═══════╪════════════════╪══════════════╪══════╪═══╪═════╪══════════════╪══════════════╪═════╡
# │ a     ┆ Hornet 4 Drive ┆ 21.4         ┆ 6    ┆ … ┆ 110 ┆ 3.08         ┆ 3.22         ┆ 1   │
# │ a     ┆ Datsun 710     ┆ 22.8         ┆ null ┆ … ┆ 93  ┆ 3.85         ┆ 2.32         ┆ 1   │
# │ b     ┆ Hornet 4 Drive ┆ 21.4         ┆ 6    ┆ … ┆ 110 ┆ 3.08         ┆ 3.22         ┆ 1   │
# │ b     ┆ Datsun 710     ┆ 22.8         ┆ null ┆ … ┆ 93  ┆ 3.85         ┆ 2.32         ┆ 1   │
# └───────┴────────────────┴──────────────┴──────┴───┴─────┴──────────────┴──────────────┴─────┘

comparison.slice_diffs("a", "mpg")
# shape: (2, 2)
# ┌────────────┬──────────────┐
# │ car        ┆ mpg          │
# │ ---        ┆ ---          │
# │ str        ┆ decimal[*,1] │
# ╞════════════╪══════════════╡
# │ Duster 360 ┆ 14.3         │
# │ Merc 240D  ┆ 24.4         │
# └────────────┴──────────────┘

comparison.slice_unmatched("b")
# shape: (2, 9)
# ┌────────────┬──────────────┬──────────────┬─────┬───┬──────┬──────┬──────────────┬─────┐
# │ car        ┆ wt           ┆ mpg          ┆ hp  ┆ … ┆ disp ┆ carb ┆ drat         ┆ vs  │
# │ ---        ┆ ---          ┆ ---          ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---          ┆ --- │
# │ str        ┆ decimal[*,2] ┆ decimal[*,1] ┆ i64 ┆   ┆ i64  ┆ i64  ┆ decimal[*,2] ┆ i64 │
# ╞════════════╪══════════════╪══════════════╪═════╪═══╪══════╪══════╪══════════════╪═════╡
# │ Merc 450SE ┆ 4.07         ┆ 16.4         ┆ 180 ┆ … ┆ 276  ┆ 3    ┆ 3.07         ┆ 0   │
# │ Merc 280C  ┆ 3.44         ┆ 17.8         ┆ 123 ┆ … ┆ 168  ┆ 4    ┆ 3.92         ┆ 1   │
# └────────────┴──────────────┴──────────────┴─────┴───┴──────┴──────┴──────────────┴─────┘

comparison.slice_unmatched_both()
# shape: (3, 9)
# ┌───────┬────────────┬──────────────┬─────┬───┬─────┬──────────────┬──────────────┬─────┐
# │ table ┆ car        ┆ mpg          ┆ cyl ┆ … ┆ hp  ┆ drat         ┆ wt           ┆ vs  │
# │ ---   ┆ ---        ┆ ---          ┆ --- ┆   ┆ --- ┆ ---          ┆ ---          ┆ --- │
# │ str   ┆ str        ┆ decimal[*,1] ┆ i64 ┆   ┆ i64 ┆ decimal[*,2] ┆ decimal[*,2] ┆ i64 │
# ╞═══════╪════════════╪══════════════╪═════╪═══╪═════╪══════════════╪══════════════╪═════╡
# │ a     ┆ Mazda RX4  ┆ 21.0         ┆ 6   ┆ … ┆ 110 ┆ 3.90         ┆ 2.62         ┆ 0   │
# │ b     ┆ Merc 280C  ┆ 17.8         ┆ 6   ┆ … ┆ 123 ┆ 3.92         ┆ 3.44         ┆ 1   │
# │ b     ┆ Merc 450SE ┆ 16.4         ┆ 8   ┆ … ┆ 180 ┆ 3.07         ┆ 4.07         ┆ 0   │
# └───────┴────────────┴──────────────┴─────┴───┴─────┴──────────────┴──────────────┴─────┘
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
  results as Polars DataFrames, so you can inspect huge tables without
  blowing up Python memory.
- Inputs stay lazy as well: `compare()` never materialises the full
  source tables in Python.
- Want to kick the tyres quickly? The `versus.examples.example_cars_*`
  helpers used in the quick start are available for ad-hoc testing.

The package exposes the same high-level helpers as the R version
(`value_diffs*`, `weave_diffs*`, `slice_*`), so if you already know the
R API you can continue working the same way here.
# pyversus
