
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
