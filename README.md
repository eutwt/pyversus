# pyversus

`pyversus` (imported as `versus`) compares DuckDB relations
(tables or views), pandas/polars DataFrames, or PySpark DataFrames
without materializing the full inputs in Python. The same `compare()`
function gives a `Comparison` object that shows where the tables
disagree, with methods for displaying the differences. DuckDB remains
the default backend; if either input is a PySpark DataFrame or you pass
`spark=`, the comparison runs on Spark instead.

> **Alpha status:** This package is in active development. Backward
> compatibility is not guaranteed between releases yet.

## Installation

Install the base package:

```bash
pip install pyversus
```

Install PySpark input support when needed:

```bash
pip install "pyversus[spark]"
```

The base install keeps DuckDB as the only required runtime dependency.
If pandas or polars are already installed in your environment,
`compare()` accepts those inputs too. The `.[spark]` extra adds the
dependencies needed for PySpark inputs and mixed Spark/non-Spark
comparisons. Spark-backed comparisons also require a Java runtime.

## Quick start

Here is a typical interactive DuckDB session using `versus.compare()`:

```python
from versus import compare, examples

rel_a = examples.example_cars_a()
rel_b = examples.example_cars_b()

comparison = compare(rel_a, rel_b, by=["car"])
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

A comparison includes:

- `comparison.intersection`: columns in both tables with counts of
  differing values
- `comparison.unmatched_cols`: columns in only one table
- `comparison.unmatched_rows`: rows in only one table with counts per
  table

Use `value_diffs()` to see the values that are different.

```python
comparison.value_diffs("disp")
```

    ┌────────┬────────┬────────────────┐
    │ disp_a │ disp_b │      car       │
    │ int32  │ int32  │    varchar     │
    ├────────┼────────┼────────────────┤
    │    109 │    108 │ Datsun 710     │
    │    259 │    258 │ Hornet 4 Drive │
    └────────┴────────┴────────────────┘

Use `value_diffs_stacked()` to compare multiple columns at once.

```python
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

Use `weave_diffs_*()` to see the differing values in context.

```python
comparison.weave_diffs_wide(["mpg", "disp"])
```

    ┌────────────────┬──────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
    │      car       │    mpg_a     │    mpg_b     │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
    │    varchar     │ decimal(3,1) │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
    ├────────────────┼──────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
    │ Merc 240D      │         24.4 │         26.4 │     4 │    147 │    147 │    62 │         3.69 │         3.19 │     1 │
    │ Duster 360     │         14.3 │         16.3 │     8 │    360 │    360 │   245 │         3.21 │         3.57 │     0 │
    │ Datsun 710     │         22.8 │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
    │ Hornet 4 Drive │         21.4 │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
    └────────────────┴──────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘

```python
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

Use `slice_diffs()` to get the rows with differing values from one
table.

```python
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

Use `slice_unmatched()` to get the unmatched rows from one table.

```python
comparison.slice_unmatched("b")
```

    ┌────────────┬──────────────┬──────────────┬───────┬───────┬───────┬───────┬──────────────┬───────┐
    │    car     │      wt      │     mpg      │  hp   │  cyl  │ disp  │ carb  │     drat     │  vs   │
    │  varchar   │ decimal(3,2) │ decimal(3,1) │ int32 │ int32 │ int32 │ int32 │ decimal(3,2) │ int32 │
    ├────────────┼──────────────┼──────────────┼───────┼───────┼───────┼───────┼──────────────┼───────┤
    │ Merc 280C  │         3.44 │         17.8 │   123 │     6 │   168 │     4 │         3.92 │     1 │
    │ Merc 450SE │         4.07 │         16.4 │   180 │     8 │   276 │     3 │         3.07 │     0 │
    └────────────┴──────────────┴──────────────┴───────┴───────┴───────┴───────┴──────────────┴───────┘

Use `slice_unmatched_both()` to get the unmatched rows from both tables.

```python
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

Use `summary()` to see what kind of differences were found.

```python
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

## Spark quick start

The public API stays the same when you switch to Spark inputs. The main
difference is that helper methods now return Spark DataFrames, so you
inspect them with `.show()` instead of printing DuckDB relations.

```python
from versus import compare, examples

spark = examples.resolve_spark()

spark_comparison = compare(
    examples.example_cars_a_spark(spark),
    examples.example_cars_b_spark(spark),
    by=["car"],
)

spark_comparison.summary().show()
spark_comparison.value_diffs("disp").show()
```

    +--------------+-----+
    |    difference|found|
    +--------------+-----+
    |   value_diffs| true|
    |unmatched_cols| true|
    |unmatched_rows| true|
    |    type_diffs|false|
    +--------------+-----+

    +------+------+--------------+
    |disp_a|disp_b|           car|
    +------+------+--------------+
    |   109|   108|    Datsun 710|
    |   259|   258|Hornet 4 Drive|
    +------+------+--------------+

## Mixed inputs

If either input is a Spark DataFrame, `versus.compare()` chooses the
Spark backend automatically and converts the other supported inputs as
needed.

```python
import pandas as pd

from versus import compare, examples

spark = examples.resolve_spark()

pandas_frame = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
spark_frame = spark.createDataFrame(
    [(1, 10), (2, 22), (3, 30)],
    ["id", "value"],
)

mixed = compare(pandas_frame, spark_frame, by=["id"])
mixed.summary().show()
mixed.value_diffs("value").show()
```

    +--------------+-----+
    |    difference|found|
    +--------------+-----+
    |   value_diffs| true|
    |unmatched_cols|false|
    |unmatched_rows| true|
    |    type_diffs|false|
    +--------------+-----+

    +-------+-------+---+
    |value_a|value_b| id|
    +-------+-------+---+
    |     20|     22|  2|
    +-------+-------+---+

## Usage

- Call `compare()` with DuckDB relations or pandas/polars DataFrames.
  If your relations live on a custom DuckDB connection, pass it via
  `con=` so the comparison queries use the same database.
- Call the same `compare()` function with PySpark DataFrames when you
  want Spark-backed comparisons. You can pass `spark=` to reuse an
  existing `SparkSession` or to force Spark dispatch for mixed inputs.
- If either input is a PySpark DataFrame, `compare()` uses Spark and
  returns Spark-backed helper outputs.
- The `by` columns must uniquely identify rows in each table. When they
  do not, `compare()` raises `ComparisonError` and tells you which key
  values repeat.
- The resulting `Comparison` object stores only metadata and row
  identifiers. Whenever you ask for actual rows (`value_diffs`, slices,
  weave helpers, etc.), the library runs backend-native queries and
  returns backend-native results.
- Need insight into the inputs? `comparison.inputs` exposes a mapping
  from table id (for example, `"a"` and `"b"`) to the input handles.
- Need the row identifiers for unmatched rows?
  `comparison.unmatched_keys` exposes the table id plus `by` columns for
  those keys.
- Inputs stay lazy as well: `compare()` never materializes the full
  source tables in Python.
- Want to kick the tires quickly? The `versus.examples.example_cars_*`
  helpers used above are available for both DuckDB and Spark.

### Materialization

When you call `compare()`, pyversus defines summary tables for the
printed output (`tables`, `by`, `intersection`, `unmatched_cols`,
`unmatched_rows`).

For DuckDB-backed comparisons, these are relation-like wrappers over
DuckDB relations. For Spark-backed comparisons, they are Spark DataFrame
wrappers that cache themselves on first materialization. The input
tables are never materialized by pyversus in Python in any mode.

In full materialization, pyversus also builds a diff table: a single
backend-native relation/DataFrame with the `by` keys plus one boolean
flag per value column indicating a difference. The table only includes
rows with at least one difference. Those precomputed flags let row-level
helpers fetch the differing rows quickly. Other modes skip the diff
table and detect differences inline.

- `materialize="all"`: store the summary tables and the diff table up
  front. This is fastest if you will call row-level helpers multiple
  times.
- `materialize="summary"`: store only the summary tables. Row-level
  helpers run inline predicates and return lazy backend-native objects.
- `materialize="none"`: do not store anything up front. Printing the
  comparison materializes the summary tables on demand.

Row-level helper outputs are always returned as backend-native objects:
DuckDB relations for DuckDB runs and Spark DataFrames for Spark runs.

The package exposes the same high-level helpers as the R version
(`value_diffs*`, `weave_diffs*`, `slice_*`), so if you already know the
R API you can continue working the same way here, including on Spark
inputs.
