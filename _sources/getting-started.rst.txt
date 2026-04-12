Install & basics
================

Installation
------------

Install the package from the repository:

.. code-block:: bash

   pip install -e .

Install PySpark input support when needed:

.. code-block:: bash

   pip install -e ".[spark]"

The base install keeps DuckDB as the only required runtime dependency.
If pandas or polars are already available in your environment,
`compare()` accepts those inputs too. Spark-backed comparisons also need
PySpark plus a Java runtime.

Inputs
------

`compare()` accepts DuckDB relations (tables or views), pandas
DataFrames, polars DataFrames, and PySpark DataFrames. If you provide
relations created on a non-default DuckDB connection, pass that
connection into `compare()` via `con=` so helper queries run in the same
session.

If either input is a PySpark DataFrame, or if you pass `spark=`, the
comparison runs on Spark and returns Spark-backed outputs while keeping
the same public `versus.compare()` entry point.

.. code-block:: pycon

   >>> import duckdb
   >>> from versus import compare
   >>> rel_a = duckdb.sql("SELECT 1 AS id, 10 AS value")
   >>> rel_b = duckdb.sql("SELECT 1 AS id, 12 AS value")
   >>> comparison = compare(rel_a, rel_b, by=["id"])
   >>> comparison.summary()
   ┌────────────────┬─────────┐
   │   difference   │  found  │
   │    varchar     │ boolean │
   ├────────────────┼─────────┤
   │ value_diffs    │ true    │
   │ unmatched_cols │ false   │
   │ unmatched_rows │ false   │
   │ type_diffs     │ false   │
   └────────────────┴─────────┘

Spark inputs
------------

When Spark is selected, the same comparison helpers return Spark
DataFrames.

.. code-block:: pycon

   >>> from versus import examples
   >>> spark = examples.resolve_spark()
   >>> left = spark.createDataFrame([(1, 10), (2, 20)], ["id", "value"])
   >>> right = spark.createDataFrame([(1, 10), (2, 22), (3, 30)], ["id", "value"])
   >>> comparison = compare(left, right, by=["id"])
   >>> comparison.summary().show()
   +--------------+-----+
   |    difference|found|
   +--------------+-----+
   |   value_diffs| true|
   |unmatched_cols|false|
   |unmatched_rows| true|
   |    type_diffs|false|
   +--------------+-----+
   >>> comparison.value_diffs("value").show()
   +-------+-------+---+
   |value_a|value_b| id|
   +-------+-------+---+
   |     20|     22|  2|
   +-------+-------+---+

Materialization modes
---------------------

When you call `compare()`, `pyversus2` defines summary tables for the
printed output (`tables`, `by`, `intersection`, `unmatched_cols`,
`unmatched_rows`).

For DuckDB-backed comparisons, these are relation-like wrappers over
DuckDB relations. For Spark-backed comparisons, they are Spark DataFrame
wrappers that cache themselves on first materialization. The input
tables are never materialized by `pyversus2` in Python in any mode.

In full materialization, `pyversus2` also builds a diff table: a single
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
