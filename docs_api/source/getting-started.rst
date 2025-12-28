Getting started
===============

Installation
------------

Clone the repository and install it into your environment:

.. code-block:: bash

   pip install -e .

The only runtime dependency is DuckDB.

Inputs
------

`compare()` accepts DuckDB relations (tables or views) or pandas/polars
DataFrames. To compare a SQL query, create a relation with
`connection.sql(...)` and pass it to `compare()`. If you provide relations
created on a non-default connection, pass that connection into `compare()` so
helper queries run in the same session.

.. code-block:: pycon

   >>> import duckdb
   >>> from versus import compare
   >>> con = duckdb.connect()
   >>> rel_a = con.sql("SELECT 1 AS id, 10 AS value")
   >>> rel_b = con.sql("SELECT 1 AS id, 12 AS value")
   >>> comparison = compare(rel_a, rel_b, by=["id"], connection=con)
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

Materialization modes
---------------------

When you call `compare()`, Pyversus defines summary tables for the
printed output (`tables`, `by`, `intersection`, `unmatched_cols`,
`unmatched_rows`). These are relation-like wrappers that materialize
themselves on print. The input tables are never materialized by Pyversus
in any mode; they stay as DuckDB relations and are queried lazily.

In full materialization, Pyversus also builds a diff table: a single
relation with the `by` keys plus one boolean flag per value column
indicating a difference. The table only includes rows with at least one
difference. Those precomputed flags let row-level helpers fetch the exact
differing rows quickly via joins, which can be faster when you call
multiple helpers. Other modes skip the diff table and detect differences
inline.

- `materialize="all"`: store the summary tables and the diff table as
  temp tables. This is fastest if you will call row-level helpers
  multiple times.
- `materialize="summary"`: store only the summary tables. Row-level
  helpers run inline predicates and return lazy relations.
- `materialize="none"`: do not store anything up front. Printing the
  comparison materializes the summary tables.

Row-level helper outputs are always returned as DuckDB relations and are
never materialized automatically; materialize them explicitly if needed.
