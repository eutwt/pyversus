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

`compare()` accepts DuckDB relations (tables, views, or SQL queries). If you
provide relations created on a non-default connection, pass that connection into
`compare()` so helper queries run in the same session.

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
  comparison materializes the summary tables and enables diff-count and
  unmatched-row optimizations for later row-level helpers, but helper
  outputs stay lazy.

Row-level helper outputs are always returned as DuckDB relations and are
never materialized automatically; materialize them explicitly if needed.
