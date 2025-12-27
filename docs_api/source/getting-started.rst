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

`materialize` controls how much data is computed eagerly:

- `all`: build summary tables and diff keys up front for fastest helpers.
- `summary`: build summary tables, but compute diff keys lazily.
- `none`: keep summaries lazy until printed or queried.
