Quick start
===========

DuckDB-powered tools for comparing two relations (tables or views) or
pandas/polars DataFrames without materializing them in Python.

.. admonition:: Alpha status

   This package is still evolving and may change between releases.

The two relations below are used as an example to demonstrate functionality.

.. code-block:: pycon

   >>> from versus import compare, examples
   >>> example_a = examples.example_cars_a()
   >>> example_b = examples.example_cars_b()

Use `compare()` to create a comparison of two tables.

A comparison contains:

- `comparison.intersection`: columns in both tables and rows with differing values
- `comparison.unmatched_cols`: columns in only one table
- `comparison.unmatched_rows`: rows in only one table

.. code-block:: pycon

   >>> comparison = compare(example_a, example_b, by=["car"])
   >>> comparison
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

Use `value_diffs()` to see the values that are different.

.. code-block:: pycon

   >>> comparison.value_diffs("disp")
   ┌────────┬────────┬────────────────┐
   │ disp_a │ disp_b │      car       │
   │ int32  │ int32  │    varchar     │
   ├────────┼────────┼────────────────┤
   │    109 │    108 │ Datsun 710     │
   │    259 │    258 │ Hornet 4 Drive │
   └────────┴────────┴────────────────┘

.. code-block:: pycon

   >>> comparison.value_diffs_stacked(["mpg", "disp"])
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

.. code-block:: pycon

   >>> comparison.weave_diffs_wide(["disp"])
   ┌────────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
   │      car       │     mpg      │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
   │    varchar     │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
   ├────────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
   │ Datsun 710     │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
   │ Hornet 4 Drive │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
   └────────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘

.. code-block:: pycon

   >>> comparison.weave_diffs_wide(["mpg", "disp"])
   ┌────────────────┬──────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
   │      car       │    mpg_a     │    mpg_b     │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
   │    varchar     │ decimal(3,1) │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
   ├────────────────┼──────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
   │ Duster 360     │         14.3 │         16.3 │     8 │    360 │    360 │   245 │         3.21 │         3.57 │     0 │
   │ Datsun 710     │         22.8 │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
   │ Merc 240D      │         24.4 │         26.4 │     4 │    147 │    147 │    62 │         3.69 │         3.19 │     1 │
   │ Hornet 4 Drive │         21.4 │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
   └────────────────┴──────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘

.. code-block:: pycon

   >>> comparison.weave_diffs_long(["disp"])
   ┌────────────┬────────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
   │ table_name │      car       │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
   │  varchar   │    varchar     │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
   ├────────────┼────────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
   │ a          │ Datsun 710     │         22.8 │  NULL │   109 │    93 │         3.85 │         2.32 │     1 │
   │ b          │ Datsun 710     │         22.8 │  NULL │   108 │    93 │         3.85 │         2.32 │     1 │
   │ a          │ Hornet 4 Drive │         21.4 │     6 │   259 │   110 │         3.08 │         3.22 │     1 │
   │ b          │ Hornet 4 Drive │         21.4 │     6 │   258 │   110 │         3.08 │         3.22 │     1 │
   └────────────┴────────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

Use `slice_diffs()` to get the rows with differing values from one table.

.. code-block:: pycon

   >>> comparison.slice_diffs("a", ["mpg"])
   ┌────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
   │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
   │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
   ├────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
   │ Duster 360 │         14.3 │     8 │   360 │   245 │         3.21 │         3.57 │     0 │     0 │
   │ Merc 240D  │         24.4 │     4 │   147 │    62 │         3.69 │         3.19 │     1 │     0 │
   └────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘

Use `slice_unmatched()` to get the unmatched rows from one or both tables.

.. code-block:: pycon

   >>> comparison.slice_unmatched("a")
   ┌───────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
   │    car    │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
   │  varchar  │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
   ├───────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
   │ Mazda RX4 │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │     1 │
   └───────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘

.. code-block:: pycon

   >>> comparison.slice_unmatched_both()
   ┌────────────┬────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
   │ table_name │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
   │  varchar   │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
   ├────────────┼────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
   │ a          │ Mazda RX4  │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │
   │ b          │ Merc 280C  │         17.8 │     6 │   168 │   123 │         3.92 │         3.44 │     1 │
   │ b          │ Merc 450SE │         16.4 │     8 │   276 │   180 │         3.07 │         4.07 │     0 │
   └────────────┴────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘

Use `summary()` to see what kind of differences were found.

.. code-block:: pycon

   >>> comparison.summary()
   ┌────────────────┬─────────┐
   │   difference   │  found  │
   │    varchar     │ boolean │
   ├────────────────┼─────────┤
   │ value_diffs    │ true    │
   │ unmatched_cols │ true    │
   │ unmatched_rows │ true    │
   │ type_diffs     │ false   │
   └────────────────┴─────────┘

.. toctree::
   :maxdepth: 2

   getting-started
   reference/compare
   reference/comparison
   reference/errors
   reference/examples
