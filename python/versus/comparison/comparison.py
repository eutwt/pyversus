from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import duckdb

from . import _exceptions as e
from . import _relations as r
from . import _slices as l
from . import _sql as q
from . import _summary as s
from . import _types as t
from . import _value_diffs as d
from . import _weave as w


class Comparison:
    """In-memory description of how two relations differ.

    Provides summary relations plus helper methods to retrieve the exact
    differences without materializing the full input tables.
    """

    def __init__(
        self,
        *,
        connection: t.VersusConn,
        handles: Mapping[str, t._TableHandle],
        table_id: Tuple[str, str],
        by_columns: List[str],
        allow_both_na: bool,
        materialize_mode: str,
        tables: duckdb.DuckDBPyRelation,
        by: duckdb.DuckDBPyRelation,
        intersection: duckdb.DuckDBPyRelation,
        unmatched_cols: duckdb.DuckDBPyRelation,
        unmatched_keys: duckdb.DuckDBPyRelation,
        unmatched_rows: duckdb.DuckDBPyRelation,
        common_columns: List[str],
        table_columns: Mapping[str, List[str]],
        diff_table: Optional[duckdb.DuckDBPyRelation],
        diff_lookup: Optional[Dict[str, int]],
        unmatched_lookup: Optional[Dict[str, int]],
    ) -> None:
        self.connection = connection
        self._handles = dict(handles)
        self.inputs = {
            identifier: handle.relation for identifier, handle in self._handles.items()
        }
        self.table_id = table_id
        self.by_columns = by_columns
        self.allow_both_na = allow_both_na
        self._materialize_mode = materialize_mode
        self._diff_lookup = diff_lookup
        self._unmatched_lookup = unmatched_lookup
        summary_materialized = materialize_mode in {"all", "summary"}
        self.tables = s.SummaryRelation(
            connection, tables, materialized=summary_materialized
        )
        self.by = s.SummaryRelation(connection, by, materialized=summary_materialized)
        self.intersection = s.SummaryRelation(
            connection,
            intersection,
            materialized=summary_materialized,
            on_materialize=self._store_diff_lookup,
        )
        self.unmatched_cols = s.SummaryRelation(
            connection, unmatched_cols, materialized=summary_materialized
        )
        self.unmatched_keys = unmatched_keys
        self.unmatched_rows = s.SummaryRelation(
            connection,
            unmatched_rows,
            materialized=summary_materialized,
            on_materialize=self._store_unmatched_lookup,
        )
        self.common_columns = common_columns
        self.table_columns = table_columns
        if materialize_mode == "all" and diff_table is None:
            raise e.ComparisonError("Diff table is required when materialize='all'.")
        self.diff_table = diff_table
        self._closed = False

    def _filter_diff_columns(self, columns: Sequence[str]) -> List[str]:
        diff_lookup = self._diff_lookup
        if diff_lookup is None:
            return list(columns)
        return [col for col in columns if diff_lookup[col] > 0]

    def _store_diff_lookup(self, relation: duckdb.DuckDBPyRelation) -> None:
        if self._diff_lookup is None:
            self._diff_lookup = r.diff_lookup_from_intersection(relation)

    def _store_unmatched_lookup(self, relation: duckdb.DuckDBPyRelation) -> None:
        if self._unmatched_lookup is None:
            self._unmatched_lookup = r.unmatched_lookup_from_rows(relation)

    def close(self) -> None:
        """Release any temporary views or tables created for the comparison.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.close()
        """
        if self._closed:
            return
        for view in reversed(self.connection.versus.views):
            try:
                self.connection.execute(f"DROP VIEW IF EXISTS {q.ident(view)}")
            except duckdb.Error:
                pass
        for view in self.connection.versus.temp_tables:
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {q.ident(view)}")
            except duckdb.Error:
                pass
        self._closed = True

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return (
            "Comparison("
            f"tables=\n{self.tables}\n"
            f"by=\n{self.by}\n"
            f"intersection=\n{self.intersection}\n"
            f"unmatched_cols=\n{self.unmatched_cols}\n"
            f"unmatched_rows=\n{self.unmatched_rows}\n"
            ")"
        )

    def value_diffs(self, column: str) -> duckdb.DuckDBPyRelation:
        """Return rows where a single column differs between the tables.

        Parameters
        ----------
        column : str
            Column name to compare.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with the differing values plus the `by` columns.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.value_diffs("disp")
        ┌────────┬────────┬────────────────┐
        │ disp_a │ disp_b │      car       │
        │ int32  │ int32  │    varchar     │
        ├────────┼────────┼────────────────┤
        │    109 │    108 │ Datsun 710     │
        │    259 │    258 │ Hornet 4 Drive │
        └────────┴────────┴────────────────┘
        """
        return d.value_diffs(self, column)

    def value_diffs_stacked(
        self, columns: Optional[Sequence[str]] = None
    ) -> duckdb.DuckDBPyRelation:
        """Return a stacked view of value differences for multiple columns.

        Parameters
        ----------
        columns : sequence of str, optional
            Columns to compare. Defaults to all comparable columns.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with `column`, `val_<table_id>`, and `by` columns.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
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
        """
        return d.value_diffs_stacked(self, columns)

    def slice_diffs(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return rows from one table that differ in the selected columns.

        Parameters
        ----------
        table : str
            Table identifier to return (one of `table_id`).
        columns : sequence of str, optional
            Columns to check for differences. Defaults to all comparable columns.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with the full schema of the requested table.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.slice_diffs("a", ["mpg"])
        ┌────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
        │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
        │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
        ├────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
        │ Duster 360 │         14.3 │     8 │   360 │   245 │         3.21 │         3.57 │     0 │     0 │
        │ Merc 240D  │         24.4 │     4 │   147 │    62 │         3.69 │         3.19 │     1 │     0 │
        └────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘
        """
        return l.slice_diffs(self, table, columns)

    def slice_unmatched(self, table: str) -> duckdb.DuckDBPyRelation:
        """Return rows from one table whose keys are missing in the other.

        Parameters
        ----------
        table : str
            Table identifier to return (one of `table_id`).

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with unmatched rows from the requested table.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.slice_unmatched("a")
        ┌───────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┬───────┐
        │    car    │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │  am   │
        │  varchar  │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │ int32 │
        ├───────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┼───────┤
        │ Mazda RX4 │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │     1 │
        └───────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┴───────┘
        """
        return l.slice_unmatched(self, table)

    def slice_unmatched_both(self) -> duckdb.DuckDBPyRelation:
        """Return unmatched rows from both tables.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with `table_name` plus key and common columns.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.slice_unmatched_both()
        ┌────────────┬────────────┬──────────────┬───────┬───────┬───────┬──────────────┬──────────────┬───────┐
        │ table_name │    car     │     mpg      │  cyl  │ disp  │  hp   │     drat     │      wt      │  vs   │
        │  varchar   │  varchar   │ decimal(3,1) │ int32 │ int32 │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
        ├────────────┼────────────┼──────────────┼───────┼───────┼───────┼──────────────┼──────────────┼───────┤
        │ a          │ Mazda RX4  │         21.0 │     6 │   160 │   110 │         3.90 │         2.62 │     0 │
        │ b          │ Merc 280C  │         17.8 │     6 │   168 │   123 │         3.92 │         3.44 │     1 │
        │ b          │ Merc 450SE │         16.4 │     8 │   276 │   180 │         3.07 │         4.07 │     0 │
        └────────────┴────────────┴──────────────┴───────┴───────┴───────┴──────────────┴──────────────┴───────┘
        """
        return l.slice_unmatched_both(self)

    def weave_diffs_wide(
        self,
        columns: Optional[Sequence[str]] = None,
        suffix: Optional[Tuple[str, str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a wide view of differing rows with split columns.

        Parameters
        ----------
        columns : sequence of str, optional
            Columns to compare. Defaults to all comparable columns.
        suffix : tuple[str, str], optional
            Suffixes appended to differing columns from table A and B.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with key columns and common columns, where differing
            columns are split into `<name><suffix>`.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
        >>> comparison.weave_diffs_wide(["disp"])
        ┌────────────────┬──────────────┬───────┬────────┬────────┬───────┬──────────────┬──────────────┬───────┐
        │      car       │     mpg      │  cyl  │ disp_a │ disp_b │  hp   │     drat     │      wt      │  vs   │
        │    varchar     │ decimal(3,1) │ int32 │ int32  │ int32  │ int32 │ decimal(3,2) │ decimal(3,2) │ int32 │
        ├────────────────┼──────────────┼───────┼────────┼────────┼───────┼──────────────┼──────────────┼───────┤
        │ Datsun 710     │         22.8 │  NULL │    109 │    108 │    93 │         3.85 │         2.32 │     1 │
        │ Hornet 4 Drive │         21.4 │     6 │    259 │    258 │   110 │         3.08 │         3.22 │     1 │
        └────────────────┴──────────────┴───────┴────────┴────────┴───────┴──────────────┴──────────────┴───────┘
        """
        return w.weave_diffs_wide(self, columns, suffix)

    def weave_diffs_long(
        self,
        columns: Optional[Sequence[str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        """Return a long view of differing rows stacked by table.

        Parameters
        ----------
        columns : sequence of str, optional
            Columns to compare. Defaults to all comparable columns.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with `table_name` plus key and common columns.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
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
        """
        return w.weave_diffs_long(self, columns)

    def summary(self) -> duckdb.DuckDBPyRelation:
        """Summarize which difference categories are present.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Relation with `difference` and `found` columns.

        Examples
        --------
        >>> from versus import compare, examples
        >>> comparison = compare(
        ...     examples.example_cars_a(),
        ...     examples.example_cars_b(),
        ...     by=["car"],
        ... )
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
        """
        value_diffs = not r.relation_is_empty(
            self.intersection.filter(f"{q.ident('n_diffs')} > 0")
        )
        unmatched_cols = not r.relation_is_empty(self.unmatched_cols)
        unmatched_rows = not r.relation_is_empty(
            self.unmatched_rows.filter(f"{q.ident('n_unmatched')} > 0")
        )
        type_a_col = f"type_{self.table_id[0]}"
        type_b_col = f"type_{self.table_id[1]}"
        type_diffs = not r.relation_is_empty(
            self.intersection.filter(
                f"{q.ident(type_a_col)} IS DISTINCT FROM {q.ident(type_b_col)}"
            )
        )
        rows = [
            ("value_diffs", value_diffs),
            ("unmatched_cols", unmatched_cols),
            ("unmatched_rows", unmatched_rows),
            ("type_diffs", type_diffs),
        ]
        schema = [("difference", "VARCHAR"), ("found", "BOOLEAN")]
        summary_rel = s.build_rows_relation(
            self.connection, rows, schema, materialize=False
        )
        return summary_rel
