from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
    from typing_extensions import Literal

import duckdb

from . import _compute as c
from . import _helpers as h
from . import _slices, _value_diffs, _weave


class Comparison:
    """In-memory description of how two relations differ."""

    def __init__(
        self,
        *,
        connection: h.VersusConn,
        handles: Mapping[str, h._TableHandle],
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
        diff_keys: Mapping[str, duckdb.DuckDBPyRelation],
        diff_lookup: Optional[Dict[str, int]],
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
        self.tables = tables
        self.by = by
        self._intersection = intersection
        self.unmatched_cols = unmatched_cols
        self.unmatched_keys = unmatched_keys
        self.unmatched_rows = unmatched_rows
        self.common_columns = common_columns
        self.table_columns = table_columns
        self.diff_keys = diff_keys
        self._diff_lookup = diff_lookup
        self._tables_materialized = materialize_mode in {"all", "summary"}
        self._by_materialized = materialize_mode in {"all", "summary"}
        self._intersection_materialized = materialize_mode in {"all", "summary"}
        self._unmatched_cols_materialized = materialize_mode in {"all", "summary"}
        self._unmatched_rows_materialized = materialize_mode in {"all", "summary"}
        self._unmatched_lookup: Optional[Dict[str, int]] = None
        self._closed = False

    @property
    def intersection(self) -> duckdb.DuckDBPyRelation:
        return self._intersection

    def _get_diff_lookup(self) -> Dict[str, int]:
        if self._diff_lookup is None:
            if not self._intersection_materialized:
                return {}
            self._diff_lookup = h.diff_lookup_from_intersection(self._intersection)
        return self._diff_lookup

    def _filter_diff_columns(self, columns: Sequence[str]) -> List[str]:
        if not self._intersection_materialized:
            return list(columns)
        diff_lookup = self._get_diff_lookup()
        return [col for col in columns if diff_lookup.get(col, 0) > 0]

    def _get_unmatched_lookup(self) -> Dict[str, int]:
        if not self._unmatched_rows_materialized:
            return {}
        if self._unmatched_lookup is None:
            self._unmatched_lookup = h.unmatched_lookup_from_rows(self.unmatched_rows)
        return self._unmatched_lookup

    def _ensure_tables_materialized(self) -> None:
        if self._tables_materialized:
            return
        self.tables = h.finalize_relation(
            self.connection, self.tables.sql_query(), materialize=True
        )
        self._tables_materialized = True

    def _ensure_by_materialized(self) -> None:
        if self._by_materialized:
            return
        self.by = h.finalize_relation(
            self.connection, self.by.sql_query(), materialize=True
        )
        self._by_materialized = True

    def _ensure_intersection_materialized(self) -> None:
        if self._intersection_materialized:
            return
        self._intersection = h.finalize_relation(
            self.connection, self._intersection.sql_query(), materialize=True
        )
        self._intersection_materialized = True
        self._diff_lookup = h.diff_lookup_from_intersection(self._intersection)

    def _ensure_unmatched_cols_materialized(self) -> None:
        if self._unmatched_cols_materialized:
            return
        self.unmatched_cols = h.finalize_relation(
            self.connection, self.unmatched_cols.sql_query(), materialize=True
        )
        self._unmatched_cols_materialized = True

    def _ensure_unmatched_rows_materialized(self) -> None:
        if self._unmatched_rows_materialized:
            return
        self.unmatched_rows = h.finalize_relation(
            self.connection, self.unmatched_rows.sql_query(), materialize=True
        )
        self._unmatched_rows_materialized = True
        self._unmatched_lookup = h.unmatched_lookup_from_rows(self.unmatched_rows)

    def close(self) -> None:
        if self._closed:
            return
        for view in reversed(self.connection.versus.views):
            try:
                self.connection.execute(f"DROP VIEW IF EXISTS {h.ident(view)}")
            except duckdb.Error:
                pass
        for view in self.connection.versus.temp_tables:
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {h.ident(view)}")
            except duckdb.Error:
                pass
        self._closed = True

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        self._ensure_tables_materialized()
        self._ensure_by_materialized()
        self._ensure_intersection_materialized()
        self._ensure_unmatched_cols_materialized()
        self._ensure_unmatched_rows_materialized()
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
        return _value_diffs.value_diffs(self, column)

    def value_diffs_stacked(
        self, columns: Optional[Sequence[str]] = None
    ) -> duckdb.DuckDBPyRelation:
        return _value_diffs.value_diffs_stacked(self, columns)

    def slice_diffs(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        return _slices.slice_diffs(self, table, columns)

    def slice_unmatched(self, table: str) -> duckdb.DuckDBPyRelation:
        return _slices.slice_unmatched(self, table)

    def slice_unmatched_both(self) -> duckdb.DuckDBPyRelation:
        return _slices.slice_unmatched_both(self)

    def weave_diffs_wide(
        self,
        columns: Optional[Sequence[str]] = None,
        suffix: Optional[Tuple[str, str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        return _weave.weave_diffs_wide(self, columns, suffix)

    def weave_diffs_long(
        self,
        columns: Optional[Sequence[str]] = None,
    ) -> duckdb.DuckDBPyRelation:
        return _weave.weave_diffs_long(self, columns)

    def summary(self) -> duckdb.DuckDBPyRelation:
        """Summarize which difference categories are present."""
        value_diffs = not h.relation_is_empty(
            self.intersection.filter(f"{h.ident('n_diffs')} > 0")
        )
        unmatched_cols = not h.relation_is_empty(self.unmatched_cols)
        unmatched_rows = not h.relation_is_empty(self.unmatched_rows)
        class_a_col = f"class_{self.table_id[0]}"
        class_b_col = f"class_{self.table_id[1]}"
        class_diffs = not h.relation_is_empty(
            self.intersection.filter(
                f"{h.ident(class_a_col)} IS DISTINCT FROM {h.ident(class_b_col)}"
            )
        )
        rows = [
            ("value_diffs", value_diffs),
            ("unmatched_cols", unmatched_cols),
            ("unmatched_rows", unmatched_rows),
            ("class_diffs", class_diffs),
        ]
        schema = [("difference", "VARCHAR"), ("found", "BOOLEAN")]
        summary_rel = h.build_rows_relation(
            self.connection, rows, schema, materialize=False
        )
        return summary_rel


def compare(
    table_a: Any,
    table_b: Any,
    *,
    by: Sequence[str],
    allow_both_na: bool = True,
    coerce: bool = True,
    table_id: Tuple[str, str] = ("a", "b"),
    connection: Optional[duckdb.DuckDBPyConnection] = None,
    materialize: Literal["all", "summary", "none"] = "all",
) -> Comparison:
    materialize_summary, materialize_keys = h.resolve_materialize(materialize)

    conn = h.resolve_connection(connection)
    clean_ids = h.validate_table_id(table_id)
    by_columns = h.normalize_column_list(by, "by", allow_empty=False)
    handles = {
        clean_ids[0]: h.register_input_view(conn, table_a, clean_ids[0]),
        clean_ids[1]: h.register_input_view(conn, table_b, clean_ids[1]),
    }
    h.validate_columns_exist(by_columns, handles, clean_ids)
    if not coerce:
        h.validate_class_compatibility(handles, clean_ids)
    for identifier in clean_ids:
        h.assert_unique_by(conn, handles[identifier], by_columns, identifier)

    tables_frame = c.build_tables_frame(conn, handles, clean_ids, materialize_summary)
    by_frame = c.build_by_frame(
        conn, by_columns, handles, clean_ids, materialize_summary
    )
    common_all = [
        col
        for col in handles[clean_ids[0]].columns
        if col in handles[clean_ids[1]].columns
    ]
    value_columns = [col for col in common_all if col not in by_columns]
    unmatched_cols = c.build_unmatched_cols(
        conn, handles, clean_ids, materialize_summary
    )
    diff_keys = c.compute_diff_keys(
        conn,
        handles,
        clean_ids,
        by_columns,
        value_columns,
        allow_both_na,
        materialize_keys,
    )
    intersection, diff_lookup = c.build_intersection_frame(
        value_columns,
        handles,
        clean_ids,
        diff_keys,
        conn,
        materialize_summary,
    )
    unmatched_keys = c.compute_unmatched_keys(
        conn, handles, clean_ids, by_columns, materialize_keys
    )
    unmatched_rows_rel = c.compute_unmatched_rows_summary(
        conn, unmatched_keys, materialize_summary
    )

    return Comparison(
        connection=conn,
        handles=handles,
        table_id=clean_ids,
        by_columns=by_columns,
        allow_both_na=allow_both_na,
        materialize_mode=materialize,
        tables=tables_frame,
        by=by_frame,
        intersection=intersection,
        unmatched_cols=unmatched_cols,
        unmatched_keys=unmatched_keys,
        unmatched_rows=unmatched_rows_rel,
        common_columns=value_columns,
        table_columns={
            identifier: handle.columns[:] for identifier, handle in handles.items()
        },
        diff_keys=diff_keys,
        diff_lookup=diff_lookup,
    )
