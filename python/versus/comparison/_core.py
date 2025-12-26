from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import duckdb

from ._exceptions import ComparisonError
from . import _helpers as h
from . import _slices
from . import _value_diffs
from . import _weave


class Comparison:
    """In-memory description of how two relations differ."""

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        handles: Mapping[str, h._TableHandle],
        table_id: Tuple[str, str],
        by_columns: List[str],
        allow_both_na: bool,
        tables: duckdb.DuckDBPyRelation,
        by: duckdb.DuckDBPyRelation,
        intersection: duckdb.DuckDBPyRelation,
        unmatched_cols: duckdb.DuckDBPyRelation,
        unmatched_rows: duckdb.DuckDBPyRelation,
        common_columns: List[str],
        table_columns: Mapping[str, List[str]],
        diff_key_tables: Mapping[str, str],
        temp_tables: Sequence[str],
        diff_lookup: Dict[str, int],
    ) -> None:
        self.connection = connection
        self._handles = dict(handles)
        self._handles_view = MappingProxyType(self._handles)
        self.table_id = table_id
        self.by_columns = by_columns
        self.allow_both_na = allow_both_na
        self.tables = tables
        self.by = by
        self.intersection = intersection
        self.unmatched_cols = unmatched_cols
        self.unmatched_rows = unmatched_rows
        self.common_columns = common_columns
        self.table_columns = table_columns
        self.diff_key_tables = diff_key_tables
        self.diff_rows = diff_key_tables
        self._temp_tables = list(temp_tables)
        self._diff_lookup = diff_lookup
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        for handle in self._handles.values():
            try:
                handle.cleanup()
            except duckdb.Error:
                pass
        for view in self._temp_tables:
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
        return (
            "Comparison("
            f"tables=\n{self.tables}\n"
            f"by=\n{self.by}\n"
            f"intersection=\n{self.intersection}\n"
            f"unmatched_cols=\n{self.unmatched_cols}\n"
            f"unmatched_rows=\n{self.unmatched_rows}\n"
            ")"
        )

    @property
    def handles(self) -> Mapping[str, h._TableHandle]:
        return self._handles_view

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
        summary_rel, _ = h.build_rows_relation(
            self.connection, rows, schema, materialize=True
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
    materialize: bool = True,
) -> Comparison:
    conn_input = connection
    if conn_input is None:
        default_conn = duckdb.default_connection
        conn_candidate = default_conn() if callable(default_conn) else default_conn
    else:
        conn_candidate = conn_input
    if not isinstance(conn_candidate, duckdb.DuckDBPyConnection):
        raise ComparisonError("`connection` must be a DuckDB connection.")
    conn = conn_candidate
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
        h.ensure_unique_by(conn, handles[identifier], by_columns, identifier)

    tables_frame, tables_table = h.build_tables_frame(
        conn, handles, clean_ids, materialize
    )
    by_frame, by_table = h.build_by_frame(
        conn, by_columns, handles, clean_ids, materialize
    )
    common_all = [
        col
        for col in handles[clean_ids[0]].columns
        if col in handles[clean_ids[1]].columns
    ]
    value_columns = [col for col in common_all if col not in by_columns]
    unmatched_cols, unmatched_cols_table = h.build_unmatched_cols(
        conn, handles, clean_ids, materialize
    )
    diff_tables = h.compute_diff_key_tables(
        conn, handles, clean_ids, by_columns, value_columns, allow_both_na
    )
    diff_key_handles = {col: diff_tables[col] for col in value_columns}
    intersection, diff_lookup, intersection_table = h.build_intersection_frame(
        value_columns, handles, clean_ids, diff_key_handles, conn, materialize
    )
    unmatched_rows_rel, unmatched_keys_table = h.compute_unmatched_rows(
        conn, handles, clean_ids, by_columns, materialize
    )
    temp_tables = list(diff_tables.values()) + [
        name
        for name in [
            tables_table,
            by_table,
            unmatched_cols_table,
            intersection_table,
            unmatched_keys_table,
        ]
        if name is not None
    ]

    return Comparison(
        connection=conn,
        handles=handles,
        table_id=clean_ids,
        by_columns=by_columns,
        allow_both_na=allow_both_na,
        tables=tables_frame,
        by=by_frame,
        intersection=intersection,
        unmatched_cols=unmatched_cols,
        unmatched_rows=unmatched_rows_rel,
        common_columns=value_columns,
        table_columns={
            identifier: handle.columns[:] for identifier, handle in handles.items()
        },
        diff_key_tables=diff_key_handles,
        temp_tables=temp_tables,
        diff_lookup=diff_lookup,
    )
