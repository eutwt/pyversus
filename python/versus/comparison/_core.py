from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
    from typing_extensions import Literal

import duckdb

from . import _helpers as h
from . import _slices, _value_diffs, _weave
from ._exceptions import ComparisonError


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
        tables: duckdb.DuckDBPyRelation,
        by: duckdb.DuckDBPyRelation,
        intersection: duckdb.DuckDBPyRelation,
        unmatched_cols: duckdb.DuckDBPyRelation,
        unmatched_keys: duckdb.DuckDBPyRelation,
        unmatched_rows: duckdb.DuckDBPyRelation,
        common_columns: List[str],
        table_columns: Mapping[str, List[str]],
        diff_keys: Mapping[str, duckdb.DuckDBPyRelation],
        diff_lookup: Dict[str, int],
    ) -> None:
        self.connection = connection
        self._handles = dict(handles)
        self.inputs = {
            identifier: self.connection.sql(f"SELECT * FROM {h.ident(handle.name)}")
            for identifier, handle in self._handles.items()
        }
        self.table_id = table_id
        self.by_columns = by_columns
        self.allow_both_na = allow_both_na
        self.tables = tables
        self.by = by
        self.intersection = intersection
        self.unmatched_cols = unmatched_cols
        self.unmatched_keys = unmatched_keys
        self.unmatched_rows = unmatched_rows
        self.common_columns = common_columns
        self.table_columns = table_columns
        self.diff_keys = diff_keys
        self._diff_lookup = diff_lookup
        self._closed = False

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
    materialize: Literal["all", "summary", "none"] = "all",
) -> Comparison:
    if not isinstance(materialize, str) or materialize not in {
        "all",
        "summary",
        "none",
    }:
        raise ComparisonError("`materialize` must be one of: 'all', 'summary', 'none'")
    materialize_summary = materialize in {"all", "summary"}
    materialize_keys = materialize == "all"

    conn_input = connection
    if conn_input is not None:
        conn_candidate = conn_input
    else:
        default_conn = duckdb.default_connection
        conn_candidate = default_conn() if callable(default_conn) else default_conn
    if not isinstance(conn_candidate, duckdb.DuckDBPyConnection):
        raise ComparisonError("`connection` must be a DuckDB connection.")
    conn = h.VersusConn(conn_candidate)
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

    tables_frame = h.build_tables_frame(conn, handles, clean_ids, materialize_summary)
    by_frame = h.build_by_frame(
        conn, by_columns, handles, clean_ids, materialize_summary
    )
    common_all = [
        col
        for col in handles[clean_ids[0]].columns
        if col in handles[clean_ids[1]].columns
    ]
    value_columns = [col for col in common_all if col not in by_columns]
    unmatched_cols = h.build_unmatched_cols(
        conn, handles, clean_ids, materialize_summary
    )
    diff_keys = h.compute_diff_keys(
        conn,
        handles,
        clean_ids,
        by_columns,
        value_columns,
        allow_both_na,
        materialize_keys,
    )
    intersection, diff_lookup = h.build_intersection_frame(
        value_columns,
        handles,
        clean_ids,
        diff_keys,
        conn,
        materialize_summary,
    )
    unmatched_keys = h.compute_unmatched_keys(
        conn, handles, clean_ids, by_columns, materialize_keys
    )
    unmatched_rows_rel = h.compute_unmatched_rows_summary(
        conn, unmatched_keys, materialize_summary
    )

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
        unmatched_keys=unmatched_keys,
        unmatched_rows=unmatched_rows_rel,
        common_columns=value_columns,
        table_columns={
            identifier: handle.columns[:] for identifier, handle in handles.items()
        },
        diff_keys=diff_keys,
        diff_lookup=diff_lookup,
    )
