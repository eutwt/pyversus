from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb

from . import _sql as q
from ._exceptions import ComparisonError
from ._types import VersusConn, _TableHandle

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import Comparison


def resolve_materialize(materialize: str) -> Tuple[bool, bool]:
    if not isinstance(materialize, str) or materialize not in {
        "all",
        "summary",
        "none",
    }:
        raise ComparisonError("`materialize` must be one of: 'all', 'summary', 'none'")
    materialize_summary = materialize in {"all", "summary"}
    materialize_keys = materialize == "all"
    return materialize_summary, materialize_keys


def resolve_connection(
    connection: Optional[duckdb.DuckDBPyConnection],
) -> VersusConn:
    if connection is not None:
        conn_candidate = connection
    else:
        default_conn = duckdb.default_connection
        conn_candidate = default_conn() if callable(default_conn) else default_conn
    if not isinstance(conn_candidate, duckdb.DuckDBPyConnection):
        raise ComparisonError("`con` must be a DuckDB connection.")
    return VersusConn(conn_candidate)


def validate_columns_exist(
    by_columns: Iterable[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    missing_a = [col for col in by_columns if col not in handles[table_id[0]].columns]
    missing_b = [col for col in by_columns if col not in handles[table_id[1]].columns]
    if missing_a:
        raise ComparisonError(
            f"`by` columns not found in `{table_id[0]}`: {', '.join(missing_a)}"
        )
    if missing_b:
        raise ComparisonError(
            f"`by` columns not found in `{table_id[1]}`: {', '.join(missing_b)}"
        )


def validate_type_compatibility(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    shared = set(handles[table_id[0]].columns) & set(handles[table_id[1]].columns)
    for column in shared:
        type_a = handles[table_id[0]].types.get(column)
        type_b = handles[table_id[1]].types.get(column)
        if type_a != type_b:
            raise ComparisonError(
                f"`coerce=False` requires compatible types. Column `{column}` has types `{type_a}` vs `{type_b}`."
            )


def validate_columns(columns: Sequence[str], label: str) -> None:
    if not all(isinstance(column, str) for column in columns):
        raise ComparisonError(f"`{label}` must have string column names")
    counts = Counter(columns)
    duplicates = [name for name, count in counts.items() if count > 1]
    if duplicates:
        dupes = ", ".join(duplicates)
        raise ComparisonError(f"`{label}` has duplicate column names: {dupes}")


def validate_tables(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    *,
    coerce: bool,
) -> None:
    validate_columns_exist(by_columns, handles, table_id)
    for identifier in table_id:
        validate_columns(handles[identifier].columns, identifier)
    if not coerce:
        validate_type_compatibility(handles, table_id)
    for identifier in table_id:
        assert_unique_by(conn, handles[identifier], by_columns, identifier)


def assert_unique_by(
    conn: VersusConn,
    handle: _TableHandle,
    by_columns: List[str],
    identifier: str,
) -> None:
    cols = q.select_cols(by_columns, alias="t")
    sql = f"""
    SELECT
      {cols},
      COUNT(*) AS n
    FROM
      {q.table_ref(handle)} AS t
    GROUP BY
      {cols}
    HAVING
      COUNT(*) > 1
    LIMIT
      1
    """
    rel = q.run_sql(conn, sql)
    rows = rel.fetchall()
    if rows:
        first = rows[0]
        values = ", ".join(f"{col}={first[i]!r}" for i, col in enumerate(by_columns))
        raise ComparisonError(
            f"`{identifier}` has more than one row for by values ({values})"
        )


def validate_table_id(table_id: Tuple[str, str]) -> Tuple[str, str]:
    if (
        not isinstance(table_id, (tuple, list))
        or len(table_id) != 2
        or not all(isinstance(val, str) for val in table_id)
    ):
        raise ComparisonError("`table_id` must be a tuple of two strings")
    first, second = table_id[0], table_id[1]
    if not first.strip() or not second.strip():
        raise ComparisonError("Entries of `table_id` must be non-empty strings")
    if first == second:
        raise ComparisonError("Entries of `table_id` must be distinct")
    return (first, second)


def normalize_column_list(
    columns: Sequence[str],
    arg_name: str,
    *,
    allow_empty: bool,
) -> List[str]:
    if isinstance(columns, str):
        parsed = [columns]
    else:
        try:
            parsed = list(columns)
        except TypeError as exc:
            raise ComparisonError(
                f"`{arg_name}` must be a sequence of column names"
            ) from exc
    if not parsed and not allow_empty:
        raise ComparisonError(f"`{arg_name}` must contain at least one column")
    if not all(isinstance(item, str) for item in parsed):
        raise ComparisonError(f"`{arg_name}` must only contain strings")
    return parsed


def normalize_table_arg(comparison: "Comparison", table: str) -> str:
    if table not in comparison.table_id:
        allowed = ", ".join(comparison.table_id)
        raise ComparisonError(f"`table` must be one of: {allowed}")
    return table


def normalize_single_column(column: str) -> str:
    if isinstance(column, str):
        return column
    raise ComparisonError("`column` must be a column name")


def resolve_column_list(
    comparison: "Comparison",
    columns: Optional[Sequence[str]],
    *,
    allow_empty: bool = True,
) -> List[str]:
    if columns is None:
        parsed = comparison.common_columns[:]
    else:
        cols = normalize_column_list(columns, "column", allow_empty=True)
        if not cols:
            raise ComparisonError("`columns` must select at least one column")
        missing = [col for col in cols if col not in comparison.common_columns]
        if missing:
            raise ComparisonError(
                f"Columns not part of the comparison: {', '.join(missing)}"
            )
        parsed = cols
    if not parsed and not allow_empty:
        raise ComparisonError("`columns` must select at least one column")
    return parsed


def assert_column_allowed(comparison: "Comparison", column: str, func: str) -> None:
    if column not in comparison.common_columns:
        raise ComparisonError(
            f"`{func}` can only reference columns in both tables: {column}"
        )
