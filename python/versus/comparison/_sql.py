from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Sequence, Tuple, Union

import duckdb

from ._exceptions import ComparisonError
from ._types import _TableHandle, VersusConn

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import Comparison


def ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def col(alias: str, column: str) -> str:
    return f"{alias}.{ident(column)}"


def table_ref(handle: _TableHandle) -> str:
    if handle.source_is_identifier:
        return ident(handle.source_sql)
    return f"({handle.source_sql})"


def select_cols(columns: Sequence[str], alias: Optional[str] = None) -> str:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    if alias is None:
        return ", ".join(ident(column) for column in columns)
    return ", ".join(col(alias, column) for column in columns)


def join_condition(by_columns: List[str], left_alias: str, right_alias: str) -> str:
    comparisons = [
        f"{col(left_alias, column)} IS NOT DISTINCT FROM {col(right_alias, column)}"
        for column in by_columns
    ]
    return " AND ".join(comparisons) if comparisons else "TRUE"


def inputs_join_sql(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
) -> str:
    join_condition_sql = join_condition(by_columns, "a", "b")
    return (
        f"{table_ref(handles[table_id[0]])} AS a\n"
        f"  INNER JOIN {table_ref(handles[table_id[1]])} AS b\n"
        f"    ON {join_condition_sql}"
    )


def diff_predicate(
    column: str, allow_both_na: bool, left_alias: str, right_alias: str
) -> str:
    left = col(left_alias, column)
    right = col(right_alias, column)
    if allow_both_na:
        return f"{left} IS DISTINCT FROM {right}"
    return f"(({left} IS NULL AND {right} IS NULL) OR {left} IS DISTINCT FROM {right})"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def run_sql(
    conn: Union[VersusConn, duckdb.DuckDBPyConnection],
    sql: str,
) -> duckdb.DuckDBPyRelation:
    return conn.sql(sql)


def require_diff_table(
    comparison: "Comparison",
) -> duckdb.DuckDBPyRelation:
    diff_table = comparison.diff_table
    if diff_table is None:
        raise ComparisonError("Diff table is only available for materialize='all'.")
    return diff_table


def collect_diff_keys(comparison: "Comparison", columns: Sequence[str]) -> str:
    diff_table = require_diff_table(comparison)
    diff_table_sql = diff_table.sql_query()
    by_cols = select_cols(comparison.by_columns, alias="diffs")
    predicate = " OR ".join(f"diffs.{ident(column)}" for column in columns)
    return f"""
    SELECT
      {by_cols}
    FROM
      ({diff_table_sql}) AS diffs
    WHERE
      {predicate}
    """


def fetch_rows_by_keys(
    comparison: "Comparison",
    table: str,
    key_sql: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    if columns is None:
        select_cols_sql = "base.*"
    else:
        if not columns:
            raise ComparisonError("Column list must be non-empty")
        select_cols_sql = select_cols(columns, alias="base")
    join_condition_sql = join_condition(comparison.by_columns, "keys", "base")
    sql = f"""
    SELECT
      {select_cols_sql}
    FROM
      ({key_sql}) AS keys
      JOIN {table_ref(comparison._handles[table])} AS base
        ON {join_condition_sql}
    """
    return run_sql(comparison.connection, sql)


def select_zero_from_table(
    comparison: "Comparison",
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    handle = comparison._handles[table]
    if columns is None:
        sql = f"SELECT * FROM {table_ref(handle)} LIMIT 0"
        return run_sql(comparison.connection, sql)
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    select_cols_sql = select_cols(columns)
    sql = f"SELECT {select_cols_sql} FROM {table_ref(handle)} LIMIT 0"
    return run_sql(comparison.connection, sql)
