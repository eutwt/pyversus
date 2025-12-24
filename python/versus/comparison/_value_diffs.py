from __future__ import annotations

from typing import Optional, Sequence, TYPE_CHECKING

import duckdb

from ._helpers import (
    _col,
    _ensure_column_allowed,
    _ident,
    _join_condition,
    _normalize_single_column,
    _resolve_column_list,
    _run_sql,
    _select_cols,
    _sql_literal,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


def value_diffs(comparison: "Comparison", column: str) -> duckdb.DuckDBPyRelation:
    target_col = _normalize_single_column(column)
    _ensure_column_allowed(comparison, target_col, "value_diffs")
    key_table = comparison.diff_key_tables[target_col]
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{_col('a', target_col)} AS {_ident(f'{target_col}_{table_a}')}",
        f"{_col('b', target_col)} AS {_ident(f'{target_col}_{table_b}')}",
        _select_cols(comparison.by_columns, alias="keys"),
    ]
    join_a = _join_condition(comparison.by_columns, "keys", "a")
    join_b = _join_condition(comparison.by_columns, "keys", "b")
    sql = f"""
    SELECT {', '.join(select_cols)}
    FROM {_ident(key_table)} AS keys
    JOIN {_ident(comparison._handles[table_a].name)} AS a
      ON {join_a}
    JOIN {_ident(comparison._handles[table_b].name)} AS b
      ON {join_b}
    """
    return _run_sql(comparison.connection, sql)


def value_diffs_stacked(
    comparison: "Comparison", columns: Optional[Sequence[str]] = None
) -> duckdb.DuckDBPyRelation:
    selected = _resolve_column_list(comparison, columns, allow_empty=False)
    selects = [
        _stack_value_diffs_sql(comparison, column, comparison.diff_key_tables[column])
        for column in selected
    ]
    sql = " UNION ALL ".join(selects)
    return _run_sql(comparison.connection, sql)


def _stack_value_diffs_sql(
    comparison: "Comparison",
    column: str,
    key_table: str,
) -> str:
    table_a, table_b = comparison.table_id
    by_columns = comparison.by_columns
    select_parts = [
        f"{_sql_literal(column)} AS {_ident('column')}",
        f"{_col('a', column)} AS {_ident(f'val_{table_a}')}",
        f"{_col('b', column)} AS {_ident(f'val_{table_b}')}",
        _select_cols(by_columns, alias="keys"),
    ]
    join_a = _join_condition(by_columns, "keys", "a")
    join_b = _join_condition(by_columns, "keys", "b")
    return f"""
    SELECT {', '.join(select_parts)}
    FROM {_ident(key_table)} AS keys
    JOIN {_ident(comparison._handles[table_a].name)} AS a
      ON {join_a}
    JOIN {_ident(comparison._handles[table_b].name)} AS b
      ON {join_b}
    """
