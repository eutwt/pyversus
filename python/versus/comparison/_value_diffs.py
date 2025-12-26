from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import duckdb

from . import _helpers as h

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


def value_diffs(comparison: "Comparison", column: str) -> duckdb.DuckDBPyRelation:
    target_col = h.normalize_single_column(column)
    h.ensure_column_allowed(comparison, target_col, "value_diffs")
    key_relation = comparison.diff_key_tables[target_col]
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{h.col('a', target_col)} AS {h.ident(f'{target_col}_{table_a}')}",
        f"{h.col('b', target_col)} AS {h.ident(f'{target_col}_{table_b}')}",
        h.select_cols(comparison.by_columns, alias="keys"),
    ]
    join_a = h.join_condition(comparison.by_columns, "keys", "a")
    join_b = h.join_condition(comparison.by_columns, "keys", "b")
    key_sql = key_relation.sql_query()
    sql = f"""
    SELECT {', '.join(select_cols)}
    FROM ({key_sql}) AS keys
    JOIN {h.ident(comparison._handles[table_a].name)} AS a
      ON {join_a}
    JOIN {h.ident(comparison._handles[table_b].name)} AS b
      ON {join_b}
    """
    return h.run_sql(comparison.connection, sql)


def value_diffs_stacked(
    comparison: "Comparison", columns: Optional[Sequence[str]] = None
) -> duckdb.DuckDBPyRelation:
    selected = h.resolve_column_list(comparison, columns, allow_empty=False)
    selects = [
        stack_value_diffs_sql(comparison, column, comparison.diff_key_tables[column])
        for column in selected
    ]
    sql = " UNION ALL ".join(selects)
    return h.run_sql(comparison.connection, sql)


def stack_value_diffs_sql(
    comparison: "Comparison",
    column: str,
    key_relation: duckdb.DuckDBPyRelation,
) -> str:
    table_a, table_b = comparison.table_id
    by_columns = comparison.by_columns
    select_parts = [
        f"{h.sql_literal(column)} AS {h.ident('column')}",
        f"{h.col('a', column)} AS {h.ident(f'val_{table_a}')}",
        f"{h.col('b', column)} AS {h.ident(f'val_{table_b}')}",
        h.select_cols(by_columns, alias="keys"),
    ]
    join_a = h.join_condition(by_columns, "keys", "a")
    join_b = h.join_condition(by_columns, "keys", "b")
    key_sql = key_relation.sql_query()
    return f"""
    SELECT {', '.join(select_parts)}
    FROM ({key_sql}) AS keys
    JOIN {h.ident(comparison._handles[table_a].name)} AS a
      ON {join_a}
    JOIN {h.ident(comparison._handles[table_b].name)} AS b
      ON {join_b}
    """
