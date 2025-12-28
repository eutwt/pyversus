from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import duckdb

from . import _sql as q
from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import Comparison


def value_diffs(comparison: "Comparison", column: str) -> duckdb.DuckDBPyRelation:
    target_col = v.normalize_single_column(column)
    v.assert_column_allowed(comparison, target_col, "value_diffs")
    if comparison._materialize_mode == "all":
        relation = _value_diffs_with_diff_table(comparison, target_col)
    else:
        relation = _value_diffs_inline(comparison, target_col)
    return relation


def value_diffs_stacked(
    comparison: "Comparison", columns: Optional[Sequence[str]] = None
) -> duckdb.DuckDBPyRelation:
    selected = v.resolve_column_list(comparison, columns, allow_empty=False)
    diff_cols = comparison._filter_diff_columns(selected)
    if not diff_cols:
        return _empty_value_diffs_stacked(comparison, selected)
    if comparison._materialize_mode == "all":

        def stack_fn(column: str) -> str:
            return stack_value_diffs_sql(
                comparison, column, q.collect_diff_keys(comparison, [column])
            )
    else:

        def stack_fn(column: str) -> str:
            return stack_value_diffs_inline_sql(comparison, column)

    selects = [stack_fn(column) for column in diff_cols]
    sql = " UNION ALL ".join(selects)
    return q.run_sql(comparison.connection, sql)


def _value_diffs_with_diff_table(
    comparison: "Comparison", target_col: str
) -> duckdb.DuckDBPyRelation:
    key_sql = q.collect_diff_keys(comparison, [target_col])
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{q.col('a', target_col)} AS {q.ident(f'{target_col}_{table_a}')}",
        f"{q.col('b', target_col)} AS {q.ident(f'{target_col}_{table_b}')}",
        q.select_cols(comparison.by_columns, alias="keys"),
    ]
    join_a = q.join_condition(comparison.by_columns, "keys", "a")
    join_b = q.join_condition(comparison.by_columns, "keys", "b")
    sql = f"""
    SELECT
      {", ".join(select_cols)}
    FROM
      ({key_sql}) AS keys
      JOIN {q.table_ref(comparison._handles[table_a])} AS a
        ON {join_a}
      JOIN {q.table_ref(comparison._handles[table_b])} AS b
        ON {join_b}
    """
    return q.run_sql(comparison.connection, sql)


def _value_diffs_inline(
    comparison: "Comparison", target_col: str
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{q.col('a', target_col)} AS {q.ident(f'{target_col}_{table_a}')}",
        f"{q.col('b', target_col)} AS {q.ident(f'{target_col}_{table_b}')}",
        q.select_cols(comparison.by_columns, alias="a"),
    ]
    join_sql = q.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = q.diff_predicate(target_col, comparison.allow_both_na, "a", "b")
    sql = f"""
    SELECT
      {", ".join(select_cols)}
    FROM
      {join_sql}
    WHERE
      {predicate}
    """
    return q.run_sql(comparison.connection, sql)


def stack_value_diffs_sql(
    comparison: "Comparison",
    column: str,
    key_sql: str,
) -> str:
    table_a, table_b = comparison.table_id
    by_columns = comparison.by_columns
    select_parts = [
        f"{q.sql_literal(column)} AS {q.ident('column')}",
        f"{q.col('a', column)} AS {q.ident(f'val_{table_a}')}",
        f"{q.col('b', column)} AS {q.ident(f'val_{table_b}')}",
        q.select_cols(by_columns, alias="keys"),
    ]
    join_a = q.join_condition(by_columns, "keys", "a")
    join_b = q.join_condition(by_columns, "keys", "b")
    return f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      ({key_sql}) AS keys
      JOIN {q.table_ref(comparison._handles[table_a])} AS a
        ON {join_a}
      JOIN {q.table_ref(comparison._handles[table_b])} AS b
        ON {join_b}
    """


def stack_value_diffs_inline_sql(comparison: "Comparison", column: str) -> str:
    table_a, table_b = comparison.table_id
    select_parts = [
        f"{q.sql_literal(column)} AS {q.ident('column')}",
        f"{q.col('a', column)} AS {q.ident(f'val_{table_a}')}",
        f"{q.col('b', column)} AS {q.ident(f'val_{table_b}')}",
        q.select_cols(comparison.by_columns, alias="a"),
    ]
    join_sql = q.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = q.diff_predicate(column, comparison.allow_both_na, "a", "b")
    return f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      {join_sql}
    WHERE
      {predicate}
    """


def _empty_value_diffs_stacked(
    comparison: "Comparison", columns: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    handle_a = comparison._handles[table_a]
    handle_b = comparison._handles[table_b]
    by_columns = comparison.by_columns

    def select_for(column: str) -> str:
        type_a = handle_a.types[column]
        type_b = handle_b.types[column]
        by_parts = [
            f"CAST(NULL AS {handle_a.types[by_col]}) AS {q.ident(by_col)}"
            for by_col in by_columns
        ]
        select_parts = [
            f"{q.sql_literal(column)} AS {q.ident('column')}",
            f"CAST(NULL AS {type_a}) AS {q.ident(f'val_{table_a}')}",
            f"CAST(NULL AS {type_b}) AS {q.ident(f'val_{table_b}')}",
            *by_parts,
        ]
        return f"SELECT {', '.join(select_parts)} LIMIT 0"

    sql = " UNION ALL ".join(select_for(column) for column in columns)
    return q.run_sql(comparison.connection, sql)
