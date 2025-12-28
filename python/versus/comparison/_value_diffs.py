from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import duckdb

from . import _helpers as h

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


def value_diffs(comparison: "Comparison", column: str) -> duckdb.DuckDBPyRelation:
    target_col = h.normalize_single_column(column)
    h.assert_column_allowed(comparison, target_col, "value_diffs")
    if comparison._materialize_mode == "all":
        relation = _value_diffs_with_diff_table(comparison, target_col)
    else:
        relation = _value_diffs_inline(comparison, target_col)
    return relation


def value_diffs_stacked(
    comparison: "Comparison", columns: Optional[Sequence[str]] = None
) -> duckdb.DuckDBPyRelation:
    selected = h.resolve_column_list(comparison, columns, allow_empty=False)
    diff_cols = comparison._filter_diff_columns(selected)
    if not diff_cols:
        return _empty_value_diffs_stacked(comparison, selected)
    if comparison._materialize_mode == "all":

        def stack_fn(column: str) -> str:
            return stack_value_diffs_sql(
                comparison, column, h.collect_diff_keys(comparison, [column])
            )
    else:

        def stack_fn(column: str) -> str:
            return stack_value_diffs_inline_sql(comparison, column)

    selects = [stack_fn(column) for column in diff_cols]
    sql = " UNION ALL ".join(selects)
    return h.run_sql(comparison.connection, sql)


def _value_diffs_with_diff_table(
    comparison: "Comparison", target_col: str
) -> duckdb.DuckDBPyRelation:
    key_sql = h.collect_diff_keys(comparison, [target_col])
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{h.col('a', target_col)} AS {h.ident(f'{target_col}_{table_a}')}",
        f"{h.col('b', target_col)} AS {h.ident(f'{target_col}_{table_b}')}",
        h.select_cols(comparison.by_columns, alias="keys"),
    ]
    join_a = h.join_condition(comparison.by_columns, "keys", "a")
    join_b = h.join_condition(comparison.by_columns, "keys", "b")
    sql = f"""
    SELECT
      {", ".join(select_cols)}
    FROM
      ({key_sql}) AS keys
      JOIN {h.table_ref(comparison._handles[table_a])} AS a
        ON {join_a}
      JOIN {h.table_ref(comparison._handles[table_b])} AS b
        ON {join_b}
    """
    return h.run_sql(comparison.connection, sql)


def _value_diffs_inline(
    comparison: "Comparison", target_col: str
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{h.col('a', target_col)} AS {h.ident(f'{target_col}_{table_a}')}",
        f"{h.col('b', target_col)} AS {h.ident(f'{target_col}_{table_b}')}",
        h.select_cols(comparison.by_columns, alias="a"),
    ]
    join_sql = h.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = h.diff_predicate(target_col, comparison.allow_both_na, "a", "b")
    sql = f"""
    SELECT
      {", ".join(select_cols)}
    FROM
      {join_sql}
    WHERE
      {predicate}
    """
    return h.run_sql(comparison.connection, sql)


def stack_value_diffs_sql(
    comparison: "Comparison",
    column: str,
    key_sql: str,
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
    return f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      ({key_sql}) AS keys
      JOIN {h.table_ref(comparison._handles[table_a])} AS a
        ON {join_a}
      JOIN {h.table_ref(comparison._handles[table_b])} AS b
        ON {join_b}
    """


def stack_value_diffs_inline_sql(comparison: "Comparison", column: str) -> str:
    table_a, table_b = comparison.table_id
    select_parts = [
        f"{h.sql_literal(column)} AS {h.ident('column')}",
        f"{h.col('a', column)} AS {h.ident(f'val_{table_a}')}",
        f"{h.col('b', column)} AS {h.ident(f'val_{table_b}')}",
        h.select_cols(comparison.by_columns, alias="a"),
    ]
    join_sql = h.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = h.diff_predicate(column, comparison.allow_both_na, "a", "b")
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
            f"CAST(NULL AS {handle_a.types[by_col]}) AS {h.ident(by_col)}"
            for by_col in by_columns
        ]
        select_parts = [
            f"{h.sql_literal(column)} AS {h.ident('column')}",
            f"CAST(NULL AS {type_a}) AS {h.ident(f'val_{table_a}')}",
            f"CAST(NULL AS {type_b}) AS {h.ident(f'val_{table_b}')}",
            *by_parts,
        ]
        return f"SELECT {', '.join(select_parts)} LIMIT 0"

    sql = " UNION ALL ".join(select_for(column) for column in columns)
    return h.run_sql(comparison.connection, sql)
