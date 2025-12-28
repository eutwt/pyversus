from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

import duckdb

from . import _helpers as h

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


def weave_diffs_wide(
    comparison: "Comparison",
    columns: Optional[Sequence[str]] = None,
    suffix: Optional[Tuple[str, str]] = None,
) -> duckdb.DuckDBPyRelation:
    selected = h.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    if not diff_cols:
        return h.select_zero_from_table(comparison, table_a, out_cols)
    if comparison._materialize_mode == "all":
        relation = _weave_diffs_wide_with_keys(comparison, diff_cols, suffix)
    else:
        relation = _weave_diffs_wide_inline(comparison, diff_cols, suffix)
    return relation


def weave_diffs_long(
    comparison: "Comparison",
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    selected = h.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    if not diff_cols:
        base = h.select_zero_from_table(comparison, table_a, out_cols)
        relation = base.query(
            "base",
            (
                f"SELECT {h.sql_literal(table_a)} AS table_name, "
                f"{h.select_cols(out_cols)} FROM base"
            ),
        )
        return relation
    if comparison._materialize_mode == "all":
        relation = _weave_diffs_long_with_keys(comparison, diff_cols)
    else:
        relation = _weave_diffs_long_inline(comparison, diff_cols)
    return relation


def _weave_select_parts(
    comparison: "Comparison",
    diff_cols: Sequence[str],
    suffix: Tuple[str, str],
) -> List[str]:
    diff_set = set(diff_cols)

    def parts_for(column: str) -> List[str]:
        if column in diff_set:
            return [
                f"{h.col('a', column)} AS {h.ident(f'{column}{suffix[0]}')}",
                f"{h.col('b', column)} AS {h.ident(f'{column}{suffix[1]}')}",
            ]
        return [h.col("a", column)]

    by_parts = [h.col("a", column) for column in comparison.by_columns]
    common_parts = list(
        chain.from_iterable(parts_for(column) for column in comparison.common_columns)
    )
    return by_parts + common_parts


def _weave_diffs_wide_with_keys(
    comparison: "Comparison",
    diff_cols: Sequence[str],
    suffix: Optional[Tuple[str, str]],
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    suffix = h.resolve_suffix(suffix, comparison.table_id)
    keys = h.collect_diff_keys(comparison, diff_cols)
    select_parts = _weave_select_parts(comparison, diff_cols, suffix)
    join_a = h.join_condition(comparison.by_columns, "keys", "a")
    join_b = h.join_condition(comparison.by_columns, "keys", "b")
    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      ({keys}) AS keys
      JOIN {h.ident(comparison._handles[table_a].name)} AS a
        ON {join_a}
      JOIN {h.ident(comparison._handles[table_b].name)} AS b
        ON {join_b}
    """
    return h.run_sql(comparison.connection, sql)


def _weave_diffs_wide_inline(
    comparison: "Comparison",
    diff_cols: Sequence[str],
    suffix: Optional[Tuple[str, str]],
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    suffix = h.resolve_suffix(suffix, comparison.table_id)
    select_parts = _weave_select_parts(comparison, diff_cols, suffix)
    join_sql = h.join_clause(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        h.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      {join_sql}
    WHERE
      {predicate}
    """
    return h.run_sql(comparison.connection, sql)


def _weave_diffs_long_with_keys(
    comparison: "Comparison", diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    keys = h.collect_diff_keys(comparison, diff_cols)
    table_column = h.ident("table_name")
    select_cols_a = h.select_cols(out_cols, alias="a")
    select_cols_b = h.select_cols(out_cols, alias="b")
    join_a = h.join_condition(comparison.by_columns, "keys", "a")
    join_b = h.join_condition(comparison.by_columns, "keys", "b")
    order_cols = h.select_cols(comparison.by_columns)
    sql = f"""
    WITH
      keys AS (
        {keys}
      )
    SELECT
      {table_column},
      {h.select_cols(out_cols)}
    FROM
      (
        SELECT
          0 AS __table_order,
          '{table_a}' AS {table_column},
          {select_cols_a}
        FROM
          keys
          JOIN {h.ident(comparison._handles[table_a].name)} AS a
            ON {join_a}
        UNION ALL
        SELECT
          1 AS __table_order,
          '{table_b}' AS {table_column},
          {select_cols_b}
        FROM
          keys
          JOIN {h.ident(comparison._handles[table_b].name)} AS b
            ON {join_b}
      ) AS stacked
    ORDER BY
      {order_cols},
      __table_order
    """
    return h.run_sql(comparison.connection, sql)


def _weave_diffs_long_inline(
    comparison: "Comparison", diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    table_column = h.ident("table_name")
    select_cols_a = h.select_cols(out_cols, alias="a")
    select_cols_b = h.select_cols(out_cols, alias="b")
    join_sql = h.join_clause(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        h.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    order_cols = h.select_cols(comparison.by_columns)
    sql = f"""
    SELECT
      {table_column},
      {h.select_cols(out_cols)}
    FROM
      (
        SELECT
          0 AS __table_order,
          '{table_a}' AS {table_column},
          {select_cols_a}
        FROM
          {join_sql}
        WHERE
          {predicate}
        UNION ALL
        SELECT
          1 AS __table_order,
          '{table_b}' AS {table_column},
          {select_cols_b}
        FROM
          {join_sql}
        WHERE
          {predicate}
      ) AS stacked
    ORDER BY
      {order_cols},
      __table_order
    """
    return h.run_sql(comparison.connection, sql)
