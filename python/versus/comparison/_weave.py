from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

import duckdb

from . import _exceptions as e
from . import _sql as q
from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import Comparison


def weave_diffs_wide(
    comparison: "Comparison",
    columns: Optional[Sequence[str]] = None,
    suffix: Optional[Tuple[str, str]] = None,
) -> duckdb.DuckDBPyRelation:
    selected = v.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    if not diff_cols:
        return q.select_zero_from_table(comparison, table_a, out_cols)
    if comparison._materialize_mode == "all":
        relation = _weave_diffs_wide_with_keys(comparison, diff_cols, suffix)
    else:
        relation = _weave_diffs_wide_inline(comparison, diff_cols, suffix)
    return relation


def weave_diffs_long(
    comparison: "Comparison",
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    selected = v.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    if not diff_cols:
        base = q.select_zero_from_table(comparison, table_a, out_cols)
        relation = base.query(
            "base",
            (
                f"SELECT {q.sql_literal(table_a)} AS table_name, "
                f"{q.select_cols(out_cols)} FROM base"
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
                f"{q.col('a', column)} AS {q.ident(f'{column}{suffix[0]}')}",
                f"{q.col('b', column)} AS {q.ident(f'{column}{suffix[1]}')}",
            ]
        return [q.col("a", column)]

    by_parts = [q.col("a", column) for column in comparison.by_columns]
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
    suffix = resolve_suffix(suffix, comparison.table_id)
    keys = q.collect_diff_keys(comparison, diff_cols)
    select_parts = _weave_select_parts(comparison, diff_cols, suffix)
    join_a = q.join_condition(comparison.by_columns, "keys", "a")
    join_b = q.join_condition(comparison.by_columns, "keys", "b")
    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      ({keys}) AS keys
      JOIN {q.table_ref(comparison._handles[table_a])} AS a
        ON {join_a}
      JOIN {q.table_ref(comparison._handles[table_b])} AS b
        ON {join_b}
    """
    return q.run_sql(comparison.connection, sql)


def _weave_diffs_wide_inline(
    comparison: "Comparison",
    diff_cols: Sequence[str],
    suffix: Optional[Tuple[str, str]],
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    suffix = resolve_suffix(suffix, comparison.table_id)
    select_parts = _weave_select_parts(comparison, diff_cols, suffix)
    join_sql = q.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        q.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    sql = f"""
    SELECT
      {", ".join(select_parts)}
    FROM
      {join_sql}
    WHERE
      {predicate}
    """
    return q.run_sql(comparison.connection, sql)


def _weave_diffs_long_with_keys(
    comparison: "Comparison", diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    keys = q.collect_diff_keys(comparison, diff_cols)
    table_column = q.ident("table_name")
    select_cols_a = q.select_cols(out_cols, alias="a")
    select_cols_b = q.select_cols(out_cols, alias="b")
    join_a = q.join_condition(comparison.by_columns, "keys", "a")
    join_b = q.join_condition(comparison.by_columns, "keys", "b")
    order_cols = q.select_cols(comparison.by_columns)
    sql = f"""
    WITH
      keys AS (
        {keys}
      )
    SELECT
      {table_column},
      {q.select_cols(out_cols)}
    FROM
      (
        SELECT
          0 AS __table_order,
          '{table_a}' AS {table_column},
          {select_cols_a}
        FROM
          keys
          JOIN {q.table_ref(comparison._handles[table_a])} AS a
            ON {join_a}
        UNION ALL
        SELECT
          1 AS __table_order,
          '{table_b}' AS {table_column},
          {select_cols_b}
        FROM
          keys
          JOIN {q.table_ref(comparison._handles[table_b])} AS b
            ON {join_b}
      ) AS stacked
    ORDER BY
      {order_cols},
      __table_order
    """
    return q.run_sql(comparison.connection, sql)


def _weave_diffs_long_inline(
    comparison: "Comparison", diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    out_cols = comparison.by_columns + comparison.common_columns
    table_column = q.ident("table_name")
    select_cols_a = q.select_cols(out_cols, alias="a")
    select_cols_b = q.select_cols(out_cols, alias="b")
    join_sql = q.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        q.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    order_cols = q.select_cols(comparison.by_columns)
    sql = f"""
    SELECT
      {table_column},
      {q.select_cols(out_cols)}
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
    return q.run_sql(comparison.connection, sql)


# ------- helpers
def resolve_suffix(
    suffix: Optional[Tuple[str, str]], table_id: Tuple[str, str]
) -> Tuple[str, str]:
    if suffix is None:
        return (f"_{table_id[0]}", f"_{table_id[1]}")
    if (
        not isinstance(suffix, (tuple, list))
        or len(suffix) != 2
        or not all(isinstance(item, str) for item in suffix)
    ):
        raise e.ComparisonError("`suffix` must be a tuple of two strings or None")
    if suffix[0] == suffix[1]:
        raise e.ComparisonError("Entries of `suffix` must be distinct")
    return (suffix[0], suffix[1])
