from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import duckdb

from . import _sql as q
from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
    from .comparison import Comparison


def slice_diffs(
    comparison: "Comparison",
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    table_name = v.normalize_table_arg(comparison, table)
    selected = v.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    if not diff_cols:
        return q.select_zero_from_table(comparison, table_name)
    if comparison._materialize_mode == "all":
        relation = _slice_diffs_with_keys(comparison, table_name, diff_cols)
    else:
        relation = _slice_diffs_inline(comparison, table_name, diff_cols)
    return relation


def _slice_diffs_with_keys(
    comparison: "Comparison", table_name: str, diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    key_sql = q.collect_diff_keys(comparison, diff_cols)
    return q.fetch_rows_by_keys(comparison, table_name, key_sql)


def _slice_diffs_inline(
    comparison: "Comparison", table_name: str, diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    base_alias = "a" if table_name == table_a else "b"
    join_sql = q.inputs_join_sql(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        q.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    sql = f"""
    SELECT
      {base_alias}.*
    FROM
      {join_sql}
    WHERE
      {predicate}
    """
    return q.run_sql(comparison.connection, sql)


def build_unmatched_keys_sql(comparison: "Comparison", table_name: str) -> str:
    unmatched_keys_sql = comparison.unmatched_keys.sql_query()
    by_cols = q.select_cols(comparison.by_columns, alias="keys")
    table_filter = f"keys.{q.ident('table_name')} = {q.sql_literal(table_name)}"
    return f"SELECT {by_cols} FROM ({unmatched_keys_sql}) AS keys WHERE {table_filter}"


def slice_unmatched(comparison: "Comparison", table: str) -> duckdb.DuckDBPyRelation:
    table_name = v.normalize_table_arg(comparison, table)
    unmatched_lookup = comparison._unmatched_lookup
    if unmatched_lookup is not None and unmatched_lookup[table_name] == 0:
        return q.select_zero_from_table(comparison, table_name)
    key_sql = build_unmatched_keys_sql(comparison, table_name)
    return q.fetch_rows_by_keys(comparison, table_name, key_sql)


def slice_unmatched_both(comparison: "Comparison") -> duckdb.DuckDBPyRelation:
    out_cols = comparison.by_columns + comparison.common_columns
    select_cols = q.select_cols(out_cols, alias="base")
    join_condition = q.join_condition(comparison.by_columns, "keys", "base")
    unmatched_lookup = comparison._unmatched_lookup
    table_names = [
        table_name
        for table_name in comparison.table_id
        if unmatched_lookup is None or unmatched_lookup[table_name] != 0
    ]

    def select_for(table_name: str) -> str:
        unmatched_keys_sql = build_unmatched_keys_sql(comparison, table_name)
        base_table = comparison._handles[table_name]
        return f"""
        SELECT
          {q.sql_literal(table_name)} AS table_name,
          {select_cols}
        FROM
          {q.table_ref(base_table)} AS base
          JOIN ({unmatched_keys_sql}) AS keys
            ON {join_condition}
        """

    selects = [select_for(table_name) for table_name in table_names]
    if not selects:
        base = q.select_zero_from_table(comparison, comparison.table_id[0], out_cols)
        sql = f"""
        SELECT
          {q.sql_literal(comparison.table_id[0])} AS table_name,
          {q.select_cols(out_cols)}
        FROM
          base
        """
        relation = base.query("base", sql)
        return relation
    sql = " UNION ALL ".join(selects)
    return q.run_sql(comparison.connection, sql)
