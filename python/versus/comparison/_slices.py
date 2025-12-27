from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import duckdb

from . import _helpers as h

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


def slice_diffs(
    comparison: "Comparison",
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    table_name = h.normalize_table_arg(comparison, table)
    selected = h.resolve_column_list(comparison, columns)
    diff_cols = comparison._filter_diff_columns(selected)
    if not diff_cols:
        return h.select_zero_from_table(comparison, table_name)
    if comparison._materialize_mode == "all":
        relation = _slice_diffs_with_keys(comparison, table_name, diff_cols)
    else:
        relation = _slice_diffs_inline(comparison, table_name, diff_cols)
    return relation


def _slice_diffs_with_keys(
    comparison: "Comparison", table_name: str, diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    key_sql = h.collect_diff_keys(comparison, diff_cols)
    return h.fetch_rows_by_keys(comparison, table_name, key_sql)


def _slice_diffs_inline(
    comparison: "Comparison", table_name: str, diff_cols: Sequence[str]
) -> duckdb.DuckDBPyRelation:
    table_a, table_b = comparison.table_id
    base_alias = "a" if table_name == table_a else "b"
    join_sql = h.join_clause(
        comparison._handles, comparison.table_id, comparison.by_columns
    )
    predicate = " OR ".join(
        h.diff_predicate(col, comparison.allow_both_na, "a", "b") for col in diff_cols
    )
    sql = f"SELECT {base_alias}.* {join_sql} WHERE {predicate}"
    return h.run_sql(comparison.connection, sql)


def unmatched_keys_sql(comparison: "Comparison", table_name: str) -> str:
    unmatched_sql = comparison.unmatched_keys.sql_query()
    by_cols = h.select_cols(comparison.by_columns, alias="keys")
    table_filter = f"keys.{h.ident('table')} = {h.sql_literal(table_name)}"
    return f"SELECT {by_cols} FROM ({unmatched_sql}) AS keys WHERE {table_filter}"


def slice_unmatched(comparison: "Comparison", table: str) -> duckdb.DuckDBPyRelation:
    table_name = h.normalize_table_arg(comparison, table)
    if comparison._unmatched_rows_materialized:
        unmatched_lookup = comparison._get_unmatched_lookup()
        if unmatched_lookup.get(table_name, 0) == 0:
            return h.select_zero_from_table(comparison, table_name)
    key_sql = unmatched_keys_sql(comparison, table_name)
    return h.fetch_rows_by_keys(comparison, table_name, key_sql)


def slice_unmatched_both(comparison: "Comparison") -> duckdb.DuckDBPyRelation:
    out_cols = comparison.by_columns + comparison.common_columns
    select_cols = h.select_cols(out_cols, alias="base")
    join_condition = h.join_condition(comparison.by_columns, "keys", "base")
    selects = []
    if comparison._unmatched_rows_materialized:
        unmatched_lookup = comparison._get_unmatched_lookup()
    else:
        unmatched_lookup = None
    for table_name in comparison.table_id:
        if unmatched_lookup is not None and unmatched_lookup.get(table_name, 0) == 0:
            continue
        keys_sql = unmatched_keys_sql(comparison, table_name)
        base_table = comparison._handles[table_name].name
        selects.append(
            f"""
                SELECT {h.sql_literal(table_name)} AS table, {select_cols}
                FROM {h.ident(base_table)} AS base
                JOIN ({keys_sql}) AS keys
                  ON {join_condition}
                """
        )
    if not selects:
        base = h.select_zero_from_table(comparison, comparison.table_id[0], out_cols)
        relation = base.query(
            "base",
            (
                f"SELECT {h.sql_literal(comparison.table_id[0])} AS table, "
                f"{h.select_cols(out_cols)} FROM base"
            ),
        )
        return relation
    sql = " UNION ALL ".join(selects)
    return h.run_sql(comparison.connection, sql)
