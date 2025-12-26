from __future__ import annotations

from typing import Optional, Sequence, TYPE_CHECKING

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
    diff_cols = [col for col in selected if comparison._diff_lookup[col] > 0]
    table_columns = comparison.table_columns[table_name]
    if not diff_cols:
        return h.select_zero_from_table(comparison, table_name, table_columns)
    key_sql = h.collect_diff_keys(comparison, diff_cols)
    return h.fetch_rows_by_keys(comparison, table_name, key_sql, table_columns)


def unmatched_keys_sql(comparison: "Comparison", table_name: str) -> str:
    unmatched_sql = comparison.unmatched_rows.sql_query()
    by_cols = h.select_cols(comparison.by_columns, alias="keys")
    table_filter = f"keys.{h.ident('table')} = {h.sql_literal(table_name)}"
    return f"SELECT {by_cols} FROM ({unmatched_sql}) AS keys WHERE {table_filter}"


def slice_unmatched(comparison: "Comparison", table: str) -> duckdb.DuckDBPyRelation:
    table_name = h.normalize_table_arg(comparison, table)
    key_sql = unmatched_keys_sql(comparison, table_name)
    return h.fetch_rows_by_keys(
        comparison, table_name, key_sql, comparison.table_columns[table_name]
    )


def slice_unmatched_both(comparison: "Comparison") -> duckdb.DuckDBPyRelation:
    out_cols = comparison.by_columns + comparison.common_columns
    select_cols = h.select_cols(out_cols, alias="base")
    join_condition = h.join_condition(comparison.by_columns, "keys", "base")
    selects = []
    for table_name in comparison.table_id:
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
    sql = " UNION ALL ".join(selects)
    return h.run_sql(comparison.connection, sql)
