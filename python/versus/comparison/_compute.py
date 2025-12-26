from __future__ import annotations

from typing import Dict, List, Mapping, Tuple

import duckdb

from . import _helpers as h
from ._exceptions import ComparisonError


def build_tables_frame(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    rows = []
    for identifier in table_id:
        handle = handles[identifier]
        result = conn.sql(
            f"SELECT COUNT(*) AS n FROM {h.ident(handle.name)}"
        ).fetchone()
        if result is None:
            raise ComparisonError("Failed to count rows for comparison table metadata")
        count = result[0]
        rows.append((identifier, count, len(handle.columns)))
    schema = [
        ("table", "VARCHAR"),
        ("nrow", "BIGINT"),
        ("ncol", "BIGINT"),
    ]
    return h.build_rows_relation(conn, rows, schema, materialize)


def build_by_frame(
    conn: h.VersusConn,
    by_columns: List[str],
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    rows = []
    first, second = table_id
    for column in by_columns:
        rows.append(
            (
                column,
                handles[first].types.get(column, ""),
                handles[second].types.get(column, ""),
            )
        )
    schema = [
        ("column", "VARCHAR"),
        (f"class_{first}", "VARCHAR"),
        (f"class_{second}", "VARCHAR"),
    ]
    return h.build_rows_relation(conn, rows, schema, materialize)


def build_unmatched_cols(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    rows = []
    first, second = table_id
    cols_first = set(handles[first].columns)
    cols_second = set(handles[second].columns)
    for column in sorted(cols_first - cols_second):
        rows.append((first, column, handles[first].types[column]))
    for column in sorted(cols_second - cols_first):
        rows.append((second, column, handles[second].types[column]))
    schema = [
        ("table", "VARCHAR"),
        ("column", "VARCHAR"),
        ("class", "VARCHAR"),
    ]
    return h.build_rows_relation(conn, rows, schema, materialize)


def build_intersection_frame(
    value_columns: List[str],
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    diff_keys: Mapping[str, duckdb.DuckDBPyRelation],
    conn: h.VersusConn,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Dict[str, int]]:
    rows = []
    diff_lookup: Dict[str, int] = {}
    first, second = table_id
    for column in value_columns:
        relation = diff_keys[column]
        count = h.table_count(conn, relation)
        rows.append(
            (
                column,
                count,
                handles[first].types.get(column, ""),
                handles[second].types.get(column, ""),
            )
        )
        diff_lookup[column] = count
    schema = [
        ("column", "VARCHAR"),
        ("n_diffs", "BIGINT"),
        (f"class_{first}", "VARCHAR"),
        (f"class_{second}", "VARCHAR"),
    ]
    relation = h.build_rows_relation(conn, rows, schema, materialize)
    return relation, diff_lookup


def compute_diff_keys(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    value_columns: List[str],
    allow_both_na: bool,
    materialize: bool,
) -> Dict[str, duckdb.DuckDBPyRelation]:
    diff_keys: Dict[str, duckdb.DuckDBPyRelation] = {}
    join_sql = h.join_clause(handles, table_id, by_columns)
    select_by = h.select_cols(by_columns, alias="a")
    for column in value_columns:
        predicate = h.diff_predicate(column, allow_both_na, "a", "b")
        sql = f"SELECT {select_by} {join_sql} WHERE {predicate}"
        relation = h.finalize_relation(conn, sql, materialize)
        diff_keys[column] = relation
    return diff_keys


def compute_unmatched_keys(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    keys_parts = []
    for identifier in table_id:
        other = table_id[1] if identifier == table_id[0] else table_id[0]
        handle_left = handles[identifier]
        handle_right = handles[other]
        select_by = h.select_cols(by_columns, alias="left_tbl")
        condition = h.join_condition(by_columns, "left_tbl", "right_tbl")
        keys_parts.append(
            f"""
            SELECT {h.sql_literal(identifier)} AS table, {select_by}
            FROM {h.ident(handle_left.name)} AS left_tbl
            WHERE NOT EXISTS (
                SELECT 1
                FROM {h.ident(handle_right.name)} AS right_tbl
                WHERE {condition}
            )
            """
        )
    keys_sql = " UNION ALL ".join(keys_parts)
    return h.finalize_relation(conn, keys_sql, materialize)


def compute_unmatched_rows_summary(
    conn: h.VersusConn,
    unmatched_keys: duckdb.DuckDBPyRelation,
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    keys_sql = unmatched_keys.sql_query()
    table_col = h.ident("table")
    count_col = h.ident("n_unmatched")
    sql = f"""
    SELECT {table_col}, COUNT(*) AS {count_col}
    FROM ({keys_sql}) AS keys
    GROUP BY {table_col}
    ORDER BY {table_col}
    """
    return h.finalize_relation(conn, sql, materialize)
