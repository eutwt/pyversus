from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Tuple

import duckdb

from . import _helpers as h


def build_tables_frame(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    def row_for(identifier: str) -> Tuple[str, int, int]:
        handle = handles[identifier]
        return identifier, h.table_count(handle), len(handle.columns)

    rows = [row_for(identifier) for identifier in table_id]
    schema = [
        ("table_name", "VARCHAR"),
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
    first, second = table_id
    rows = [
        (
            column,
            handles[first].types[column],
            handles[second].types[column],
        )
        for column in by_columns
    ]
    schema = [
        ("column", "VARCHAR"),
        (f"type_{first}", "VARCHAR"),
        (f"type_{second}", "VARCHAR"),
    ]
    return h.build_rows_relation(conn, rows, schema, materialize)


def build_unmatched_cols(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    first, second = table_id
    cols_first = set(handles[first].columns)
    cols_second = set(handles[second].columns)
    rows = [
        (first, column, handles[first].types[column])
        for column in sorted(cols_first - cols_second)
    ] + [
        (second, column, handles[second].types[column])
        for column in sorted(cols_second - cols_first)
    ]
    schema = [
        ("table_name", "VARCHAR"),
        ("column", "VARCHAR"),
        ("type", "VARCHAR"),
    ]
    return h.build_rows_relation(conn, rows, schema, materialize)


def build_intersection_frame(
    value_columns: List[str],
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    allow_both_na: bool,
    diff_keys: Optional[Mapping[str, duckdb.DuckDBPyRelation]],
    conn: h.VersusConn,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[Dict[str, int]]]:
    if diff_keys is None:
        return _build_intersection_frame_inline(
            value_columns,
            handles,
            table_id,
            by_columns,
            allow_both_na,
            conn,
            materialize,
        )
    return _build_intersection_frame_with_keys(
        value_columns, handles, table_id, diff_keys, conn, materialize
    )


def _build_intersection_frame_with_keys(
    value_columns: List[str],
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    diff_keys: Mapping[str, duckdb.DuckDBPyRelation],
    conn: h.VersusConn,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[Dict[str, int]]]:
    first, second = table_id
    schema = [
        ("column", "VARCHAR"),
        ("n_diffs", "BIGINT"),
        (f"type_{first}", "VARCHAR"),
        (f"type_{second}", "VARCHAR"),
    ]
    if not value_columns:
        relation = h.build_rows_relation(conn, [], schema, materialize)
        return relation, {} if materialize else None

    def select_for(column: str) -> str:
        relation_sql = diff_keys[column].sql_query()
        return f"""
        SELECT
          {h.sql_literal(column)} AS {h.ident("column")},
          COUNT(*) AS {h.ident("n_diffs")},
          {h.sql_literal(handles[first].types[column])} AS {h.ident(f"type_{first}")},
          {h.sql_literal(handles[second].types[column])} AS {h.ident(f"type_{second}")}
        FROM
          ({relation_sql}) AS diff_keys
        """

    sql = " UNION ALL ".join(select_for(column) for column in value_columns)
    relation = h.finalize_relation(conn, sql, materialize)
    if not materialize:
        return relation, None
    return relation, h.diff_lookup_from_intersection(relation)


def _build_intersection_frame_inline(
    value_columns: List[str],
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    allow_both_na: bool,
    conn: h.VersusConn,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[Dict[str, int]]]:
    first, second = table_id
    schema = [
        ("column", "VARCHAR"),
        ("n_diffs", "BIGINT"),
        (f"type_{first}", "VARCHAR"),
        (f"type_{second}", "VARCHAR"),
    ]
    if not value_columns:
        relation = h.build_rows_relation(conn, [], schema, materialize)
        return relation, {} if materialize else None
    join_sql = h.join_clause(handles, table_id, by_columns)

    def diff_alias(column: str) -> str:
        return f"n_diffs_{column}"

    count_columns = ",\n      ".join(
        f"COUNT(*) FILTER (WHERE {h.diff_predicate(column, allow_both_na, 'a', 'b')}) "
        f"AS {h.ident(diff_alias(column))}"
        for column in value_columns
    )
    struct_list = ",\n        ".join(
        (
            "struct_pack("
            f"{h.ident('column')} := {h.sql_literal(column)}, "
            f"{h.ident('n_diffs')} := counts.{h.ident(diff_alias(column))}, "
            f"{h.ident(f'type_{first}')} := {h.sql_literal(handles[first].types[column])}, "
            f"{h.ident(f'type_{second}')} := {h.sql_literal(handles[second].types[column])}"
            ")"
        )
        for column in value_columns
    )
    sql = f"""
    WITH counts AS (
      SELECT
        {count_columns}
      FROM
        {join_sql}
    )
    SELECT
      item.{h.ident("column")} AS {h.ident("column")},
      item.{h.ident("n_diffs")} AS {h.ident("n_diffs")},
      item.{h.ident(f"type_{first}")} AS {h.ident(f"type_{first}")},
      item.{h.ident(f"type_{second}")} AS {h.ident(f"type_{second}")}
    FROM
      counts,
      UNNEST(
        [
          {struct_list}
        ]
      ) AS unnest(item)
    """
    relation = h.finalize_relation(conn, sql, materialize)
    if not materialize:
        return relation, None
    return relation, h.diff_lookup_from_intersection(relation)


def compute_diff_keys(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    value_columns: List[str],
    allow_both_na: bool,
) -> Dict[str, duckdb.DuckDBPyRelation]:
    diff_keys: Dict[str, duckdb.DuckDBPyRelation] = {}
    join_sql = h.join_clause(handles, table_id, by_columns)
    select_by = h.select_cols(by_columns, alias="a")
    for column in value_columns:
        predicate = h.diff_predicate(column, allow_both_na, "a", "b")
        sql = f"""
        SELECT
          {select_by}
        FROM
          {join_sql}
        WHERE
          {predicate}
        """
        relation = h.finalize_relation(conn, sql, materialize=True)
        diff_keys[column] = relation
    return diff_keys


def compute_unmatched_keys(
    conn: h.VersusConn,
    handles: Mapping[str, h._TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    def key_part(identifier: str) -> str:
        other = table_id[1] if identifier == table_id[0] else table_id[0]
        handle_left = handles[identifier]
        handle_right = handles[other]
        select_by = h.select_cols(by_columns, alias="left_tbl")
        condition = h.join_condition(by_columns, "left_tbl", "right_tbl")
        return f"""
        SELECT
          {h.sql_literal(identifier)} AS table_name,
          {select_by}
        FROM
          {h.ident(handle_left.name)} AS left_tbl
        WHERE
          NOT EXISTS (
            SELECT
              1
            FROM
              {h.ident(handle_right.name)} AS right_tbl
            WHERE
              {condition}
          )
        """

    keys_parts = [key_part(identifier) for identifier in table_id]
    keys_sql = " UNION ALL ".join(keys_parts)
    return h.finalize_relation(conn, keys_sql, materialize)


def compute_unmatched_rows_summary(
    conn: h.VersusConn,
    unmatched_keys: duckdb.DuckDBPyRelation,
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    keys_sql = unmatched_keys.sql_query()
    table_col = h.ident("table_name")
    count_col = h.ident("n_unmatched")
    base_sql = h.rows_relation_sql(
        [(table_id[0],), (table_id[1],)], [("table_name", "VARCHAR")]
    )
    counts_sql = f"""
    SELECT
      {table_col},
      COUNT(*) AS {count_col}
    FROM
      ({keys_sql}) AS keys
    GROUP BY
      {table_col}
    """
    order_case = (
        f"CASE base.{table_col} "
        f"WHEN {h.sql_literal(table_id[0])} THEN 0 "
        f"WHEN {h.sql_literal(table_id[1])} THEN 1 "
        "ELSE 2 END"
    )
    sql = f"""
    SELECT
      base.{table_col} AS {table_col},
      COALESCE(counts.{count_col}, CAST(0 AS BIGINT)) AS {count_col}
    FROM
      ({base_sql}) AS base
      LEFT JOIN ({counts_sql}) AS counts
        ON base.{table_col} = counts.{table_col}
    ORDER BY
      {order_case}
    """
    return h.finalize_relation(conn, sql, materialize)
