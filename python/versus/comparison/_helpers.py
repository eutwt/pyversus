from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)

import duckdb

from ._exceptions import ComparisonError

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


@dataclass
class _TableHandle:
    name: str
    display: str
    columns: List[str]
    types: Dict[str, str]
    cleanup: Callable[[], None]


def _normalize_table_arg(comparison: "Comparison", table: str) -> str:
    if table not in comparison.table_id:
        allowed = ", ".join(comparison.table_id)
        raise ComparisonError(f"`table` must be one of: {allowed}")
    return table


def _normalize_single_column(column: str) -> str:
    if isinstance(column, str):
        return column
    raise ComparisonError("`column` must be a column name")


def _resolve_column_list(
    comparison: "Comparison",
    columns: Optional[Sequence[str]],
    *,
    allow_empty: bool = True,
) -> List[str]:
    if columns is None:
        parsed = comparison.common_columns[:]
    else:
        cols = _normalize_column_list(columns, "column", allow_empty=True)
        if not cols:
            raise ComparisonError("`columns` must select at least one column")
        missing = [col for col in cols if col not in comparison.common_columns]
        if missing:
            raise ComparisonError(
                f"Columns not part of the comparison: {', '.join(missing)}"
            )
        parsed = cols
    if not parsed and not allow_empty:
        raise ComparisonError("`columns` must select at least one column")
    return parsed


def _ensure_column_allowed(comparison: "Comparison", column: str, func: str) -> None:
    if column not in comparison.common_columns:
        raise ComparisonError(
            f"`{func}` can only reference columns in both tables: {column}"
        )


def _resolve_suffix(
    suffix: Optional[Tuple[str, str]], table_id: Tuple[str, str]
) -> Tuple[str, str]:
    if suffix is None:
        return (f"_{table_id[0]}", f"_{table_id[1]}")
    if (
        not isinstance(suffix, (tuple, list))
        or len(suffix) != 2
        or not all(isinstance(item, str) for item in suffix)
    ):
        raise ComparisonError("`suffix` must be a tuple of two strings or None")
    if suffix[0] == suffix[1]:
        raise ComparisonError("Entries of `suffix` must be distinct")
    if any(item == "" for item in suffix):
        raise ComparisonError("Entries of `suffix` must be non-empty")
    return (suffix[0], suffix[1])


def _validate_table_id(table_id: Tuple[str, str]) -> Tuple[str, str]:
    if (
        not isinstance(table_id, (tuple, list))
        or len(table_id) != 2
        or not all(isinstance(val, str) for val in table_id)
    ):
        raise ComparisonError("`table_id` must be a tuple of two strings")
    first, second = table_id[0], table_id[1]
    if not first.strip() or not second.strip():
        raise ComparisonError("Entries of `table_id` must be non-empty strings")
    if first == second:
        raise ComparisonError("Entries of `table_id` must be distinct")
    return (first, second)


def _normalize_column_list(
    columns: Sequence[str],
    arg_name: str,
    *,
    allow_empty: bool,
) -> List[str]:
    if isinstance(columns, str):
        parsed = [columns]
    else:
        try:
            parsed = list(columns)
        except TypeError as exc:
            raise ComparisonError(
                f"`{arg_name}` must be a sequence of column names"
            ) from exc
    if not parsed and not allow_empty:
        raise ComparisonError(f"`{arg_name}` must contain at least one column")
    if not all(isinstance(item, str) for item in parsed):
        raise ComparisonError(f"`{arg_name}` must only contain strings")
    return parsed


def _register_input_view(
    conn: duckdb.DuckDBPyConnection,
    source: Any,
    label: str,
) -> _TableHandle:
    name = f"__versus_{label}_{uuid.uuid4().hex}"
    cleanup_statements: List[str] = []
    display = "relation"
    if isinstance(source, duckdb.DuckDBPyRelation):
        base_name = f"{name}_base"
        source.to_view(base_name, replace=True)
        source_ref = _ident(base_name)
        cleanup_statements.append(f"DROP VIEW IF EXISTS {source_ref}")
        display = getattr(source, "alias", "relation")
    elif isinstance(source, str):
        source_ref = f"({source})"
        display = source
    else:
        raise ComparisonError("Inputs must be DuckDB relations or SQL queries/views.")

    conn.execute(
        f"CREATE OR REPLACE TEMP VIEW {_ident(name)} AS SELECT * FROM {source_ref}"
    )
    cleanup_statements.insert(0, f"DROP VIEW IF EXISTS {_ident(name)}")

    def _cleanup() -> None:
        for stmt in cleanup_statements:
            try:
                conn.execute(stmt)
            except duckdb.Error:
                pass

    columns, types = _describe_view(conn, name)
    return _TableHandle(
        name=name, display=display, columns=columns, types=types, cleanup=_cleanup
    )


def _describe_view(
    conn: duckdb.DuckDBPyConnection, name: str
) -> Tuple[List[str], Dict[str, str]]:
    rel = _run_sql(conn, f"DESCRIBE SELECT * FROM {_ident(name)}")
    rows = rel.fetchall()
    columns = [row[0] for row in rows]
    types = {row[0]: row[1] for row in rows}
    return columns, types


def _build_tables_frame(
    conn: duckdb.DuckDBPyConnection,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[str]]:
    rows = []
    for identifier in table_id:
        handle = handles[identifier]
        result = conn.sql(f"SELECT COUNT(*) AS n FROM {_ident(handle.name)}").fetchone()
        if result is None:
            raise ComparisonError("Failed to count rows for comparison table metadata")
        count = result[0]
        rows.append((identifier, count, len(handle.columns)))
    schema = [
        ("table", "VARCHAR"),
        ("nrow", "BIGINT"),
        ("ncol", "BIGINT"),
    ]
    return _build_rows_relation(conn, rows, schema, materialize)


def _build_by_frame(
    conn: duckdb.DuckDBPyConnection,
    by_columns: List[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[str]]:
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
    return _build_rows_relation(conn, rows, schema, materialize)


def _build_unmatched_cols(
    conn: duckdb.DuckDBPyConnection,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[str]]:
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
    return _build_rows_relation(conn, rows, schema, materialize)


def _build_intersection_frame(
    value_columns: List[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    diff_key_tables: Mapping[str, str],
    conn: duckdb.DuckDBPyConnection,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Dict[str, int], Optional[str]]:
    rows = []
    diff_lookup: Dict[str, int] = {}
    first, second = table_id
    for column in value_columns:
        table = diff_key_tables[column]
        count = _table_count(conn, table)
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
    relation, table_name = _build_rows_relation(conn, rows, schema, materialize)
    return relation, diff_lookup, table_name


def _compute_diff_key_tables(
    conn: duckdb.DuckDBPyConnection,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    value_columns: List[str],
    allow_both_na: bool,
) -> Dict[str, str]:
    diff_tables: Dict[str, str] = {}
    join_clause = _join_clause(handles, table_id, by_columns)
    select_by = _select_cols(by_columns, alias="a")
    for column in value_columns:
        predicate = _diff_predicate(column, allow_both_na, "a", "b")
        sql = f"SELECT {select_by} {join_clause} WHERE {predicate}"
        table_name = _materialize_temp_table(conn, sql)
        diff_tables[column] = table_name
    return diff_tables


def _compute_unmatched_rows(
    conn: duckdb.DuckDBPyConnection,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Dict[str, str], Optional[str]]:
    tables: Dict[str, str] = {}
    summary_parts = []
    for identifier in table_id:
        other = table_id[1] if identifier == table_id[0] else table_id[0]
        handle_left = handles[identifier]
        handle_right = handles[other]
        select_by = _select_cols(by_columns, alias="left_tbl")
        condition = _join_condition(by_columns, "left_tbl", "right_tbl")
        sql = f"""
        SELECT {select_by}
        FROM {_ident(handle_left.name)} AS left_tbl
        WHERE NOT EXISTS (
            SELECT 1
            FROM {_ident(handle_right.name)} AS right_tbl
            WHERE {condition}
        )
        """
        table_name = _materialize_temp_table(conn, sql)
        tables[identifier] = table_name
        summary_parts.append(
            f"SELECT '{identifier}' AS table, * FROM {_ident(table_name)}"
        )
    summary_sql = " UNION ALL ".join(summary_parts)
    summary_rel, summary_table = _finalize_relation(conn, summary_sql, materialize)
    return summary_rel, tables, summary_table


def _collect_diff_keys(comparison: "Comparison", columns: Sequence[str]) -> str:
    selects = []
    for column in columns:
        selects.append(f"SELECT * FROM {_ident(comparison.diff_key_tables[column])}")
    if len(selects) == 1:
        return selects[0]
    return " UNION DISTINCT ".join(selects)


def _fetch_rows_by_keys(
    comparison: "Comparison",
    table: str,
    key_sql: str,
    columns: Sequence[str],
) -> duckdb.DuckDBPyRelation:
    select_cols = _select_cols(columns, alias="base")
    join_condition = " AND ".join(
        f"{_col('keys', col)} IS NOT DISTINCT FROM {_col('base', col)}"
        for col in comparison.by_columns
    )
    sql = f"""
    SELECT {select_cols}
    FROM ({key_sql}) AS keys
    JOIN {_ident(comparison._handles[table].name)} AS base
      ON {join_condition}
    """
    return _run_sql(comparison.connection, sql)


def _run_sql(conn: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyRelation:
    return conn.sql(sql)


def _relation_is_empty(relation: duckdb.DuckDBPyRelation) -> bool:
    return relation.limit(1).fetchone() is None


def _validate_columns_exist(
    by_columns: Iterable[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    missing_a = [col for col in by_columns if col not in handles[table_id[0]].columns]
    missing_b = [col for col in by_columns if col not in handles[table_id[1]].columns]
    if missing_a:
        raise ComparisonError(
            f"`by` columns not found in `{table_id[0]}`: {', '.join(missing_a)}"
        )
    if missing_b:
        raise ComparisonError(
            f"`by` columns not found in `{table_id[1]}`: {', '.join(missing_b)}"
        )


def _validate_class_compatibility(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    shared = set(handles[table_id[0]].columns) & set(handles[table_id[1]].columns)
    for column in shared:
        type_a = handles[table_id[0]].types.get(column)
        type_b = handles[table_id[1]].types.get(column)
        if type_a != type_b:
            raise ComparisonError(
                f"`coerce=False` requires compatible classes. Column `{column}` has types `{type_a}` vs `{type_b}`."
            )


def _ensure_unique_by(
    conn: duckdb.DuckDBPyConnection,
    handle: _TableHandle,
    by_columns: List[str],
    identifier: str,
) -> None:
    cols = _select_cols(by_columns, alias="t")
    sql = f"""
    SELECT {cols}, COUNT(*) AS n
    FROM {_ident(handle.name)} AS t
    GROUP BY {cols}
    HAVING COUNT(*) > 1
    LIMIT 1
    """
    rel = _run_sql(conn, sql)
    rows = rel.fetchall()
    if rows:
        first = rows[0]
        values = ", ".join(f"{col}={first[i]!r}" for i, col in enumerate(by_columns))
        raise ComparisonError(
            f"`{identifier}` has more than one row for by values ({values})"
        )


def _diff_predicate(
    column: str, allow_both_na: bool, left_alias: str, right_alias: str
) -> str:
    left = _col(left_alias, column)
    right = _col(right_alias, column)
    if allow_both_na:
        return f"{left} IS DISTINCT FROM {right}"
    return f"(({left} IS NULL AND {right} IS NULL) OR {left} IS DISTINCT FROM {right})"


def _join_clause(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
) -> str:
    join_condition = _join_condition(by_columns, "a", "b")
    return (
        f"FROM {_ident(handles[table_id[0]].name)} AS a "
        f"INNER JOIN {_ident(handles[table_id[1]].name)} AS b ON {join_condition}"
    )


def _join_condition(by_columns: List[str], left_alias: str, right_alias: str) -> str:
    comparisons = [
        f"{_col(left_alias, column)} IS NOT DISTINCT FROM {_col(right_alias, column)}"
        for column in by_columns
    ]
    return " AND ".join(comparisons) if comparisons else "TRUE"


def _col(alias: str, column: str) -> str:
    return f"{alias}.{_ident(column)}"


def _select_cols(columns: Sequence[str], alias: Optional[str] = None) -> str:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    if alias is None:
        return ", ".join(_ident(column) for column in columns)
    return ", ".join(_col(alias, column) for column in columns)


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _rows_relation_sql(
    rows: Sequence[Sequence[Any]], schema: Sequence[Tuple[str, str]]
) -> str:
    if rows:
        value_rows = []
        for row in rows:
            value_rows.append(
                "(" + ", ".join(_sql_literal(value) for value in row) + ")"
            )
        alias_cols = ", ".join(f"col{i}" for i in range(len(schema)))
        select_list = ", ".join(
            f"CAST(col{i} AS {dtype}) AS {_ident(name)}"
            for i, (name, dtype) in enumerate(schema)
        )
        return f"SELECT {select_list} FROM (VALUES {', '.join(value_rows)}) AS v({alias_cols})"
    select_list = ", ".join(
        f"CAST(NULL AS {dtype}) AS {_ident(name)}" for name, dtype in schema
    )
    return f"SELECT {select_list} WHERE 1=0"


def _finalize_relation(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[str]]:
    if materialize:
        table = _materialize_temp_table(conn, sql)
        return conn.sql(f"SELECT * FROM {_ident(table)}"), table
    return conn.sql(sql), None


def _build_rows_relation(
    conn: duckdb.DuckDBPyConnection,
    rows: Sequence[Sequence[Any]],
    schema: Sequence[Tuple[str, str]],
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Optional[str]]:
    sql = _rows_relation_sql(rows, schema)
    return _finalize_relation(conn, sql, materialize)


def _table_count(conn: duckdb.DuckDBPyConnection, table_name: Optional[str]) -> int:
    if table_name is None:
        return 0
    row = conn.sql(f"SELECT COUNT(*) FROM {_ident(table_name)}").fetchone()
    if row is None:
        raise ComparisonError("Failed to count rows for diff key table")
    return row[0]


def _select_zero_from_table(
    comparison: "Comparison",
    table: str,
    columns: Sequence[str],
) -> duckdb.DuckDBPyRelation:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    select_cols = _select_cols(columns, alias="base")
    sql = f"""
    SELECT {select_cols}
    FROM {_ident(comparison._handles[table].name)} AS base
    LIMIT 0
    """
    return _run_sql(comparison.connection, sql)


def _ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _materialize_temp_table(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    name = f"__versus_table_{uuid.uuid4().hex}"
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {_ident(name)} AS {sql}")
    return name
