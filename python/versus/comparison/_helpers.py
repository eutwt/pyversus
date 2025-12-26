from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
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


@dataclass
@dataclass
class VersusState:
    temp_tables: List[str]
    views: List[str]


class VersusConn:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        temp_tables: Optional[List[str]] = None,
        views: Optional[List[str]] = None,
    ) -> None:
        self._connection = connection
        self.versus = VersusState(
            temp_tables if temp_tables is not None else [],
            views if views is not None else [],
        )

    @property
    def raw_connection(self) -> duckdb.DuckDBPyConnection:
        return self._connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def normalize_table_arg(comparison: "Comparison", table: str) -> str:
    if table not in comparison.table_id:
        allowed = ", ".join(comparison.table_id)
        raise ComparisonError(f"`table` must be one of: {allowed}")
    return table


def normalize_single_column(column: str) -> str:
    if isinstance(column, str):
        return column
    raise ComparisonError("`column` must be a column name")


def resolve_column_list(
    comparison: "Comparison",
    columns: Optional[Sequence[str]],
    *,
    allow_empty: bool = True,
) -> List[str]:
    if columns is None:
        parsed = comparison.common_columns[:]
    else:
        cols = normalize_column_list(columns, "column", allow_empty=True)
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


def ensure_column_allowed(comparison: "Comparison", column: str, func: str) -> None:
    if column not in comparison.common_columns:
        raise ComparisonError(
            f"`{func}` can only reference columns in both tables: {column}"
        )


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
        raise ComparisonError("`suffix` must be a tuple of two strings or None")
    if suffix[0] == suffix[1]:
        raise ComparisonError("Entries of `suffix` must be distinct")
    if any(item == "" for item in suffix):
        raise ComparisonError("Entries of `suffix` must be non-empty")
    return (suffix[0], suffix[1])


def validate_table_id(table_id: Tuple[str, str]) -> Tuple[str, str]:
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


def normalize_column_list(
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


def resolve_materialize(materialize: str) -> Tuple[bool, bool]:
    if not isinstance(materialize, str) or materialize not in {
        "all",
        "summary",
        "none",
    }:
        raise ComparisonError("`materialize` must be one of: 'all', 'summary', 'none'")
    materialize_summary = materialize in {"all", "summary"}
    materialize_keys = materialize == "all"
    return materialize_summary, materialize_keys


def resolve_connection(
    connection: Optional[duckdb.DuckDBPyConnection],
) -> VersusConn:
    if connection is not None:
        conn_candidate = connection
    else:
        default_conn = duckdb.default_connection
        conn_candidate = default_conn() if callable(default_conn) else default_conn
    if not isinstance(conn_candidate, duckdb.DuckDBPyConnection):
        raise ComparisonError("`connection` must be a DuckDB connection.")
    return VersusConn(conn_candidate)


def register_input_view(
    conn: VersusConn,
    source: Any,
    label: str,
) -> _TableHandle:
    name = f"__versus_{label}_{uuid.uuid4().hex}"
    display = "relation"
    if isinstance(source, duckdb.DuckDBPyRelation):
        base_name = f"{name}_base"
        source.to_view(base_name, replace=True)
        source_ref = ident(base_name)
        conn.versus.views.append(base_name)
        display = getattr(source, "alias", "relation")
    elif isinstance(source, str):
        source_ref = f"({source})"
        display = source
    else:
        raise ComparisonError("Inputs must be DuckDB relations or SQL queries/views.")

    conn.execute(
        f"CREATE OR REPLACE TEMP VIEW {ident(name)} AS SELECT * FROM {source_ref}"
    )
    conn.versus.views.append(name)

    columns, types = describe_view(conn, name)
    return _TableHandle(name=name, display=display, columns=columns, types=types)


def describe_view(conn: VersusConn, name: str) -> Tuple[List[str], Dict[str, str]]:
    rel = run_sql(conn, f"DESCRIBE SELECT * FROM {ident(name)}")
    rows = rel.fetchall()
    columns = [row[0] for row in rows]
    types = {row[0]: row[1] for row in rows}
    return columns, types


def build_tables_frame(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    rows = []
    for identifier in table_id:
        handle = handles[identifier]
        result = conn.sql(f"SELECT COUNT(*) AS n FROM {ident(handle.name)}").fetchone()
        if result is None:
            raise ComparisonError("Failed to count rows for comparison table metadata")
        count = result[0]
        rows.append((identifier, count, len(handle.columns)))
    schema = [
        ("table", "VARCHAR"),
        ("nrow", "BIGINT"),
        ("ncol", "BIGINT"),
    ]
    return build_rows_relation(conn, rows, schema, materialize)


def build_by_frame(
    conn: VersusConn,
    by_columns: List[str],
    handles: Mapping[str, _TableHandle],
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
    return build_rows_relation(conn, rows, schema, materialize)


def build_unmatched_cols(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
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
    return build_rows_relation(conn, rows, schema, materialize)


def build_intersection_frame(
    value_columns: List[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    diff_keys: Mapping[str, duckdb.DuckDBPyRelation],
    conn: VersusConn,
    materialize: bool,
) -> Tuple[duckdb.DuckDBPyRelation, Dict[str, int]]:
    rows = []
    diff_lookup: Dict[str, int] = {}
    first, second = table_id
    for column in value_columns:
        relation = diff_keys[column]
        count = table_count(conn, relation)
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
    relation = build_rows_relation(conn, rows, schema, materialize)
    return relation, diff_lookup


def compute_diff_keys(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    value_columns: List[str],
    allow_both_na: bool,
    materialize: bool,
) -> Dict[str, duckdb.DuckDBPyRelation]:
    diff_keys: Dict[str, duckdb.DuckDBPyRelation] = {}
    join_sql = join_clause(handles, table_id, by_columns)
    select_by = select_cols(by_columns, alias="a")
    for column in value_columns:
        predicate = diff_predicate(column, allow_both_na, "a", "b")
        sql = f"SELECT {select_by} {join_sql} WHERE {predicate}"
        relation = finalize_relation(conn, sql, materialize)
        diff_keys[column] = relation
    return diff_keys


def compute_unmatched_keys(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    keys_parts = []
    for identifier in table_id:
        other = table_id[1] if identifier == table_id[0] else table_id[0]
        handle_left = handles[identifier]
        handle_right = handles[other]
        select_by = select_cols(by_columns, alias="left_tbl")
        condition = join_condition(by_columns, "left_tbl", "right_tbl")
        keys_parts.append(
            f"""
            SELECT {sql_literal(identifier)} AS table, {select_by}
            FROM {ident(handle_left.name)} AS left_tbl
            WHERE NOT EXISTS (
                SELECT 1
                FROM {ident(handle_right.name)} AS right_tbl
                WHERE {condition}
            )
            """
        )
    keys_sql = " UNION ALL ".join(keys_parts)
    return finalize_relation(conn, keys_sql, materialize)


def compute_unmatched_rows_summary(
    conn: VersusConn,
    unmatched_keys: duckdb.DuckDBPyRelation,
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    keys_sql = unmatched_keys.sql_query()
    table_col = ident("table")
    count_col = ident("n_unmatched")
    sql = f"""
    SELECT {table_col}, COUNT(*) AS {count_col}
    FROM ({keys_sql}) AS keys
    GROUP BY {table_col}
    ORDER BY {table_col}
    """
    return finalize_relation(conn, sql, materialize)


def collect_diff_keys(comparison: "Comparison", columns: Sequence[str]) -> str:
    selects = []
    for column in columns:
        key_sql = comparison.diff_keys[column].sql_query()
        selects.append(key_sql)
    return " UNION DISTINCT ".join(selects)


def fetch_rows_by_keys(
    comparison: "Comparison",
    table: str,
    key_sql: str,
    columns: Sequence[str],
) -> duckdb.DuckDBPyRelation:
    select_cols_sql = select_cols(columns, alias="base")
    join_condition_sql = join_condition(comparison.by_columns, "keys", "base")
    sql = f"""
    SELECT {select_cols_sql}
    FROM ({key_sql}) AS keys
    JOIN {ident(comparison._handles[table].name)} AS base
      ON {join_condition_sql}
    """
    return run_sql(comparison.connection, sql)


def run_sql(
    conn: Union[VersusConn, duckdb.DuckDBPyConnection],
    sql: str,
) -> duckdb.DuckDBPyRelation:
    return conn.sql(sql)


def relation_is_empty(relation: duckdb.DuckDBPyRelation) -> bool:
    return relation.limit(1).fetchone() is None


def validate_columns_exist(
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


def validate_class_compatibility(
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


def ensure_unique_by(
    conn: VersusConn,
    handle: _TableHandle,
    by_columns: List[str],
    identifier: str,
) -> None:
    cols = select_cols(by_columns, alias="t")
    sql = f"""
    SELECT {cols}, COUNT(*) AS n
    FROM {ident(handle.name)} AS t
    GROUP BY {cols}
    HAVING COUNT(*) > 1
    LIMIT 1
    """
    rel = run_sql(conn, sql)
    rows = rel.fetchall()
    if rows:
        first = rows[0]
        values = ", ".join(f"{col}={first[i]!r}" for i, col in enumerate(by_columns))
        raise ComparisonError(
            f"`{identifier}` has more than one row for by values ({values})"
        )


def diff_predicate(
    column: str, allow_both_na: bool, left_alias: str, right_alias: str
) -> str:
    left = col(left_alias, column)
    right = col(right_alias, column)
    if allow_both_na:
        return f"{left} IS DISTINCT FROM {right}"
    return f"(({left} IS NULL AND {right} IS NULL) OR {left} IS DISTINCT FROM {right})"


def join_clause(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
) -> str:
    join_condition_sql = join_condition(by_columns, "a", "b")
    return (
        f"FROM {ident(handles[table_id[0]].name)} AS a "
        f"INNER JOIN {ident(handles[table_id[1]].name)} AS b ON {join_condition_sql}"
    )


def join_condition(by_columns: List[str], left_alias: str, right_alias: str) -> str:
    comparisons = [
        f"{col(left_alias, column)} IS NOT DISTINCT FROM {col(right_alias, column)}"
        for column in by_columns
    ]
    return " AND ".join(comparisons) if comparisons else "TRUE"


def col(alias: str, column: str) -> str:
    return f"{alias}.{ident(column)}"


def select_cols(columns: Sequence[str], alias: Optional[str] = None) -> str:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    if alias is None:
        return ", ".join(ident(column) for column in columns)
    return ", ".join(col(alias, column) for column in columns)


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def rows_relation_sql(
    rows: Sequence[Sequence[Any]], schema: Sequence[Tuple[str, str]]
) -> str:
    if not rows:
        select_list = ", ".join(
            f"CAST(NULL AS {dtype}) AS {ident(name)}" for name, dtype in schema
        )
        return f"SELECT {select_list} LIMIT 0"
    value_rows = []
    for row in rows:
        value_rows.append("(" + ", ".join(sql_literal(value) for value in row) + ")")
    alias_cols = ", ".join(f"col{i}" for i in range(len(schema)))
    select_list = ", ".join(
        f"CAST(col{i} AS {dtype}) AS {ident(name)}"
        for i, (name, dtype) in enumerate(schema)
    )
    return (
        f"SELECT {select_list} FROM (VALUES {', '.join(value_rows)}) AS v({alias_cols})"
    )


def finalize_relation(
    conn: VersusConn,
    sql: str,
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    if not materialize:
        return conn.sql(sql)
    table = materialize_temp_table(conn, sql)
    conn.versus.temp_tables.append(table)
    return conn.sql(f"SELECT * FROM {ident(table)}")


def build_rows_relation(
    conn: VersusConn,
    rows: Sequence[Sequence[Any]],
    schema: Sequence[Tuple[str, str]],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    sql = rows_relation_sql(rows, schema)
    return finalize_relation(conn, sql, materialize)


def table_count(conn: VersusConn, relation: duckdb.DuckDBPyRelation) -> int:
    sql = relation.sql_query()
    row = conn.sql(f"SELECT COUNT(*) FROM ({sql}) AS t").fetchone()
    if row is None:
        raise ComparisonError("Failed to count rows for diff key relation")
    return row[0]


def select_zero_from_table(
    comparison: "Comparison",
    table: str,
    columns: Sequence[str],
) -> duckdb.DuckDBPyRelation:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    select_cols_sql = select_cols(columns, alias="base")
    sql = f"""
    SELECT {select_cols_sql}
    FROM {ident(comparison._handles[table].name)} AS base
    LIMIT 0
    """
    return run_sql(comparison.connection, sql)


def ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def materialize_temp_table(conn: VersusConn, sql: str) -> str:
    name = f"__versus_table_{uuid.uuid4().hex}"
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {ident(name)} AS {sql}")
    return name
