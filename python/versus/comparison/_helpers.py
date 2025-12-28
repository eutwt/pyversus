from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import duckdb

from ._exceptions import ComparisonError

if TYPE_CHECKING:  # pragma: no cover
    from ._core import Comparison


# --------------- Data structures
@dataclass
class _TableHandle:
    name: str
    display: str
    relation: duckdb.DuckDBPyRelation
    columns: List[str]
    types: Dict[str, str]
    source_sql: str
    source_is_identifier: bool
    row_count: int

    def __getattr__(self, name: str) -> Any:
        return getattr(self.relation, name)


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
        self.raw_connection = connection
        self.versus = VersusState(
            temp_tables if temp_tables is not None else [],
            views if views is not None else [],
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw_connection, name)


class SummaryRelation:
    def __init__(
        self,
        conn: VersusConn,
        relation: duckdb.DuckDBPyRelation,
        *,
        materialized: bool,
        on_materialize: Optional[Callable[[duckdb.DuckDBPyRelation], None]] = None,
    ) -> None:
        self._conn = conn
        self.relation = relation
        self.materialized = materialized
        self._on_materialize = on_materialize
        if self.materialized and self._on_materialize is not None:
            self._on_materialize(self.relation)

    def materialize(self) -> None:
        if self.materialized:
            return
        self.relation = finalize_relation(
            self._conn, self.relation.sql_query(), materialize=True
        )
        self.materialized = True
        if self._on_materialize is not None:
            self._on_materialize(self.relation)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.relation, name)

    def __repr__(self) -> str:
        self.materialize()
        return repr(self.relation)

    def __str__(self) -> str:
        self.materialize()
        return str(self.relation)

    def __iter__(self) -> Any:
        return iter(cast(Any, self.relation))


# --------------- Core-only helpers
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


def validate_type_compatibility(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    shared = set(handles[table_id[0]].columns) & set(handles[table_id[1]].columns)
    for column in shared:
        type_a = handles[table_id[0]].types.get(column)
        type_b = handles[table_id[1]].types.get(column)
        if type_a != type_b:
            raise ComparisonError(
                f"`coerce=False` requires compatible types. Column `{column}` has types `{type_a}` vs `{type_b}`."
            )


def validate_columns(columns: Sequence[str], label: str) -> None:
    if not all(isinstance(column, str) for column in columns):
        raise ComparisonError(f"`{label}` must have string column names")
    counts = Counter(columns)
    duplicates = [name for name, count in counts.items() if count > 1]
    if duplicates:
        dupes = ", ".join(duplicates)
        raise ComparisonError(f"`{label}` has duplicate column names: {dupes}")


def validate_tables(
    conn: VersusConn,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    *,
    coerce: bool,
) -> None:
    validate_columns_exist(by_columns, handles, table_id)
    for identifier in table_id:
        validate_columns(handles[identifier].columns, identifier)
    if not coerce:
        validate_type_compatibility(handles, table_id)
    for identifier in table_id:
        assert_unique_by(conn, handles[identifier], by_columns, identifier)


def assert_unique_by(
    conn: VersusConn,
    handle: _TableHandle,
    by_columns: List[str],
    identifier: str,
) -> None:
    cols = select_cols(by_columns, alias="t")
    sql = f"""
    SELECT
      {cols},
      COUNT(*) AS n
    FROM
      {table_ref(handle)} AS t
    GROUP BY
      {cols}
    HAVING
      COUNT(*) > 1
    LIMIT
      1
    """
    rel = run_sql(conn, sql)
    rows = rel.fetchall()
    if rows:
        first = rows[0]
        values = ", ".join(f"{col}={first[i]!r}" for i, col in enumerate(by_columns))
        raise ComparisonError(
            f"`{identifier}` has more than one row for by values ({values})"
        )


# --------------- Input validation and normalization
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


def assert_column_allowed(comparison: "Comparison", column: str, func: str) -> None:
    if column not in comparison.common_columns:
        raise ComparisonError(
            f"`{func}` can only reference columns in both tables: {column}"
        )


# --------------- Input registration and metadata
def build_table_handle(
    conn: VersusConn,
    source: Union[
        duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"
    ],
    label: str,
    *,
    connection_supplied: bool,
) -> _TableHandle:
    name = f"__versus_{label}_{uuid.uuid4().hex}"
    display = "relation"
    if isinstance(source, duckdb.DuckDBPyRelation):
        validate_columns(source.columns, label)
        source_sql = source.sql_query()
        display = getattr(source, "alias", "relation")
        assert_relation_connection(conn, source, label, connection_supplied)
        try:
            columns, types = describe_source(conn, source_sql, is_identifier=False)
        except duckdb.Error as exc:
            raise_relation_connection_error(label, connection_supplied, exc)
        row_count = resolve_row_count(conn, source, source_sql, is_identifier=False)
        relation = conn.sql(source_sql)
        return _TableHandle(
            name=name,
            display=display,
            relation=relation,
            columns=columns,
            types=types,
            source_sql=source_sql,
            source_is_identifier=False,
            row_count=row_count,
        )
    if isinstance(source, str):
        raise ComparisonError(
            "String inputs are not supported. Pass a DuckDB relation or pandas/polars "
            "DataFrame."
        )
    source_columns = getattr(source, "columns", None)
    if source_columns is not None:
        validate_columns(list(source_columns), label)
    try:
        conn.register(name, source)
    except Exception as exc:
        raise ComparisonError(
            "Inputs must be DuckDB relations or pandas/polars DataFrames."
        ) from exc
    conn.versus.views.append(name)
    source_sql = name
    columns, types = describe_source(conn, source_sql, is_identifier=True)
    row_count = resolve_row_count(conn, source, source_sql, is_identifier=True)
    relation = conn.table(name)
    return _TableHandle(
        name=name,
        display=type(source).__name__,
        relation=relation,
        columns=columns,
        types=types,
        source_sql=source_sql,
        source_is_identifier=True,
        row_count=row_count,
    )


def describe_source(
    conn: VersusConn,
    source_sql: str,
    *,
    is_identifier: bool,
) -> Tuple[List[str], Dict[str, str]]:
    source_ref = source_ref_for_sql(source_sql, is_identifier)
    rel = run_sql(conn, f"DESCRIBE SELECT * FROM {source_ref}")
    rows = rel.fetchall()
    columns = [row[0] for row in rows]
    types = {row[0]: row[1] for row in rows}
    return columns, types


def source_ref_for_sql(source_sql: str, is_identifier: bool) -> str:
    return ident(source_sql) if is_identifier else f"({source_sql})"


def resolve_row_count(
    conn: VersusConn,
    source: Union[
        duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"
    ],
    source_sql: str,
    *,
    is_identifier: bool,
) -> int:
    frame_row_count = row_count_from_frame(source)
    if frame_row_count is not None:
        return frame_row_count
    source_ref = source_ref_for_sql(source_sql, is_identifier)
    row = run_sql(conn, f"SELECT COUNT(*) FROM {source_ref}").fetchone()
    assert row is not None and isinstance(row[0], int)
    return row[0]


def row_count_from_frame(
    source: Union[
        duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"
    ],
) -> Optional[int]:
    module = type(source).__module__
    if module.startswith("pandas"):
        return int(source.shape[0])
    if module.startswith("polars"):
        return int(source.height)
    return None


def raise_relation_connection_error(
    label: str,
    connection_supplied: bool,
    exc: Exception,
) -> None:
    arg_name = f"table_{label}"
    if connection_supplied:
        hint = (
            f"`{arg_name}` appears to be bound to a different DuckDB "
            "connection than the one passed to `compare()`. Pass the same "
            "connection that created the relations via `connection=...`."
        )
    else:
        hint = (
            f"`{arg_name}` appears to be bound to a non-default DuckDB "
            "connection. Pass that connection to `compare()` via "
            "`connection=...`."
        )
    raise ComparisonError(hint) from exc


def assert_relation_connection(
    conn: VersusConn,
    relation: duckdb.DuckDBPyRelation,
    label: str,
    connection_supplied: bool,
) -> None:
    probe_name = f"__versus_probe_{uuid.uuid4().hex}"
    try:
        conn.register(probe_name, relation)
    except Exception as exc:
        raise_relation_connection_error(label, connection_supplied, exc)
    else:
        conn.unregister(probe_name)


# --------------- SQL builder helpers
def ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def col(alias: str, column: str) -> str:
    return f"{alias}.{ident(column)}"


def table_ref(handle: _TableHandle) -> str:
    if handle.source_is_identifier:
        return ident(handle.source_sql)
    return f"({handle.source_sql})"


def select_cols(columns: Sequence[str], alias: Optional[str] = None) -> str:
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    if alias is None:
        return ", ".join(ident(column) for column in columns)
    return ", ".join(col(alias, column) for column in columns)


def join_condition(by_columns: List[str], left_alias: str, right_alias: str) -> str:
    comparisons = [
        f"{col(left_alias, column)} IS NOT DISTINCT FROM {col(right_alias, column)}"
        for column in by_columns
    ]
    return " AND ".join(comparisons) if comparisons else "TRUE"


def inputs_join_sql(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
) -> str:
    join_condition_sql = join_condition(by_columns, "a", "b")
    return (
        f"{table_ref(handles[table_id[0]])} AS a\n"
        f"  INNER JOIN {table_ref(handles[table_id[1]])} AS b\n"
        f"    ON {join_condition_sql}"
    )


def diff_predicate(
    column: str, allow_both_na: bool, left_alias: str, right_alias: str
) -> str:
    left = col(left_alias, column)
    right = col(right_alias, column)
    if allow_both_na:
        return f"{left} IS DISTINCT FROM {right}"
    return f"(({left} IS NULL AND {right} IS NULL) OR {left} IS DISTINCT FROM {right})"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


# --------------- Comparison-specific SQL assembly
def require_diff_table(
    comparison: "Comparison",
) -> duckdb.DuckDBPyRelation:
    diff_table = comparison.diff_table
    if diff_table is None:
        raise ComparisonError("Diff table is only available for materialize='all'.")
    return diff_table


def collect_diff_keys(comparison: "Comparison", columns: Sequence[str]) -> str:
    diff_table = require_diff_table(comparison)
    diff_table_sql = diff_table.sql_query()
    by_cols = select_cols(comparison.by_columns, alias="diffs")
    predicate = " OR ".join(f"diffs.{ident(column)}" for column in columns)
    return f"""
    SELECT
      {by_cols}
    FROM
      ({diff_table_sql}) AS diffs
    WHERE
      {predicate}
    """


def fetch_rows_by_keys(
    comparison: "Comparison",
    table: str,
    key_sql: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    if columns is None:
        select_cols_sql = "base.*"
    else:
        if not columns:
            raise ComparisonError("Column list must be non-empty")
        select_cols_sql = select_cols(columns, alias="base")
    join_condition_sql = join_condition(comparison.by_columns, "keys", "base")
    sql = f"""
    SELECT
      {select_cols_sql}
    FROM
      ({key_sql}) AS keys
      JOIN {table_ref(comparison._handles[table])} AS base
        ON {join_condition_sql}
    """
    return run_sql(comparison.connection, sql)


# --------------- Relation utilities
def run_sql(
    conn: Union[VersusConn, duckdb.DuckDBPyConnection],
    sql: str,
) -> duckdb.DuckDBPyRelation:
    return conn.sql(sql)


def relation_is_empty(
    relation: Union[duckdb.DuckDBPyRelation, SummaryRelation],
) -> bool:
    return relation.limit(1).fetchone() is None


def diff_lookup_from_intersection(
    relation: duckdb.DuckDBPyRelation,
) -> Dict[str, int]:
    rows = relation.fetchall()
    return {row[0]: int(row[1]) for row in rows}


def unmatched_lookup_from_rows(
    relation: duckdb.DuckDBPyRelation,
) -> Dict[str, int]:
    rows = relation.fetchall()
    return {row[0]: int(row[1]) for row in rows}


def rows_relation_sql(
    rows: Sequence[Sequence[Any]], schema: Sequence[Tuple[str, str]]
) -> str:
    if not rows:
        select_list = ", ".join(
            f"CAST(NULL AS {dtype}) AS {ident(name)}" for name, dtype in schema
        )
        return f"SELECT {select_list} LIMIT 0"
    value_rows = [
        "(" + ", ".join(sql_literal(value) for value in row) + ")" for row in rows
    ]
    alias_cols = ", ".join(f"col{i}" for i in range(len(schema)))
    select_list = ", ".join(
        f"CAST(col{i} AS {dtype}) AS {ident(name)}"
        for i, (name, dtype) in enumerate(schema)
    )
    return (
        f"SELECT {select_list} FROM (VALUES {', '.join(value_rows)}) AS v({alias_cols})"
    )


def materialize_temp_table(conn: VersusConn, sql: str) -> str:
    name = f"__versus_table_{uuid.uuid4().hex}"
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {ident(name)} AS {sql}")
    return name


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


def table_count(relation: Union[duckdb.DuckDBPyRelation, _TableHandle]) -> int:
    if isinstance(relation, _TableHandle):
        return relation.row_count
    row = relation.count("*").fetchall()[0]
    assert isinstance(row[0], int)
    return row[0]


def select_zero_from_table(
    comparison: "Comparison",
    table: str,
    columns: Optional[Sequence[str]] = None,
) -> duckdb.DuckDBPyRelation:
    handle = comparison._handles[table]
    if columns is None:
        sql = f"SELECT * FROM {table_ref(handle)} LIMIT 0"
        return run_sql(comparison.connection, sql)
    if not columns:
        raise ComparisonError("Column list must be non-empty")
    select_cols_sql = select_cols(columns)
    sql = f"SELECT {select_cols_sql} FROM {table_ref(handle)} LIMIT 0"
    return run_sql(comparison.connection, sql)
