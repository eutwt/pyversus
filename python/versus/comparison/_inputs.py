from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, cast

import duckdb

from ._exceptions import ComparisonError
from ._sql import ident, run_sql
from ._types import _Input, _TableHandle, VersusConn
from ._validation import validate_columns

if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import polars


def build_table_handle(
    conn: VersusConn,
    source: _Input,
    label: str,
    *,
    connection_supplied: bool,
) -> _TableHandle:
    name = f"__versus_{label}_{uuid.uuid4().hex}"
    if isinstance(source, duckdb.DuckDBPyRelation):
        return build_table_handle_from_relation(
            conn,
            source,
            label,
            name=name,
            connection_supplied=connection_supplied,
        )
    if isinstance(source, str):
        raise ComparisonError(
            "String inputs are not supported. Pass a DuckDB relation or pandas/polars "
            "DataFrame."
        )
    return build_table_handle_from_frame(conn, source, label, name=name)


def build_table_handle_from_relation(
    conn: VersusConn,
    source: duckdb.DuckDBPyRelation,
    label: str,
    *,
    name: str,
    connection_supplied: bool,
) -> _TableHandle:
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


def build_table_handle_from_frame(
    conn: VersusConn,
    source: _Input,
    label: str,
    *,
    name: str,
) -> _TableHandle:
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
    source: _Input,
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


def row_count_from_frame(source: _Input) -> Optional[int]:
    module = type(source).__module__
    if module.startswith("pandas"):
        return int(cast("pandas.DataFrame", source).shape[0])
    if module.startswith("polars"):
        return int(cast("polars.DataFrame", source).height)
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
