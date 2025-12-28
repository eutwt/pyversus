from __future__ import annotations

import uuid
from typing import Any, Callable, Optional, Sequence, Tuple, cast

import duckdb

from . import _sql as q
from ._types import VersusConn


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


def rows_relation_sql(
    rows: Sequence[Sequence[Any]], schema: Sequence[Tuple[str, str]]
) -> str:
    if not rows:
        select_list = ", ".join(
            f"CAST(NULL AS {dtype}) AS {q.ident(name)}" for name, dtype in schema
        )
        return f"SELECT {select_list} LIMIT 0"
    value_rows = [
        "(" + ", ".join(q.sql_literal(value) for value in row) + ")" for row in rows
    ]
    alias_cols = ", ".join(f"col{i}" for i in range(len(schema)))
    select_list = ", ".join(
        f"CAST(col{i} AS {dtype}) AS {q.ident(name)}"
        for i, (name, dtype) in enumerate(schema)
    )
    return (
        f"SELECT {select_list} FROM (VALUES {', '.join(value_rows)}) AS v({alias_cols})"
    )


def materialize_temp_table(conn: VersusConn, sql: str) -> str:
    name = f"__versus_table_{uuid.uuid4().hex}"
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {q.ident(name)} AS {sql}")
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
    return conn.sql(f"SELECT * FROM {q.ident(table)}")


def build_rows_relation(
    conn: VersusConn,
    rows: Sequence[Sequence[Any]],
    schema: Sequence[Tuple[str, str]],
    materialize: bool,
) -> duckdb.DuckDBPyRelation:
    sql = rows_relation_sql(rows, schema)
    return finalize_relation(conn, sql, materialize)
