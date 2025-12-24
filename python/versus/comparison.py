from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb
import polars as pl


class ComparisonError(ValueError):
    """Raised when comparison inputs are invalid or inconsistent."""


@dataclass
class _TableHandle:
    name: str
    display: str
    columns: List[str]
    types: Dict[str, str]
    cleanup: Callable[[], None]


class Comparison:
    """In-memory description of how two relations differ."""

    def __init__(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        handles: Mapping[str, _TableHandle],
        table_id: Tuple[str, str],
        by_columns: List[str],
        allow_both_na: bool,
        tables: pl.DataFrame,
        by: pl.DataFrame,
        intersection: pl.DataFrame,
        unmatched_cols: pl.DataFrame,
        unmatched_rows: pl.DataFrame,
        common_columns: List[str],
        table_columns: Mapping[str, List[str]],
        diff_key_tables: Mapping[str, "DiffKeyTable"],
        unmatched_tables: Mapping[str, str],
        temp_tables: Sequence[str],
    ) -> None:
        self.connection = connection
        self._handles = handles
        self.table_id = table_id
        self.by_columns = by_columns
        self.allow_both_na = allow_both_na
        self.tables = tables
        self.by = by
        self.intersection = intersection
        self.unmatched_cols = unmatched_cols
        self.unmatched_rows = unmatched_rows
        self.common_columns = common_columns
        self.table_columns = table_columns
        self.diff_key_tables = diff_key_tables
        self.diff_rows = diff_key_tables
        self._unmatched_tables = unmatched_tables
        self._temp_tables = list(temp_tables)
        if intersection.height > 0:
            col_names = intersection["column"].to_list()
            diff_counts = intersection["n_diffs"].to_list()
            self._diff_lookup = dict(zip(col_names, diff_counts))
        else:
            self._diff_lookup = {}
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        for handle in self._handles.values():
            try:
                handle.cleanup()
            except duckdb.Error:
                pass
        for view in self._temp_tables:
            try:
                self.connection.execute(f"DROP TABLE IF EXISTS {_ident(view)}")
            except duckdb.Error:
                pass
        self._closed = True

    def __del__(self) -> None:  # pragma: no cover
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return (
            "Comparison("
            f"tables=\n{self.tables}\n"
            f"by=\n{self.by}\n"
            f"intersection=\n{self.intersection}\n"
            f"unmatched_cols=\n{self.unmatched_cols}\n"
            f"unmatched_rows=\n{self.unmatched_rows}\n"
            ")"
        )

    def value_diffs(self, column: str) -> pl.DataFrame:
        target_col = _normalize_single_column(column)
        _ensure_column_allowed(self, target_col, "value_diffs")
        diff_keys = self.diff_key_tables.get(target_col)
        key_table = diff_keys.table if diff_keys is not None else None
        col_names = [
            f"{target_col}_{self.table_id[0]}",
            f"{target_col}_{self.table_id[1]}",
            *self.by_columns,
        ]
        if key_table is None:
            return _empty_value_diffs(self, target_col)

        table_a, table_b = self.table_id
        select_cols = [
            f"{_col('a', target_col)} AS {_ident(f'{target_col}_{table_a}')}",
            f"{_col('b', target_col)} AS {_ident(f'{target_col}_{table_b}')}",
            *[f"{_col('keys', by)} AS {_ident(by)}" for by in self.by_columns],
        ]
        join_a = _join_condition(self.by_columns, "keys", "a")
        join_b = _join_condition(self.by_columns, "keys", "b")
        sql = f"""
        SELECT DISTINCT {", ".join(select_cols)}
        FROM {_ident(key_table)} AS keys
        JOIN {_ident(self._handles[table_a].name)} AS a
          ON {join_a}
        JOIN {_ident(self._handles[table_b].name)} AS b
          ON {join_b}
        """
        return _run_query(self.connection, sql)

    def value_diffs_stacked(self, columns: Optional[Sequence[str]] = None) -> pl.DataFrame:
        selected = _resolve_column_list(self, columns)
        table_a, table_b = self.table_id
        frames: List[pl.DataFrame] = []
        for column in selected:
            df = self.value_diffs(column)
            if df.height == 0:
                continue
            df = df.rename(
                {
                    f"{column}_{table_a}": f"val_{table_a}",
                    f"{column}_{table_b}": f"val_{table_b}",
                }
            )
            df = df.with_columns(pl.lit(column).alias("column"))
            ordered = ["column", f"val_{table_a}", f"val_{table_b}", *self.by_columns]
            frames.append(df.select(ordered))
        if not frames:
            sample = self.value_diffs(selected[0])
            sample = sample.rename(
                {
                    f"{selected[0]}_{table_a}": f"val_{table_a}",
                    f"{selected[0]}_{table_b}": f"val_{table_b}",
                }
            )
            sample = sample.with_columns(pl.lit(selected[0]).alias("column"))
            ordered = ["column", f"val_{table_a}", f"val_{table_b}", *self.by_columns]
            return sample.select(ordered).head(0)
        val_columns = [f"val_{table_a}", f"val_{table_b}"]
        for column in val_columns:
            dtypes = {frame[column].dtype for frame in frames if column in frame.columns}
            if len(dtypes) > 1:
                frames = [frame.with_columns(pl.col(column).cast(pl.Utf8)) for frame in frames]
        return pl.concat(frames)

    def slice_diffs(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
    ) -> pl.DataFrame:
        table_name = _normalize_table_arg(self, table)
        selected = _resolve_column_list(self, columns)
        diff_cols = [col for col in selected if self._diff_lookup.get(col, 0) > 0]
        ordered_cols: List[str] = []
        for col in [*self.by_columns, *selected]:
            if col not in ordered_cols:
                ordered_cols.append(col)
        if not diff_cols:
            return _select_zero_from_table(self, table_name, ordered_cols)
        key_sql = _collect_diff_keys(self, diff_cols)
        if key_sql is None:
            return _select_zero_from_table(self, table_name, ordered_cols)
        return _fetch_rows_by_keys(self, table_name, key_sql, ordered_cols)

    def slice_unmatched(self, table: str) -> pl.DataFrame:
        table_name = _normalize_table_arg(self, table)
        table_ref = self._unmatched_tables.get(table_name)
        if table_ref is None:
            return _select_zero_from_table(self, table_name, self.table_columns[table_name])
        key_sql = f"SELECT * FROM {_ident(table_ref)}"
        return _fetch_rows_by_keys(self, table_name, key_sql, self.table_columns[table_name])

    def slice_unmatched_both(self) -> pl.DataFrame:
        frames = []
        out_cols = self.by_columns + self.common_columns
        for table_name in self.table_id:
            df = self.slice_unmatched(table_name)
            if df.height == 0:
                continue
            subset = df.select(out_cols).with_columns(pl.lit(table_name).alias("table"))
            frames.append(subset.select(["table", *out_cols]))
        if not frames:
            base = _select_zero_from_table(self, self.table_id[0], out_cols)
            return base.with_columns(pl.lit(self.table_id[0]).alias("table")).select(["table", *out_cols])
        return pl.concat(frames)

    def weave_diffs_wide(
        self,
        columns: Optional[Sequence[str]] = None,
        suffix: Optional[Tuple[str, str]] = None,
    ) -> pl.DataFrame:
        selected = _resolve_column_list(self, columns)
        diff_cols = [col for col in selected if self._diff_lookup.get(col, 0) > 0]
        table_a, table_b = self.table_id
        out_cols = self.by_columns + self.common_columns
        if not diff_cols:
            return _select_zero_from_table(self, table_a, out_cols)
        suffix = _resolve_suffix(suffix, self.table_id)
        keys = _collect_diff_keys(self, diff_cols)
        if keys is None:
            return _select_zero_from_table(self, table_a, out_cols)

        rows_a = _fetch_rows_by_keys(self, table_a, keys, out_cols)
        rows_b = _fetch_rows_by_keys(self, table_b, keys, [*self.by_columns, *diff_cols])

        for column in diff_cols:
            rows_a = rows_a.rename({column: f"{column}{suffix[0]}"})
            rows_b = rows_b.rename({column: f"{column}{suffix[1]}"})

        rows_b = rows_b.select([*self.by_columns, *[f"{col}{suffix[1]}" for col in diff_cols]])
        merged = rows_a.join(rows_b, on=self.by_columns, how="left")

        ordered = self.by_columns.copy()
        for column in self.common_columns:
            if column in diff_cols:
                ordered.append(f"{column}{suffix[0]}")
                ordered.append(f"{column}{suffix[1]}")
            else:
                ordered.append(column)
        return merged.select(ordered)

    def weave_diffs_long(
        self,
        columns: Optional[Sequence[str]] = None,
    ) -> pl.DataFrame:
        selected = _resolve_column_list(self, columns)
        diff_cols = [col for col in selected if self._diff_lookup.get(col, 0) > 0]
        table_a, table_b = self.table_id
        out_cols = self.by_columns + self.common_columns
        if not diff_cols:
            base = _select_zero_from_table(self, table_a, out_cols)
            return base.with_columns(pl.lit(table_a).alias("table")).select(["table", *out_cols])
        keys = _collect_diff_keys(self, diff_cols)
        if keys is None:
            base = _select_zero_from_table(self, table_a, out_cols)
            return base.with_columns(pl.lit(table_a).alias("table")).select(["table", *out_cols])

        rows_a = _fetch_rows_by_keys(self, table_a, keys, out_cols).with_columns(
            pl.lit(table_a).alias("table")
        )
        rows_b = _fetch_rows_by_keys(self, table_b, keys, out_cols).with_columns(
            pl.lit(table_b).alias("table")
        )
        return pl.concat(
            [
                rows_a.select(["table", *out_cols]),
                rows_b.select(["table", *out_cols]),
            ],
            how="vertical",
        )


def compare(
    table_a: Any,
    table_b: Any,
    *,
    by: Sequence[str],
    allow_both_na: bool = True,
    coerce: bool = True,
    table_id: Tuple[str, str] = ("a", "b"),
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> Comparison:
    conn = connection or duckdb.default_connection
    clean_ids = _validate_table_id(table_id)
    by_columns = _normalize_column_list(by, "by", allow_empty=False)
    handles = {
        clean_ids[0]: _register_input_view(conn, table_a, clean_ids[0]),
        clean_ids[1]: _register_input_view(conn, table_b, clean_ids[1]),
    }
    _validate_columns_exist(by_columns, handles, clean_ids)
    if not coerce:
        _validate_class_compatibility(handles, clean_ids)
    for identifier in clean_ids:
        _ensure_unique_by(conn, handles[identifier], by_columns, identifier)

    tables_frame = _build_tables_frame(conn, handles, clean_ids)
    by_frame = _build_by_frame(by_columns, handles, clean_ids)
    common_all = [col for col in handles[clean_ids[0]].columns if col in handles[clean_ids[1]].columns]
    value_columns = [col for col in common_all if col not in by_columns]
    unmatched_cols = _build_unmatched_cols(handles, clean_ids)
    diff_tables = _compute_diff_key_tables(
        conn, handles, clean_ids, by_columns, value_columns, allow_both_na
    )
    diff_key_handles = {col: DiffKeyTable(conn, diff_tables.get(col)) for col in value_columns}
    intersection = _build_intersection_frame(
        value_columns, handles, clean_ids, diff_key_handles, conn
    )
    unmatched_rows_df, unmatched_tables = _compute_unmatched_rows(conn, handles, clean_ids, by_columns)
    temp_tables = list(diff_tables.values()) + list(unmatched_tables.values())

    return Comparison(
        connection=conn,
        handles=handles,
        table_id=clean_ids,
        by_columns=by_columns,
        allow_both_na=allow_both_na,
        tables=tables_frame,
        by=by_frame,
        intersection=intersection,
        unmatched_cols=unmatched_cols,
        unmatched_rows=unmatched_rows_df,
        common_columns=value_columns,
        table_columns={identifier: handle.columns[:] for identifier, handle in handles.items()},
        diff_key_tables=diff_key_handles,
        unmatched_tables=unmatched_tables,
        temp_tables=temp_tables,
    )


# Helper utilities -----------------------------------------------------------


def _normalize_table_arg(comparison: Comparison, table: str) -> str:
    if table not in comparison.table_id:
        allowed = ", ".join(comparison.table_id)
        raise ComparisonError(f"`table` must be one of: {allowed}")
    return table


def _normalize_single_column(column: str) -> str:
    if isinstance(column, str):
        return column
    raise ComparisonError("`column` must be a column name")


def _resolve_column_list(comparison: Comparison, columns: Optional[Sequence[str]]) -> List[str]:
    if columns is None:
        return comparison.common_columns[:]
    cols = _normalize_column_list(columns, "column", allow_empty=True)
    if not cols:
        raise ComparisonError("`columns` must select at least one column")
    missing = [col for col in cols if col not in comparison.common_columns]
    if missing:
        raise ComparisonError(f"Columns not part of the comparison: {', '.join(missing)}")
    return cols


def _ensure_column_allowed(comparison: Comparison, column: str, func: str) -> None:
    if column not in comparison.common_columns:
        raise ComparisonError(f"`{func}` can only reference columns in both tables: {column}")


def _resolve_suffix(suffix: Optional[Tuple[str, str]], table_id: Tuple[str, str]) -> Tuple[str, str]:
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
            raise ComparisonError(f"`{arg_name}` must be a sequence of column names") from exc
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
    """Register the provided relation or SQL string as a temp view and capture metadata."""
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

    conn.execute(f"CREATE OR REPLACE TEMP VIEW {_ident(name)} AS SELECT * FROM {source_ref}")
    cleanup_statements.insert(0, f"DROP VIEW IF EXISTS {_ident(name)}")

    def _cleanup() -> None:
        for stmt in cleanup_statements:
            try:
                conn.execute(stmt)
            except duckdb.Error:
                pass

    columns, types = _describe_view(conn, name)
    return _TableHandle(name=name, display=display, columns=columns, types=types, cleanup=_cleanup)


def _describe_view(conn: duckdb.DuckDBPyConnection, name: str) -> Tuple[List[str], Dict[str, str]]:
    df = _run_query(conn, f"DESCRIBE SELECT * FROM {_ident(name)}")
    columns = df["column_name"].to_list()
    types = dict(zip(df["column_name"].to_list(), df["column_type"].to_list()))
    return columns, types


def _build_tables_frame(
    conn: duckdb.DuckDBPyConnection,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> pl.DataFrame:
    rows = []
    for identifier in table_id:
        handle = handles[identifier]
        count = conn.sql(f"SELECT COUNT(*) AS n FROM {_ident(handle.name)}").fetchone()[0]
        rows.append(
            {
                "table": identifier,
                "source": handle.display,
                "nrows": count,
                "ncols": len(handle.columns),
            }
        )
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        {
            "table": pl.Series(name="table", values=[], dtype=pl.String),
            "source": pl.Series(name="source", values=[], dtype=pl.String),
            "nrows": pl.Series(name="nrows", values=[], dtype=pl.Int64),
            "ncols": pl.Series(name="ncols", values=[], dtype=pl.Int64),
        }
    )


def _build_by_frame(
    by_columns: List[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> pl.DataFrame:
    rows = []
    first, second = table_id
    for column in by_columns:
        rows.append(
            {
                "column": column,
                f"class_{first}": handles[first].types.get(column, ""),
                f"class_{second}": handles[second].types.get(column, ""),
            }
        )
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        {
            "column": pl.Series(name="column", values=[], dtype=pl.String),
            f"class_{first}": pl.Series(name=f"class_{first}", values=[], dtype=pl.String),
            f"class_{second}": pl.Series(name=f"class_{second}", values=[], dtype=pl.String),
        }
    )


def _build_unmatched_cols(
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> pl.DataFrame:
    rows = []
    first, second = table_id
    cols_first = set(handles[first].columns)
    cols_second = set(handles[second].columns)
    for column in sorted(cols_first - cols_second):
        rows.append({"table": first, "column": column, "class": handles[first].types[column]})
    for column in sorted(cols_second - cols_first):
        rows.append({"table": second, "column": column, "class": handles[second].types[column]})
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        {
            "table": pl.Series("table", [], dtype=pl.String),
            "column": pl.Series("column", [], dtype=pl.String),
            "class": pl.Series("class", [], dtype=pl.String),
        }
    )


def _build_intersection_frame(
    value_columns: List[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    diff_key_tables: Mapping[str, "DiffKeyTable"],
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    rows = []
    first, second = table_id
    for column in value_columns:
        diff_keys = diff_key_tables.get(column)
        table = diff_keys.table if diff_keys is not None else None
        rows.append(
            {
                "column": column,
                "n_diffs": _table_count(conn, table),
                f"class_{first}": handles[first].types.get(column, ""),
                f"class_{second}": handles[second].types.get(column, ""),
                "diff_rows": diff_keys,
            }
        )
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame(
        {
            "column": pl.Series(name="column", values=[], dtype=pl.String),
            "n_diffs": pl.Series(name="n_diffs", values=[], dtype=pl.Int64),
            f"class_{first}": pl.Series(name=f"class_{first}", values=[], dtype=pl.String),
            f"class_{second}": pl.Series(name=f"class_{second}", values=[], dtype=pl.String),
            "diff_rows": pl.Series(name="diff_rows", values=[], dtype=pl.Object),
        }
    )


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
    select_by = ", ".join(f"{_col('a', col)} AS {_ident(col)}" for col in by_columns)
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
) -> Tuple[pl.DataFrame, Dict[str, str]]:
    tables: Dict[str, str] = {}
    summary_parts = []
    for identifier in table_id:
        other = table_id[1] if identifier == table_id[0] else table_id[0]
        handle_left = handles[identifier]
        handle_right = handles[other]
        select_by = ", ".join(f"{_col('left_tbl', col)} AS {_ident(col)}" for col in by_columns)
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
        summary_parts.append(f"SELECT '{identifier}' AS table, * FROM {_ident(table_name)}")
    if summary_parts:
        summary_sql = " UNION ALL ".join(summary_parts)
        summary_df = _run_query(conn, summary_sql)
    else:
        sample_handle = handles[table_id[0]]
        select_cols = ", ".join(f"{_col('base', col)} AS {_ident(col)}" for col in by_columns)
        select_clause = f", {select_cols}" if select_cols else ""
        sql = f"""
        SELECT '{table_id[0]}' AS table{select_clause}
        FROM {_ident(sample_handle.name)} AS base
        LIMIT 0
        """
        summary_df = _run_query(conn, sql)
    return summary_df, tables


def _collect_diff_keys(comparison: Comparison, columns: Sequence[str]) -> Optional[str]:
    selects = []
    for column in columns:
        diff_keys = comparison.diff_key_tables.get(column)
        table = diff_keys.table if diff_keys is not None else None
        if table is None:
            continue
        selects.append(f"SELECT * FROM {_ident(table)}")
    if not selects:
        return None
    if len(selects) == 1:
        return selects[0]
    return " UNION DISTINCT ".join(selects)


def _fetch_rows_by_keys(
    comparison: Comparison,
    table: str,
    key_sql: Optional[str],
    columns: Sequence[str],
) -> pl.DataFrame:
    if key_sql is None:
        return _select_zero_from_table(comparison, table, columns)
    select_cols = ", ".join(f"{_col('base', col)} AS {_ident(col)}" for col in columns)
    join_condition = " AND ".join(
        f"{_col('keys', col)} IS NOT DISTINCT FROM {_col('base', col)}" for col in comparison.by_columns
    )
    sql = f"""
    SELECT DISTINCT {select_cols}
    FROM ({key_sql}) AS keys
    JOIN {_ident(comparison._handles[table].name)} AS base
      ON {join_condition}
    """
    return _run_query(comparison.connection, sql)


def _run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    relation = conn.sql(sql)
    df = relation.pl()
    if df is None:
        return _empty_df(relation.columns)
    return df


def _validate_columns_exist(
    by_columns: Iterable[str],
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
) -> None:
    missing_a = [col for col in by_columns if col not in handles[table_id[0]].columns]
    missing_b = [col for col in by_columns if col not in handles[table_id[1]].columns]
    if missing_a:
        raise ComparisonError(f"`by` columns not found in `{table_id[0]}`: {', '.join(missing_a)}")
    if missing_b:
        raise ComparisonError(f"`by` columns not found in `{table_id[1]}`: {', '.join(missing_b)}")


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
    cols = ", ".join(_col("t", column) for column in by_columns)
    sql = f"""
    SELECT {cols}, COUNT(*) AS n
    FROM {_ident(handle.name)} AS t
    GROUP BY {cols}
    HAVING COUNT(*) > 1
    LIMIT 1
    """
    df = _run_query(conn, sql)
    if df.height > 0:
        values = ", ".join(f"{col}={df[col][0]!r}" for col in by_columns)
        raise ComparisonError(f"`{identifier}` has more than one row for by values ({values})")


def _diff_predicate(column: str, allow_both_na: bool, left_alias: str, right_alias: str) -> str:
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
        f"{_col(left_alias, column)} IS NOT DISTINCT FROM {_col(right_alias, column)}" for column in by_columns
    ]
    return " AND ".join(comparisons) if comparisons else "TRUE"


def _col(alias: str, column: str) -> str:
    return f"{alias}.{_ident(column)}"


def _table_count(conn: duckdb.DuckDBPyConnection, table_name: Optional[str]) -> int:
    if table_name is None:
        return 0
    return conn.sql(f"SELECT COUNT(*) FROM {_ident(table_name)}").fetchone()[0]


def _empty_df(columns: Sequence[str]) -> pl.DataFrame:
    return pl.DataFrame({col: [] for col in columns})


def _select_zero_from_table(comparison: Comparison, table: str, columns: Sequence[str]) -> pl.DataFrame:
    if not columns:
        return pl.DataFrame()
    select_cols = ", ".join(f"{_col('base', col)} AS {_ident(col)}" for col in columns)
    sql = f"""
    SELECT {select_cols}
    FROM {_ident(comparison._handles[table].name)} AS base
    LIMIT 0
    """
    return _run_query(comparison.connection, sql)


def _empty_value_diffs(comparison: Comparison, column: str) -> pl.DataFrame:
    table_a, table_b = comparison.table_id
    select_cols = [
        f"{_col('a', column)} AS {_ident(f'{column}_{table_a}')}",
        f"{_col('b', column)} AS {_ident(f'{column}_{table_b}')}",
        *[f"{_col('a', by)} AS {_ident(by)}" for by in comparison.by_columns],
    ]
    join_condition = _join_condition(comparison.by_columns, "a", "b")
    sql = f"""
    SELECT DISTINCT {", ".join(select_cols)}
    FROM {_ident(comparison._handles[table_a].name)} AS a
    JOIN {_ident(comparison._handles[table_b].name)} AS b
      ON {join_condition}
    LIMIT 0
    """
    return _run_query(comparison.connection, sql)


def _ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _materialize_temp_table(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    name = f"__versus_table_{uuid.uuid4().hex}"
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {_ident(name)} AS {sql}")
    return name


@dataclass
class DiffKeyTable:
    connection: duckdb.DuckDBPyConnection
    table: Optional[str]

    def df(self) -> pl.DataFrame:
        if self.table is None:
            return pl.DataFrame()
        return _run_query(self.connection, f"SELECT * FROM {_ident(self.table)}")

    def __repr__(self) -> str:
        if self.table is None:
            return "<0 rows>"
        count = self.connection.sql(f"SELECT COUNT(*) FROM {_ident(self.table)}").fetchone()[0]
        return f"<{count} rows>"
