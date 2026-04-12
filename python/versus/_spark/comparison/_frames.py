from __future__ import annotations

from typing import Dict, List, Mapping, Optional, SupportsInt, Tuple, cast

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from versus._spark.comparison import _core

from . import _relations as r
from . import _summary as s
from . import _types as t


def build_tables_frame(
  conn: t.VersusConn,
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
) -> DataFrame:
  def row_for(identifier: str) -> Tuple[str, int, int]:
    handle = handles[identifier]
    return identifier, r.table_count(handle), len(handle.columns)

  rows = [row_for(identifier) for identifier in table_id]
  schema = [
    ("table_name", T.StringType()),
    ("nrow", T.LongType()),
    ("ncol", T.LongType()),
  ]
  return s.build_rows_frame(conn, rows, schema, False)


def build_by_frame(
  conn: t.VersusConn,
  by_columns: List[str],
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
) -> DataFrame:
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
    ("column", T.StringType()),
    (f"type_{first}", T.StringType()),
    (f"type_{second}", T.StringType()),
  ]
  return s.build_rows_frame(conn, rows, schema, False)


def build_unmatched_cols(
  conn: t.VersusConn,
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
) -> DataFrame:
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
    ("table_name", T.StringType()),
    ("column", T.StringType()),
    ("type", T.StringType()),
  ]
  return s.build_rows_frame(conn, rows, schema, False)


def build_intersection_frame(
  conn: t.VersusConn,
  value_columns: List[str],
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
  allow_both_na: bool,
  diff_table: Optional[DataFrame],
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  if diff_table is None:
    out = _build_intersection_frame_inline(
      conn,
      value_columns,
      handles,
      table_id,
      by_columns,
      allow_both_na,
      materialize,
    )
    return out
  out = _build_intersection_frame_with_table(
    conn,
    value_columns,
    handles,
    table_id,
    diff_table,
    materialize,
  )
  return out


def _build_empty_intersection_frame(
  conn: t.VersusConn,
  table_id: Tuple[str, str],
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  first, second = table_id
  schema = [
    ("column", T.StringType()),
    ("n_diffs", T.LongType()),
    (f"type_{first}", T.StringType()),
    (f"type_{second}", T.StringType()),
  ]
  frame = s.build_rows_frame(conn, [], schema, False)
  return frame, {} if materialize else None


def _build_intersection_frame_with_table(
  conn: t.VersusConn,
  value_columns: List[str],
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  diff_table: DataFrame,
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  if not value_columns:
    return _build_empty_intersection_frame(conn, table_id, materialize)

  count_parts = [_diff_count_part(column) for column in value_columns]
  counts_row = (
    diff_table.agg(*count_parts)
    .collect()[0]
    .asDict()
  )
  out = _build_intersection_rows(
    conn,
    value_columns,
    handles,
    table_id,
    counts_row,
    materialize,
  )
  return out


def _build_intersection_frame_inline(
  conn: t.VersusConn,
  value_columns: List[str],
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
  allow_both_na: bool,
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  if not value_columns:
    return _build_empty_intersection_frame(conn, table_id, materialize)

  first, second = table_id
  joined = _core._join_inputs(handles, table_id, by_columns)
  count_parts = [
    _inline_diff_count_part(
      column,
      allow_both_na,
      handles,
      first,
      second,
    )
    for column in value_columns
  ]
  counts_row = (
    joined.agg(*count_parts)
    .collect()[0]
    .asDict()
  )
  out = _build_intersection_rows(
    conn,
    value_columns,
    handles,
    table_id,
    counts_row,
    materialize,
  )
  return out


def _build_intersection_rows(
  conn: t.VersusConn,
  value_columns: List[str],
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  counts_row: Mapping[str, object],
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  first, second = table_id

  def diff_count(column: str) -> int:
    value = counts_row.get(column)
    if value is None:
      return 0
    return int(cast(SupportsInt, value))

  rows = [
    (
      column,
      diff_count(column),
      handles[first].types[column],
      handles[second].types[column],
    )
    for column in value_columns
  ]
  schema = [
    ("column", T.StringType()),
    ("n_diffs", T.LongType()),
    (f"type_{first}", T.StringType()),
    (f"type_{second}", T.StringType()),
  ]
  frame = s.build_rows_frame(conn, rows, schema, False)
  if not materialize:
    return frame, None
  return frame, r.diff_lookup_from_intersection(frame)


def _diff_count_part(column: str) -> Column:
  out = (
    F.sum(F.when(F.col(column), F.lit(1)).otherwise(F.lit(0)))
    .cast("long")
    .alias(column)
  )
  return out


def _inline_diff_count_part(
  column: str,
  allow_both_na: bool,
  handles: Mapping[str, t._TableHandle],
  first: str,
  second: str,
) -> Column:
  diff_expr = _core._diff_expression(
    column,
    allow_both_na,
    "a",
    "b",
    handles[first].data_types,
    handles[second].data_types,
  )
  out = F.sum(F.when(diff_expr, F.lit(1)).otherwise(F.lit(0))).cast(
    "long"
  ).alias(column)
  return out


def compute_diff_table(
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
  value_columns: List[str],
  allow_both_na: bool,
  resolved_types: Mapping[str, T.DataType],
) -> DataFrame:
  first, second = table_id
  spark = handles[first].frame.sparkSession
  if not value_columns:
    out = s.build_rows_frame(
      spark,
      [],
      [(column, resolved_types[column]) for column in by_columns],
      False,
    )
    return out

  joined = _core._join_inputs(handles, table_id, by_columns)
  select_parts = [
    _core._cast_to_type(
      F.col(f"a.{column}"),
      handles[first].data_types[column],
      resolved_types[column],
    ).alias(column)
    for column in by_columns
  ] + [
    _core._diff_expression(
      column,
      allow_both_na,
      "a",
      "b",
      handles[first].data_types,
      handles[second].data_types,
    ).alias(column)
    for column in value_columns
  ]
  diff_table = joined.select(*select_parts)
  predicate = _core._combine_or([F.col(column) for column in value_columns])
  order_cols = _order_cols(by_columns)
  out = diff_table.filter(predicate).orderBy(*order_cols)
  return out


def compute_unmatched_keys(
  handles: Mapping[str, t._TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
  resolved_types: Mapping[str, T.DataType],
) -> DataFrame:
  def build_unmatched_frame(index: int, identifier: str) -> DataFrame:
    other = table_id[1] if identifier == table_id[0] else table_id[0]
    left = handles[identifier].frame.alias("left")
    right = handles[other].frame.alias("right")
    join_expr = _core._join_condition(
      "left",
      "right",
      by_columns,
      handles[identifier].data_types,
      handles[other].data_types,
    )
    unmatched = left.join(right, join_expr, "left_anti")
    key_parts = _resolved_key_parts(
      handles,
      resolved_types,
      identifier,
      "left",
      by_columns,
    )
    out = unmatched.select(
      F.lit(index).alias("__table_order"),
      F.lit(identifier).alias("table_name"),
      *key_parts,
    )
    return out

  frames = [
    build_unmatched_frame(index, identifier)
    for index, identifier in enumerate(table_id)
  ]
  order_cols = [F.col("__table_order"), *_order_cols(by_columns)]
  out = (
    _core._union_all(frames)
    .orderBy(*order_cols)
    .drop("__table_order")
  )
  return out


def _order_cols(columns: List[str]) -> list[Column]:
  out = [F.col(column) for column in columns]
  return out


def _resolved_key_parts(
  handles: Mapping[str, t._TableHandle],
  resolved_types: Mapping[str, T.DataType],
  table_name: str,
  alias: str,
  columns: List[str],
) -> list[Column]:
  out = [
    _core._cast_to_type(
      F.col(f"{alias}.{column}"),
      handles[table_name].data_types[column],
      resolved_types[column],
    ).alias(column)
    for column in columns
  ]
  return out


def compute_unmatched_rows_summary(
  conn: t.VersusConn,
  unmatched_keys: DataFrame,
  table_id: Tuple[str, str],
  materialize: bool,
) -> Tuple[DataFrame, Optional[Dict[str, int]]]:
  counts = {
    row["table_name"]: int(row["count"])
    for row in unmatched_keys.groupBy("table_name").count().collect()
  }
  rows = [(identifier, counts.get(identifier, 0)) for identifier in table_id]
  frame = s.build_rows_frame(
    conn,
    rows,
    [
      ("table_name", T.StringType()),
      ("n_unmatched", T.LongType()),
    ],
    False,
  )
  if not materialize:
    return frame, None
  return frame, r.unmatched_lookup_from_rows(frame)


__all__ = [
  "build_by_frame",
  "build_intersection_frame",
  "build_tables_frame",
  "build_unmatched_cols",
  "compute_diff_table",
  "compute_unmatched_keys",
  "compute_unmatched_rows_summary",
]
