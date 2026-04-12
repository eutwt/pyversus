from pyspark.sql import functions as F
from pyspark.sql import types as T

from versus._spark.comparison._core import (
  SummaryDataFrame,
  _cache_frame,
  _rows_frame,
)

SummaryFrame = SummaryDataFrame


def rows_frame_data(rows, schema):
  return rows, schema


def cache_frame(conn, frame):
  del conn
  return _cache_frame(frame)


def finalize_frame(conn, frame, materialize):
  del conn
  if materialize:
    return _cache_frame(frame)
  return frame


def build_rows_frame(conn, rows, schema, materialize):
  frame = _rows_frame(conn, rows, schema)
  return finalize_frame(conn, frame, materialize)


def summary(comparison):
  value_diffs = (
    comparison.intersection.filter(F.col("n_diffs") > 0).limit(1).count() > 0
  )
  unmatched_cols = comparison.unmatched_cols.limit(1).count() > 0
  unmatched_rows = (
    comparison.unmatched_rows.filter(F.col("n_unmatched") > 0).limit(1).count()
    > 0
  )
  type_a_col = f"type_{comparison.table_id[0]}"
  type_b_col = f"type_{comparison.table_id[1]}"
  type_diffs = (
    comparison.intersection.filter(F.col(type_a_col) != F.col(type_b_col))
    .limit(1)
    .count()
    > 0
  )
  rows = [
    ("value_diffs", value_diffs),
    ("unmatched_cols", unmatched_cols),
    ("unmatched_rows", unmatched_rows),
    ("type_diffs", type_diffs),
  ]
  out = SummaryDataFrame(
    _rows_frame(
      comparison.spark,
      rows,
      [("difference", T.StringType()), ("found", T.BooleanType())],
    ),
    materialized=False,
  )
  return out


__all__ = [
  "SummaryFrame",
  "build_rows_frame",
  "cache_frame",
  "finalize_frame",
  "rows_frame_data",
  "summary",
]
