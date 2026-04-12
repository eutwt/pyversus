from pyspark.sql import DataFrame

from versus._spark.comparison._core import _TableHandle


def frame_is_empty(frame):
  return frame.limit(1).count() == 0


def diff_lookup_from_intersection(frame):
  return {row["column"]: int(row["n_diffs"]) for row in frame.collect()}


def unmatched_lookup_from_rows(frame):
  return {row["table_name"]: int(row["n_unmatched"]) for row in frame.collect()}


def table_count(table):
  if isinstance(table, _TableHandle):
    return table.row_count
  if isinstance(table, DataFrame):
    return int(table.count())
  raise TypeError(f"Unsupported table type: {type(table)}")


__all__ = [
  "diff_lookup_from_intersection",
  "frame_is_empty",
  "table_count",
  "unmatched_lookup_from_rows",
]
