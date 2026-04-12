from pyspark.sql import DataFrame

from versus._spark.comparison._core import _build_table_handle, _TableHandle

from ._exceptions import ComparisonError


def build_table_handle(conn, source, label, *, connection_supplied):
  del conn, connection_supplied
  if not isinstance(source, DataFrame):
    raise ComparisonError("Inputs must be PySpark DataFrames.")
  return _build_table_handle(source, label)


def describe_input(conn, source_name, *, is_identifier):
  del conn, source_name, is_identifier
  raise ComparisonError(
    "Spark inputs do not expose DuckDB-style SQL descriptions."
  )


def input_reference(source_name, is_identifier):
  del is_identifier
  return source_name


def resolve_input_row_count(conn, source, source_name, *, is_identifier):
  del conn, source_name, is_identifier
  return row_count_from_frame(source)


def row_count_from_frame(source):
  if not isinstance(source, DataFrame):
    return None
  return int(source.count())


def raise_dataframe_input_error(label, connection_supplied, exc):
  del connection_supplied
  raise ComparisonError(f"`table_{label}` is not a PySpark DataFrame.") from exc


def assert_dataframe_input(conn, frame, label, connection_supplied):
  del conn, label, connection_supplied
  if not isinstance(frame, DataFrame):
    raise ComparisonError("Inputs must be PySpark DataFrames.")


__all__ = [
  "_TableHandle",
  "assert_dataframe_input",
  "build_table_handle",
  "describe_input",
  "input_reference",
  "raise_dataframe_input_error",
  "resolve_input_row_count",
  "row_count_from_frame",
]
