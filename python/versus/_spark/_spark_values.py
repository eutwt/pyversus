"""Helpers for building Spark DataFrames from SQL VALUES clauses."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


def build_values_frame(
  spark: SparkSession,
  rows: Sequence[Sequence[Any]],
  schema: T.StructType | Sequence[str],
) -> DataFrame:
  """Build a DataFrame without spinning up Python worker processes."""
  struct = _normalize_schema(rows, schema)
  _validate_schema(struct)
  if not rows:
    return _empty_frame(spark, struct)
  return spark.sql(_rows_sql(rows, struct))


def _normalize_schema(
  rows: Sequence[Sequence[Any]],
  schema: T.StructType | Sequence[str],
) -> T.StructType:
  if isinstance(schema, T.StructType):
    return schema

  fields = [
    T.StructField(
      name,
      _infer_type([row[index] for row in rows]),
      True,
    )
    for index, name in enumerate(schema)
  ]
  out = T.StructType(fields)
  return out


def _infer_type(values: Sequence[Any]) -> T.DataType:
  seen = {type(value) for value in values if value is not None}
  if not seen:
    return T.StringType()
  if seen <= {bool}:
    return T.BooleanType()
  if seen <= {int}:
    return T.IntegerType()
  if seen <= {int, float}:
    return T.DoubleType()
  return T.StringType()


def _empty_frame(spark: SparkSession, schema: T.StructType) -> DataFrame:
  return spark.sql(_empty_sql(schema))


def _rows_sql(
  rows: Sequence[Sequence[Any]],
  schema: T.StructType,
) -> str:
  aliases = ", ".join(_quote_name(field.name) for field in schema)
  select_list = ", ".join(
    _cast_select(field.name, field.dataType) for field in schema
  )
  values_sql = ", ".join(
    "(" + ", ".join(_sql_literal(value) for value in row) + ")" for row in rows
  )
  out = (
    f"SELECT {select_list} FROM VALUES {values_sql} AS values_table ({aliases})"
  )
  return out


def _empty_sql(schema: T.StructType) -> str:
  select_list = ", ".join(
    f"CAST(NULL AS {_sql_type_name(field.dataType)}) "
    f"AS {_quote_name(field.name)}"
    for field in schema
  )
  return f"SELECT {select_list} LIMIT 0"


def _validate_schema(schema: T.StructType) -> None:
  for field in schema:
    _sql_type_name(field.dataType)


def _cast_select(name: str, data_type: T.DataType) -> str:
  quoted_name = _quote_name(name)
  sql_type = _sql_type_name(data_type)
  return f"CAST({quoted_name} AS {sql_type}) AS {quoted_name}"


def _sql_type_name(data_type: T.DataType) -> str:
  if isinstance(data_type, T.NullType):
    return "STRING"
  if isinstance(data_type, (T.ArrayType, T.MapType, T.StructType)):
    msg = f"Unsupported SQL VALUES type: {data_type.simpleString()}"
    raise TypeError(msg)
  return data_type.simpleString().upper()


def _sql_literal(value: Any) -> str:
  if value is None:
    return "NULL"
  if isinstance(value, bool):
    return "TRUE" if value else "FALSE"
  if isinstance(value, dt.datetime):
    stamp = value.isoformat(sep=" ", timespec="microseconds")
    return f"TIMESTAMP '{stamp}'"
  if isinstance(value, dt.date):
    return f"DATE '{value.isoformat()}'"
  if isinstance(value, Decimal):
    return str(value)
  if isinstance(value, str):
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
  return repr(value)


def _quote_name(name: str) -> str:
  escaped = name.replace("`", "``")
  return f"`{escaped}`"
