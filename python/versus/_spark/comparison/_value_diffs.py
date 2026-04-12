from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from versus._spark.comparison import _core

from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
  from .comparison import Comparison


def value_diffs(comparison: "Comparison", column: str) -> DataFrame:
  target_col = v.normalize_single_column(column)
  v.assert_column_allowed(comparison, target_col, "value_diffs")
  if comparison._materialize_mode == "all":
    keys = _core._collect_diff_keys(comparison, [target_col])
    return _value_diffs_from_keys(comparison, keys, target_col)
  return _value_diffs_inline(comparison, target_col)


def value_diffs_stacked(
  comparison: "Comparison",
  columns: Optional[Sequence[str]] = None,
) -> DataFrame:
  selected = v.resolve_column_list(comparison, columns, allow_empty=False)
  diff_cols = comparison._filter_diff_columns(selected)
  if not diff_cols:
    return _empty_value_diffs_stacked(comparison)
  if comparison._materialize_mode == "all":
    frames = [
      _stack_value_diffs_by_keys_frame(
        comparison,
        column,
        _core._collect_diff_keys(comparison, [column]),
        index,
      )
      for index, column in enumerate(diff_cols)
    ]
  else:
    frames = [
      _stack_value_diffs_inline_frame(comparison, column, index)
      for index, column in enumerate(diff_cols)
    ]
  order_cols = [F.col("__column_order"), *_by_parts(comparison)]
  return _core._union_all(frames).orderBy(*order_cols).drop("__column_order")


def stack_value_diffs_by_keys(
  comparison: "Comparison",
  column: str,
  key_frame: DataFrame,
) -> DataFrame:
  out = _stack_value_diffs_by_keys_frame(
    comparison,
    column,
    key_frame,
    0,
  ).drop("__column_order")
  return out


def stack_value_diffs_inline(
  comparison: "Comparison",
  column: str,
) -> DataFrame:
  out = _stack_value_diffs_inline_frame(
    comparison,
    column,
    0,
  ).drop("__column_order")
  return out


def _value_diffs_inline(
  comparison: "Comparison",
  target_col: str,
) -> DataFrame:
  first, second = comparison.table_id
  joined = _core._join_inputs(
    comparison._handles,
    comparison.table_id,
    comparison.by_columns,
  )
  predicate = _core._diff_expression(
    target_col,
    comparison.allow_both_na,
    "a",
    "b",
    comparison._handles[first].data_types,
    comparison._handles[second].data_types,
  )
  key_parts = _resolved_by_parts(comparison, first, "a")
  order_cols = _by_parts(comparison)
  out = (
    joined.filter(predicate)
    .select(
      F.col(f"a.{target_col}").alias(f"{target_col}_{first}"),
      F.col(f"b.{target_col}").alias(f"{target_col}_{second}"),
      *key_parts,
    )
    .orderBy(*order_cols)
  )
  return out


def _value_diffs_from_keys(
  comparison: "Comparison",
  keys: DataFrame,
  column: str,
) -> DataFrame:
  first, second = comparison.table_id
  left = comparison._handles[first].frame.alias("a")
  right = comparison._handles[second].frame.alias("b")
  keyed = keys.alias("keys")
  key_types = _core._frame_data_types(keys)
  join_left = _core._join_condition(
    "keys",
    "a",
    comparison.by_columns,
    key_types,
    comparison._handles[first].data_types,
  )
  join_right = _core._join_condition(
    "keys",
    "b",
    comparison.by_columns,
    key_types,
    comparison._handles[second].data_types,
  )
  joined = keyed.join(left, join_left, "inner").join(right, join_right, "inner")
  key_parts = _aliased_by_parts(comparison, "keys")
  order_cols = _by_parts(comparison)
  out = joined.select(
    F.col(f"a.{column}").alias(f"{column}_{first}"),
    F.col(f"b.{column}").alias(f"{column}_{second}"),
    *key_parts,
  ).orderBy(*order_cols)
  return out


def _stack_value_diffs_by_keys_frame(
  comparison: "Comparison",
  column: str,
  key_frame: DataFrame,
  column_order: int,
) -> DataFrame:
  first, second = comparison.table_id
  base = _value_diffs_from_keys(comparison, key_frame, column)
  key_parts = _by_parts(comparison)
  out = base.select(
    F.lit(column_order).alias("__column_order"),
    F.lit(column).alias("column"),
    F.col(f"{column}_{first}").cast("string").alias(f"val_{first}"),
    F.col(f"{column}_{second}").cast("string").alias(f"val_{second}"),
    *key_parts,
  )
  return out


def _stack_value_diffs_inline_frame(
  comparison: "Comparison",
  column: str,
  column_order: int,
) -> DataFrame:
  first, second = comparison.table_id
  joined = _core._join_inputs(
    comparison._handles,
    comparison.table_id,
    comparison.by_columns,
  )
  predicate = _core._diff_expression(
    column,
    comparison.allow_both_na,
    "a",
    "b",
    comparison._handles[first].data_types,
    comparison._handles[second].data_types,
  )
  key_parts = _resolved_by_parts(comparison, first, "a")
  out = joined.filter(predicate).select(
    F.lit(column_order).alias("__column_order"),
    F.lit(column).alias("column"),
    F.col(f"a.{column}").cast("string").alias(f"val_{first}"),
    F.col(f"b.{column}").cast("string").alias(f"val_{second}"),
    *key_parts,
  )
  return out


def _empty_value_diffs_stacked(comparison: "Comparison") -> DataFrame:
  first, second = comparison.table_id
  schema = [
    ("column", T.StringType()),
    (f"val_{first}", T.StringType()),
    (f"val_{second}", T.StringType()),
    *_by_schema(comparison),
  ]
  out = _core._rows_frame(
    comparison.spark,
    [],
    schema,
  )
  return out


def _by_parts(comparison: "Comparison") -> list[Column]:
  out = [F.col(column) for column in comparison.by_columns]
  return out


def _aliased_by_parts(comparison: "Comparison", alias: str) -> list[Column]:
  out = [
    F.col(f"{alias}.{column}").alias(column)
    for column in comparison.by_columns
  ]
  return out


def _resolved_by_parts(
  comparison: "Comparison",
  table_name: str,
  alias: str,
) -> list[Column]:
  handle = comparison._handles[table_name]
  out = [
    _core._cast_to_type(
      F.col(f"{alias}.{column}"),
      handle.data_types[column],
      comparison._resolved_types[column],
    ).alias(column)
    for column in comparison.by_columns
  ]
  return out


def _by_schema(comparison: "Comparison") -> list[tuple[str, T.DataType]]:
  out = [
    (column, comparison._resolved_types[column])
    for column in comparison.by_columns
  ]
  return out


__all__ = [
  "stack_value_diffs_by_keys",
  "stack_value_diffs_inline",
  "value_diffs",
  "value_diffs_stacked",
]
