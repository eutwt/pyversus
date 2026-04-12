from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from versus._spark.comparison import _core

from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
  from .comparison import Comparison


def slice_diffs(
  comparison: "Comparison",
  table: str,
  columns: Optional[Sequence[str]] = None,
) -> DataFrame:
  table_name = v.normalize_table_arg(comparison, table)
  selected = v.resolve_column_list(comparison, columns)
  diff_cols = comparison._filter_diff_columns(selected)
  if not diff_cols:
    return _select_zero_from_table(comparison, table_name)
  if comparison._materialize_mode == "all":
    keys = _core._collect_diff_keys(comparison, diff_cols)
    return _fetch_rows_by_keys(comparison, table_name, keys)
  return _slice_diffs_inline(comparison, table_name, diff_cols)


def build_unmatched_keys_selection(
  comparison: "Comparison",
  table_name: str,
) -> DataFrame:
  out = comparison.unmatched_keys.filter(
    F.col("table_name") == table_name
  ).select(*comparison.by_columns)
  return out


def slice_unmatched(comparison: "Comparison", table: str) -> DataFrame:
  table_name = v.normalize_table_arg(comparison, table)
  if (
    comparison._unmatched_lookup is not None
    and comparison._unmatched_lookup[table_name] == 0
  ):
    return _select_zero_from_table(comparison, table_name)
  out = _fetch_rows_by_keys(
    comparison,
    table_name,
    build_unmatched_keys_selection(comparison, table_name),
  )
  return out


def slice_unmatched_both(comparison: "Comparison") -> DataFrame:
  out_cols = comparison.by_columns + comparison.common_columns
  table_names = [
    table_name
    for table_name in comparison.table_id
    if comparison._unmatched_lookup is None
    or comparison._unmatched_lookup.get(table_name, 0) != 0
  ]
  if not table_names:
    return _empty_table_name_frame(comparison, out_cols)
  frames = [
    _fetch_rows_by_keys_for_union(
      comparison,
      table_name,
      build_unmatched_keys_selection(comparison, table_name),
      index,
      out_cols,
    )
    for index, table_name in enumerate(table_names)
  ]
  order_cols = [F.col("__table_order"), *_order_cols(comparison.by_columns)]
  return _core._union_all(frames).orderBy(*order_cols).drop("__table_order")


def _slice_diffs_inline(
  comparison: "Comparison",
  table_name: str,
  diff_cols: Sequence[str],
) -> DataFrame:
  first, second = comparison.table_id
  joined = _core._join_inputs(
    comparison._handles,
    comparison.table_id,
    comparison.by_columns,
  )
  predicate = _core._combine_or(
    [
      _core._diff_expression(
        column,
        comparison.allow_both_na,
        "a",
        "b",
        comparison._handles[first].data_types,
        comparison._handles[second].data_types,
      )
      for column in diff_cols
    ]
  )
  alias = "a" if table_name == first else "b"
  handle = comparison._handles[table_name]
  select_parts = _aliased_columns(handle.columns, alias)
  order_cols = _order_cols(comparison.by_columns)
  out = (
    joined.filter(predicate)
    .select(*select_parts)
    .orderBy(*order_cols)
  )
  return out


def _fetch_rows_by_keys(
  comparison: "Comparison",
  table_name: str,
  keys: DataFrame,
  columns: Optional[Sequence[str]] = None,
) -> DataFrame:
  handle = comparison._handles[table_name]
  selected_columns = handle.columns if columns is None else list(columns)
  base = handle.frame.alias("base")
  keyed = keys.alias("keys")
  key_types = _core._frame_data_types(keys)
  join_expr = _core._join_condition(
    "keys",
    "base",
    comparison.by_columns,
    key_types,
    handle.data_types,
  )
  select_parts = _aliased_columns(selected_columns, "base")
  order_cols = _order_cols(comparison.by_columns)
  out = (
    keyed.join(base, join_expr, "inner")
    .select(*select_parts)
    .orderBy(*order_cols)
  )
  return out


def _fetch_rows_by_keys_for_union(
  comparison: "Comparison",
  table_name: str,
  keys: DataFrame,
  table_order: int,
  columns: Sequence[str],
) -> DataFrame:
  handle = comparison._handles[table_name]
  base = handle.frame.alias("base")
  keyed = keys.alias("keys")
  key_types = _core._frame_data_types(keys)
  join_expr = _core._join_condition(
    "keys",
    "base",
    comparison.by_columns,
    key_types,
    handle.data_types,
  )
  column_parts = _project_columns(comparison, table_name, "base", columns)
  out = keyed.join(base, join_expr, "inner").select(
    F.lit(table_order).alias("__table_order"),
    F.lit(table_name).alias("table_name"),
    *column_parts,
  )
  return out


def _select_zero_from_table(
  comparison: "Comparison",
  table_name: str,
  columns: Optional[Sequence[str]] = None,
) -> DataFrame:
  handle = comparison._handles[table_name]
  if columns is None:
    return handle.frame.limit(0)
  if not columns:
    raise _core.ComparisonError("Column list must be non-empty")
  return handle.frame.select(*list(columns)).limit(0)


def _empty_table_name_frame(
  comparison: "Comparison",
  columns: Sequence[str],
) -> DataFrame:
  schema = [("table_name", T.StringType()), *_schema_parts(comparison, columns)]
  out = _core._rows_frame(
    comparison.spark,
    [],
    schema,
  )
  return out


def _order_cols(columns: Sequence[str]) -> list[Column]:
  out = [F.col(column) for column in columns]
  return out


def _aliased_columns(columns: Sequence[str], alias: str) -> list[Column]:
  out = [F.col(f"{alias}.{column}").alias(column) for column in columns]
  return out


def _project_columns(
  comparison: "Comparison",
  table_name: str,
  alias: str,
  columns: Sequence[str],
) -> list[Column]:
  handle = comparison._handles[table_name]
  out = [
    _core._cast_to_type(
      F.col(f"{alias}.{column}"),
      handle.data_types[column],
      comparison._resolved_types[column],
    ).alias(column)
    for column in columns
  ]
  return out


def _schema_parts(
  comparison: "Comparison",
  columns: Sequence[str],
) -> list[tuple[str, T.DataType]]:
  out = [(column, comparison._resolved_types[column]) for column in columns]
  return out


__all__ = [
  "build_unmatched_keys_selection",
  "slice_diffs",
  "slice_unmatched",
  "slice_unmatched_both",
]
