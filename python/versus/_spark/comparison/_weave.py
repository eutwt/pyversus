from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from versus._spark.comparison import _core

from . import _slices as l
from . import _validation as v

if TYPE_CHECKING:  # pragma: no cover
  from .comparison import Comparison


def weave_diffs_wide(
  comparison: "Comparison",
  columns: Optional[Sequence[str]] = None,
  suffix: Optional[Tuple[str, str]] = None,
) -> DataFrame:
  selected = v.resolve_column_list(comparison, columns)
  diff_cols = comparison._filter_diff_columns(selected)
  table_a = comparison.table_id[0]
  out_cols = comparison.by_columns + comparison.common_columns
  if not diff_cols:
    return l._select_zero_from_table(comparison, table_a, out_cols)
  clean_suffix = resolve_suffix(suffix, comparison.table_id)
  if comparison._materialize_mode == "all":
    keys = _core._collect_diff_keys(comparison, diff_cols)
    out = _weave_diffs_wide_with_keys(
      comparison,
      diff_cols,
      clean_suffix,
      keys,
    )
    return out
  return _weave_diffs_wide_inline(comparison, diff_cols, clean_suffix)


def weave_diffs_long(
  comparison: "Comparison",
  columns: Optional[Sequence[str]] = None,
) -> DataFrame:
  selected = v.resolve_column_list(comparison, columns)
  diff_cols = comparison._filter_diff_columns(selected)
  out_cols = comparison.by_columns + comparison.common_columns
  if not diff_cols:
    return l._empty_table_name_frame(comparison, out_cols)
  if comparison._materialize_mode == "all":
    out = _weave_diffs_long_with_keys(
      comparison,
      _core._collect_diff_keys(comparison, diff_cols),
    )
    return out
  return _weave_diffs_long_inline(comparison, diff_cols)


def resolve_suffix(
  suffix: Optional[Tuple[str, str]],
  table_id: Tuple[str, str],
) -> Tuple[str, str]:
  if suffix is None:
    return (f"_{table_id[0]}", f"_{table_id[1]}")
  if (
    not isinstance(suffix, (tuple, list))
    or len(suffix) != 2
    or not all(isinstance(item, str) for item in suffix)
  ):
    raise _core.ComparisonError(
      "`suffix` must be a tuple of two strings or None"
    )
  if suffix[0] == suffix[1]:
    raise _core.ComparisonError("Entries of `suffix` must be distinct")
  return (suffix[0], suffix[1])


def _weave_diffs_wide_inline(
  comparison: "Comparison",
  diff_cols: Sequence[str],
  suffix: Tuple[str, str],
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
  select_parts = _weave_select_parts(comparison, diff_cols, suffix)
  order_cols = _order_cols(comparison.by_columns)
  out = (
    joined.filter(predicate)
    .select(*select_parts)
    .orderBy(*order_cols)
  )
  return out


def _weave_diffs_wide_with_keys(
  comparison: "Comparison",
  diff_cols: Sequence[str],
  suffix: Tuple[str, str],
  keys: DataFrame,
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
  select_parts = _weave_select_parts(
    comparison,
    diff_cols,
    suffix,
    by_alias="keys",
  )
  order_cols = _order_cols(comparison.by_columns)
  out = joined.select(*select_parts).orderBy(*order_cols)
  return out


def _weave_select_parts(
  comparison: "Comparison",
  diff_cols: Sequence[str],
  suffix: Tuple[str, str],
  *,
  by_alias: str = "a",
) -> list[Column]:
  diff_set = set(diff_cols)
  key_handle = comparison._handles[comparison.table_id[0]]

  def common_column_parts(column: str) -> list[Column]:
    if column in diff_set:
      out = [
        F.col(f"a.{column}").alias(f"{column}{suffix[0]}"),
        F.col(f"b.{column}").alias(f"{column}{suffix[1]}"),
      ]
      return out
    out = [F.col(f"a.{column}").alias(column)]
    return out

  if by_alias == "keys":
    by_parts = [
      F.col(f"{by_alias}.{column}").alias(column)
      for column in comparison.by_columns
    ]
  else:
    by_parts = [
      _core._cast_to_type(
        F.col(f"{by_alias}.{column}"),
        key_handle.data_types[column],
        comparison._resolved_types[column],
      ).alias(column)
      for column in comparison.by_columns
    ]
  value_parts = [
    part
    for column in comparison.common_columns
    for part in common_column_parts(column)
  ]
  out = by_parts + value_parts
  return out


def _weave_diffs_long_inline(
  comparison: "Comparison",
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
  filtered = joined.filter(predicate)
  row_columns = comparison.by_columns + comparison.common_columns
  frames = [
    _weave_long_frame(
      comparison,
      filtered,
      table_name,
      index,
      "a" if table_name == first else "b",
      row_columns,
    )
    for index, table_name in enumerate(comparison.table_id)
  ]
  order_cols = [*_order_cols(comparison.by_columns), F.col("__table_order")]
  out = _core._union_all(frames)
  out = out.orderBy(*order_cols).drop("__table_order")
  return out


def _weave_diffs_long_with_keys(
  comparison: "Comparison",
  keys: DataFrame,
) -> DataFrame:
  frames = [
    l._fetch_rows_by_keys_for_union(
      comparison,
      table_name,
      keys,
      index,
      comparison.by_columns + comparison.common_columns,
    )
    for index, table_name in enumerate(comparison.table_id)
  ]
  order_cols = [*_order_cols(comparison.by_columns), F.col("__table_order")]
  out = _core._union_all(frames)
  out = out.orderBy(*order_cols).drop("__table_order")
  return out


def _weave_long_frame(
  comparison: "Comparison",
  filtered: DataFrame,
  table_name: str,
  table_order: int,
  alias: str,
  columns: Sequence[str],
) -> DataFrame:
  select_parts = [
    F.lit(table_order).alias("__table_order"),
    F.lit(table_name).alias("table_name"),
    *_project_table_columns(comparison, table_name, alias, columns),
  ]
  out = filtered.select(*select_parts)
  return out


def _project_table_columns(
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


def _order_cols(columns: Sequence[str]) -> list[Column]:
  out = [F.col(column) for column in columns]
  return out


__all__ = [
  "resolve_suffix",
  "weave_diffs_long",
  "weave_diffs_wide",
]
