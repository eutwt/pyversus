from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass
from functools import reduce
from typing import (
  Any,
  Callable,
  Dict,
  Iterable,
  List,
  Mapping,
  Optional,
  Sequence,
  Tuple,
)

try:
  from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
  from typing_extensions import Literal

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from versus._base import Comparison as BaseComparison

from .._spark_values import build_values_frame
from ._exceptions import ComparisonError


@dataclass
class _TableHandle:
  label: str
  display: str
  frame: DataFrame
  columns: List[str]
  types: Dict[str, str]
  data_types: Dict[str, T.DataType]
  row_count: int


class SummaryDataFrame:
  def __init__(
    self,
    dataframe: DataFrame,
    *,
    materialized: bool,
    on_materialize: Optional[Callable[[DataFrame], None]] = None,
  ) -> None:
    self.dataframe = dataframe
    self.materialized = False
    self._on_materialize = on_materialize
    if materialized:
      self.materialize()

  def materialize(self) -> None:
    if self.materialized:
      return
    self.dataframe = _cache_frame(self.dataframe)
    self.materialized = True
    if self._on_materialize is not None:
      self._on_materialize(self.dataframe)

  def unpersist(self) -> None:
    if self.materialized:
      self.dataframe.unpersist()

  def __getattr__(self, name: str) -> Any:
    return getattr(self.dataframe, name)

  def __repr__(self) -> str:
    self.materialize()
    return _show_string(self.dataframe)

  def __str__(self) -> str:
    self.materialize()
    return _show_string(self.dataframe)

  def __iter__(self) -> Any:
    return iter(self.dataframe.toLocalIterator())


def compare(
  table_a: DataFrame,
  table_b: DataFrame,
  *,
  by: Sequence[str],
  allow_both_na: bool = True,
  coerce: bool = True,
  table_id: Tuple[str, str] = ("a", "b"),
  spark: Optional[SparkSession] = None,
  materialize: Literal["all", "summary", "none"] = "all",
) -> "Comparison":
  """Compare two PySpark DataFrames by key columns.

  Parameters
  ----------
  table_a, table_b : pyspark.sql.DataFrame
    PySpark DataFrames to compare.
  by : sequence of str
    Column names that uniquely identify rows.
  allow_both_na : bool, default True
    Whether to treat NULL values as equal when both sides are missing.
  coerce : bool, default True
    If True, allow compatible Spark types to be compared after coercion.
    If False, shared columns must already have exactly matching types.
  table_id : tuple[str, str], default ("a", "b")
    Labels used in outputs for the two inputs.
  spark : pyspark.sql.SparkSession, optional
    Spark session used to build helper DataFrames. If omitted, the session
    is resolved from the input DataFrames.
  materialize : {"all", "summary", "none"}, default "all"
    Controls which helper DataFrames are cached up front.

  Returns
  -------
  Comparison
    Comparison object with summary DataFrames and diff helpers.

  Examples
  --------
  >>> from versus._spark import compare, examples
  >>> comparison = compare(
  ...   examples.example_cars_a(),
  ...   examples.example_cars_b(),
  ...   by=["car"],
  ... )
  >>> comparison.summary().show()
  +--------------+-----+
  |    difference|found|
  +--------------+-----+
  |   value_diffs| true|
  |unmatched_cols| true|
  |unmatched_rows| true|
  |    type_diffs|false|
  +--------------+-----+
  """
  from . import _frames as f

  materialize_summary, materialize_keys = _resolve_materialize(materialize)
  clean_ids = validate_table_id(table_id)
  by_columns = normalize_column_list(by, "by", allow_empty=False)
  spark_session = _resolve_spark_session(table_a, table_b, spark)
  handles = {
    clean_ids[0]: _build_table_handle(table_a, clean_ids[0]),
    clean_ids[1]: _build_table_handle(table_b, clean_ids[1]),
  }
  validate_tables(handles, clean_ids, by_columns, coerce=coerce)

  common_all = [
    column
    for column in handles[clean_ids[0]].columns
    if column in handles[clean_ids[1]].columns
  ]
  value_columns = [column for column in common_all if column not in by_columns]
  resolved_types = {
    column: _resolve_output_type(
      handles[clean_ids[0]].data_types[column],
      handles[clean_ids[1]].data_types[column],
    )
    for column in common_all
  }

  diff_table = None
  if materialize_keys:
    diff_table = _cache_frame(
      f.compute_diff_table(
        handles,
        clean_ids,
        by_columns,
        value_columns,
        allow_both_na,
        resolved_types,
      )
    )

  tables_frame = f.build_tables_frame(spark_session, handles, clean_ids)
  by_frame = f.build_by_frame(spark_session, by_columns, handles, clean_ids)
  unmatched_cols = f.build_unmatched_cols(spark_session, handles, clean_ids)
  intersection_frame, diff_lookup = f.build_intersection_frame(
    spark_session,
    value_columns,
    handles,
    clean_ids,
    by_columns,
    allow_both_na,
    diff_table,
    materialize_summary,
  )
  unmatched_keys = f.compute_unmatched_keys(
    handles,
    clean_ids,
    by_columns,
    resolved_types,
  )
  if materialize_keys:
    unmatched_keys = _cache_frame(unmatched_keys)
  unmatched_rows, unmatched_lookup = f.compute_unmatched_rows_summary(
    spark_session,
    unmatched_keys,
    clean_ids,
    materialize_summary,
  )

  out = Comparison(
    spark=spark_session,
    handles=handles,
    table_id=clean_ids,
    by_columns=by_columns,
    allow_both_na=allow_both_na,
    materialize_mode=materialize,
    tables=tables_frame,
    by=by_frame,
    intersection=intersection_frame,
    unmatched_cols=unmatched_cols,
    unmatched_keys=unmatched_keys,
    unmatched_rows=unmatched_rows,
    common_columns=value_columns,
    table_columns={
      identifier: handle.columns[:] for identifier, handle in handles.items()
    },
    diff_table=diff_table,
    diff_lookup=diff_lookup,
    unmatched_lookup=unmatched_lookup,
    resolved_types=resolved_types,
  )
  return out


class Comparison(BaseComparison):
  """In-memory description of how two PySpark DataFrames differ.

  Provides summary DataFrames plus helper methods to retrieve the exact
  differences without collecting the full inputs back to Python.
  """

  def __init__(
    self,
    *,
    spark: SparkSession,
    handles: Mapping[str, _TableHandle],
    table_id: Tuple[str, str],
    by_columns: List[str],
    allow_both_na: bool,
    materialize_mode: str,
    tables: DataFrame,
    by: DataFrame,
    intersection: DataFrame,
    unmatched_cols: DataFrame,
    unmatched_keys: DataFrame,
    unmatched_rows: DataFrame,
    common_columns: List[str],
    table_columns: Mapping[str, List[str]],
    diff_table: Optional[DataFrame],
    diff_lookup: Optional[Dict[str, int]],
    unmatched_lookup: Optional[Dict[str, int]],
    resolved_types: Mapping[str, T.DataType],
  ) -> None:
    self.spark = spark
    self._handles = dict(handles)
    self.inputs = {
      identifier: handle.frame for identifier, handle in self._handles.items()
    }
    self.table_id = table_id
    self.by_columns = by_columns
    self.allow_both_na = allow_both_na
    self._materialize_mode = materialize_mode
    self._diff_lookup = diff_lookup
    self._unmatched_lookup = unmatched_lookup
    self._resolved_types = dict(resolved_types)
    summary_materialized = materialize_mode in {"all", "summary"}
    self.tables = SummaryDataFrame(tables, materialized=summary_materialized)
    self.by = SummaryDataFrame(by, materialized=summary_materialized)
    self.intersection = SummaryDataFrame(
      intersection,
      materialized=summary_materialized,
      on_materialize=self._store_diff_lookup,
    )
    self.unmatched_cols = SummaryDataFrame(
      unmatched_cols,
      materialized=summary_materialized,
    )
    self.unmatched_keys = unmatched_keys
    self.unmatched_rows = SummaryDataFrame(
      unmatched_rows,
      materialized=summary_materialized,
      on_materialize=self._store_unmatched_lookup,
    )
    self.common_columns = common_columns
    self.table_columns = table_columns
    self.diff_table = diff_table
    self._closed = False

  def _filter_diff_columns(self, columns: Sequence[str]) -> List[str]:
    if self._diff_lookup is None:
      return list(columns)
    out = [column for column in columns if self._diff_lookup.get(column, 0) > 0]
    return out

  def _store_diff_lookup(self, frame: DataFrame) -> None:
    if self._diff_lookup is None:
      self._diff_lookup = _lookup_from_rows(frame, "column", "n_diffs")

  def _store_unmatched_lookup(self, frame: DataFrame) -> None:
    if self._unmatched_lookup is None:
      self._unmatched_lookup = _lookup_from_rows(
        frame, "table_name", "n_unmatched"
      )

  def close(self) -> None:
    """Release cached helper DataFrames created for the comparison.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.close()
    """
    if self._closed:
      return
    self.tables.unpersist()
    self.by.unpersist()
    self.intersection.unpersist()
    self.unmatched_cols.unpersist()
    self.unmatched_rows.unpersist()
    if self.diff_table is not None:
      self.diff_table.unpersist()
    self.unmatched_keys.unpersist()
    self._closed = True

  def __del__(self) -> None:  # pragma: no cover
    try:
      self.close()
    except Exception:
      pass

  def __repr__(self) -> str:
    out = (
      "Comparison("
      f"tables=\n{self.tables}\n"
      f"by=\n{self.by}\n"
      f"intersection=\n{self.intersection}\n"
      f"unmatched_cols=\n{self.unmatched_cols}\n"
      f"unmatched_rows=\n{self.unmatched_rows}\n"
      ")"
    )
    return out


def _resolve_materialize(materialize: str) -> Tuple[bool, bool]:
  if materialize not in {"all", "summary", "none"}:
    raise ComparisonError(
      "`materialize` must be one of: 'all', 'summary', 'none'"
    )
  return materialize in {"all", "summary"}, materialize == "all"


def _resolve_spark_session(
  table_a: DataFrame,
  table_b: DataFrame,
  spark: Optional[SparkSession],
) -> SparkSession:
  if not isinstance(table_a, DataFrame) or not isinstance(table_b, DataFrame):
    raise ComparisonError("Inputs must be PySpark DataFrames.")
  session_a = table_a.sparkSession
  session_b = table_b.sparkSession
  if _session_id(session_a) != _session_id(session_b):
    raise ComparisonError(
      "`table_a` and `table_b` must belong to the same Spark session."
    )
  if spark is None:
    return session_a
  if _session_id(session_a) != _session_id(spark):
    raise ComparisonError(
      "`spark` must be the Spark session that created the input DataFrames."
    )
  return spark


def _session_id(spark: SparkSession) -> str:
  jsession = getattr(spark, "_jsparkSession", None)
  if jsession is not None:
    try:
      return str(jsession)
    except Exception:
      pass
  return spark.sparkContext.applicationId


def _build_table_handle(frame: DataFrame, label: str) -> _TableHandle:
  columns = list(frame.columns)
  validate_columns(columns, label)
  data_types = {field.name: field.dataType for field in frame.schema.fields}
  types = {
    field.name: field.dataType.simpleString() for field in frame.schema.fields
  }
  out = _TableHandle(
    label=label,
    display=type(frame).__name__,
    frame=frame,
    columns=columns,
    types=types,
    data_types=data_types,
    row_count=frame.count(),
  )
  return out


def validate_columns(columns: Sequence[str], label: str) -> None:
  if not all(isinstance(column, str) for column in columns):
    raise ComparisonError(f"`{label}` must have string column names")
  duplicates = [name for name, count in Counter(columns).items() if count > 1]
  if duplicates:
    duplicate_names = ", ".join(duplicates)
    raise ComparisonError(
      f"`{label}` has duplicate column names: {duplicate_names}"
    )


def validate_table_id(table_id: Tuple[str, str]) -> Tuple[str, str]:
  if (
    not isinstance(table_id, (tuple, list))
    or len(table_id) != 2
    or not all(isinstance(item, str) for item in table_id)
  ):
    raise ComparisonError("`table_id` must be a tuple of two strings")
  first, second = table_id[0], table_id[1]
  if not first.strip() or not second.strip():
    raise ComparisonError("Entries of `table_id` must be non-empty strings")
  if first == second:
    raise ComparisonError("Entries of `table_id` must be distinct")
  return (first, second)


def normalize_column_list(
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
      raise ComparisonError(
        f"`{arg_name}` must be a sequence of column names"
      ) from exc
  if not parsed and not allow_empty:
    raise ComparisonError(f"`{arg_name}` must contain at least one column")
  if not all(isinstance(item, str) for item in parsed):
    raise ComparisonError(f"`{arg_name}` must only contain strings")
  return parsed


def normalize_table_arg(comparison: Comparison, table: str) -> str:
  if table not in comparison.table_id:
    names = ", ".join(comparison.table_id)
    raise ComparisonError(f"`table` must be one of: {names}")
  return table


def normalize_single_column(column: str) -> str:
  if isinstance(column, str):
    return column
  raise ComparisonError("`column` must be a column name")


def resolve_column_list(
  comparison: Comparison,
  columns: Optional[Sequence[str]],
  *,
  allow_empty: bool = True,
) -> List[str]:
  if columns is None:
    parsed = comparison.common_columns[:]
  else:
    parsed = normalize_column_list(columns, "column", allow_empty=True)
    if not parsed:
      raise ComparisonError("`columns` must select at least one column")
    missing = [
      column for column in parsed if column not in comparison.common_columns
    ]
    if missing:
      names = ", ".join(missing)
      raise ComparisonError(f"Columns not part of the comparison: {names}")
  if not parsed and not allow_empty:
    raise ComparisonError("`columns` must select at least one column")
  return parsed


def assert_column_allowed(
  comparison: Comparison, column: str, func: str
) -> None:
  if column not in comparison.common_columns:
    raise ComparisonError(
      f"`{func}` can only reference columns in both tables: {column}"
    )


def validate_tables(
  handles: Mapping[str, _TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
  *,
  coerce: bool,
) -> None:
  _validate_columns_exist(by_columns, handles, table_id)
  for identifier in table_id:
    validate_columns(handles[identifier].columns, identifier)
  if not coerce:
    _validate_type_compatibility(handles, table_id)
  for identifier in table_id:
    _assert_unique_by(handles[identifier], by_columns, identifier)


def _validate_columns_exist(
  by_columns: Iterable[str],
  handles: Mapping[str, _TableHandle],
  table_id: Tuple[str, str],
) -> None:
  missing_a = [
    column
    for column in by_columns
    if column not in handles[table_id[0]].columns
  ]
  missing_b = [
    column
    for column in by_columns
    if column not in handles[table_id[1]].columns
  ]
  if missing_a:
    names = ", ".join(missing_a)
    raise ComparisonError(f"`by` columns not found in `{table_id[0]}`: {names}")
  if missing_b:
    names = ", ".join(missing_b)
    raise ComparisonError(f"`by` columns not found in `{table_id[1]}`: {names}")


def _validate_type_compatibility(
  handles: Mapping[str, _TableHandle],
  table_id: Tuple[str, str],
) -> None:
  shared = set(handles[table_id[0]].columns) & set(handles[table_id[1]].columns)
  for column in shared:
    type_a = handles[table_id[0]].types[column]
    type_b = handles[table_id[1]].types[column]
    if type_a != type_b:
      raise ComparisonError(
        "`coerce=False` requires compatible types. "
        f"Column `{column}` has types `{type_a}` vs `{type_b}`."
      )


def _assert_unique_by(
  handle: _TableHandle,
  by_columns: List[str],
  identifier: str,
) -> None:
  view_name = f"__versus_by_check_{uuid.uuid4().hex}"
  select_list = ", ".join(_quote_identifier(column) for column in by_columns)
  sql = (
    f"SELECT {select_list} "
    f"FROM {_quote_identifier(view_name)} "
    f"GROUP BY {select_list} "
    "HAVING COUNT(*) > 1 "
    "LIMIT 1"
  )
  handle.frame.createOrReplaceTempView(view_name)
  try:
    duplicates = handle.frame.sparkSession.sql(sql).collect()
  finally:
    handle.frame.sparkSession.catalog.dropTempView(view_name)
  if not duplicates:
    return
  first = duplicates[0].asDict(recursive=True)
  values = ", ".join(f"{column}={first[column]!r}" for column in by_columns)
  raise ComparisonError(
    f"`{identifier}` has more than one row for by values ({values})"
  )


def _join_inputs(
  handles: Mapping[str, _TableHandle],
  table_id: Tuple[str, str],
  by_columns: List[str],
) -> DataFrame:
  first, second = table_id
  left = handles[first].frame.alias("a")
  right = handles[second].frame.alias("b")
  join_expr = _join_condition(
    "a",
    "b",
    by_columns,
    handles[first].data_types,
    handles[second].data_types,
  )
  return left.join(right, join_expr, "inner")


def _collect_diff_keys(
  comparison: Comparison, columns: Sequence[str]
) -> DataFrame:
  if comparison.diff_table is None:
    raise ComparisonError("Diff table is only available for materialize='all'.")
  predicate = _combine_or([F.col(column) for column in columns])
  return comparison.diff_table.filter(predicate).select(*comparison.by_columns)


def _rows_frame(
  spark: SparkSession,
  rows: Sequence[Sequence[Any]],
  fields: Sequence[Tuple[str, T.DataType]],
) -> DataFrame:
  schema = T.StructType(
    [T.StructField(name, data_type, True) for name, data_type in fields]
  )
  return build_values_frame(spark, rows, schema)


def _lookup_from_rows(
  dataframe: DataFrame,
  key_col: str,
  value_col: str,
) -> Dict[str, int]:
  return {row[key_col]: int(row[value_col]) for row in dataframe.collect()}


def _quote_identifier(name: str) -> str:
  escaped = name.replace("`", "``")
  return f"`{escaped}`"


def _show_string(dataframe: DataFrame) -> str:
  try:
    return dataframe._jdf.showString(20, 20, False)  # type: ignore[attr-defined]
  except Exception:
    return repr(dataframe)


def _cache_frame(dataframe: DataFrame) -> DataFrame:
  dataframe.cache()
  dataframe.count()
  return dataframe


def _frame_data_types(dataframe: DataFrame) -> Dict[str, T.DataType]:
  return {field.name: field.dataType for field in dataframe.schema.fields}


def _combine_or(expressions: Sequence[Column]) -> Column:
  if not expressions:
    return F.lit(False)
  out = reduce(
    lambda left, right: left | right, expressions[1:], expressions[0]
  )
  return out


def _combine_and(expressions: Sequence[Column]) -> Column:
  if not expressions:
    return F.lit(True)
  out = reduce(
    lambda left, right: left & right, expressions[1:], expressions[0]
  )
  return out


def _join_condition(
  left_alias: str,
  right_alias: str,
  columns: Sequence[str],
  left_types: Mapping[str, T.DataType],
  right_types: Mapping[str, T.DataType],
) -> Column:
  out = _combine_and(
    [
      _nullsafe_equal(
        F.col(f"{left_alias}.{column}"),
        F.col(f"{right_alias}.{column}"),
        left_types[column],
        right_types[column],
      )
      for column in columns
    ]
  )
  return out


def _nullsafe_equal(
  left: Column,
  right: Column,
  left_type: T.DataType,
  right_type: T.DataType,
) -> Column:
  left_cmp, right_cmp = _comparison_columns(left, right, left_type, right_type)
  return left_cmp.eqNullSafe(right_cmp)


def _diff_expression(
  column: str,
  allow_both_na: bool,
  left_alias: str,
  right_alias: str,
  left_types: Mapping[str, T.DataType],
  right_types: Mapping[str, T.DataType],
) -> Column:
  left_cmp, right_cmp = _comparison_columns(
    F.col(f"{left_alias}.{column}"),
    F.col(f"{right_alias}.{column}"),
    left_types[column],
    right_types[column],
  )
  distinct = ~left_cmp.eqNullSafe(right_cmp)
  if allow_both_na:
    return distinct
  left_is_null = left_cmp.isNull()
  right_is_null = right_cmp.isNull()
  return (left_is_null & right_is_null) | distinct


def _comparison_columns(
  left: Column,
  right: Column,
  left_type: T.DataType,
  right_type: T.DataType,
) -> Tuple[Column, Column]:
  if _same_type(left_type, right_type):
    return left, right
  if _is_numeric(left_type) and _is_numeric(right_type):
    return left.cast("double"), right.cast("double")
  return left.cast("string"), right.cast("string")


def _cast_to_type(
  column: Column,
  source_type: T.DataType,
  target_type: T.DataType,
) -> Column:
  if _same_type(source_type, target_type):
    return column
  return column.cast(target_type.simpleString())


def _resolve_output_type(
  left_type: T.DataType,
  right_type: T.DataType,
) -> T.DataType:
  if _same_type(left_type, right_type):
    return left_type
  if _is_numeric(left_type) and _is_numeric(right_type):
    return T.DoubleType()
  return T.StringType()


def _same_type(left_type: T.DataType, right_type: T.DataType) -> bool:
  return left_type.simpleString() == right_type.simpleString()


def _is_numeric(data_type: T.DataType) -> bool:
  out = isinstance(
    data_type,
    (
      T.ByteType,
      T.ShortType,
      T.IntegerType,
      T.LongType,
      T.FloatType,
      T.DoubleType,
      T.DecimalType,
    ),
  )
  return out


def _union_all(frames: Sequence[DataFrame]) -> DataFrame:
  if not frames:
    raise ComparisonError("At least one DataFrame is required")
  out = reduce(
    lambda left, right: left.unionByName(right), frames[1:], frames[0]
  )
  return out
