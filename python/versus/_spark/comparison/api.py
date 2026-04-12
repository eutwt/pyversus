from __future__ import annotations

from typing import Optional, Sequence, Tuple

try:
  from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
  from typing_extensions import Literal

from pyspark.sql import DataFrame, SparkSession

from versus._spark.comparison import _core

from . import _frames as f
from . import _inputs as i
from . import _validation as v
from .comparison import Comparison


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
) -> Comparison:
  materialize_summary, materialize_keys = v.resolve_materialize(materialize)
  clean_ids = v.validate_table_id(table_id)
  by_columns = v.normalize_column_list(by, "by", allow_empty=False)
  spark_session = v.resolve_connection(table_a, table_b, spark)
  handles = {
    clean_ids[0]: i.build_table_handle(
      spark_session,
      table_a,
      clean_ids[0],
      connection_supplied=spark is not None,
    ),
    clean_ids[1]: i.build_table_handle(
      spark_session,
      table_b,
      clean_ids[1],
      connection_supplied=spark is not None,
    ),
  }
  v.validate_tables(handles, clean_ids, by_columns, coerce=coerce)

  common_all = [
    column
    for column in handles[clean_ids[0]].columns
    if column in handles[clean_ids[1]].columns
  ]
  value_columns = [column for column in common_all if column not in by_columns]
  resolved_types = {
    column: _core._resolve_output_type(
      handles[clean_ids[0]].data_types[column],
      handles[clean_ids[1]].data_types[column],
    )
    for column in common_all
  }

  diff_table = None
  if materialize_keys:
    diff_table = _core._cache_frame(
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
    unmatched_keys = _core._cache_frame(unmatched_keys)
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


compare.__doc__ = _core.compare.__doc__

__all__ = ["compare"]
