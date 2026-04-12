from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Tuple, Union, cast

try:
    from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
    from typing_extensions import Literal

import duckdb

from .._base import Comparison as BaseComparison
from . import _frames as f
from . import _inputs as i
from . import _validation as v
from ._exceptions import ComparisonError
from .comparison import Comparison

if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import polars
    from pyspark.sql import DataFrame, SparkSession


DuckInput = Union[duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"]


def compare(
    table_a: Union[DuckInput, "DataFrame"],
    table_b: Union[DuckInput, "DataFrame"],
    *,
    by: Sequence[str],
    allow_both_na: bool = True,
    coerce: bool = True,
    table_id: Tuple[str, str] = ("a", "b"),
    con: Optional[duckdb.DuckDBPyConnection] = None,
    spark: Optional["SparkSession"] = None,
    materialize: Literal["all", "summary", "none"] = "all",
) -> BaseComparison:
    """Compare two tables by key columns using DuckDB or PySpark.

    Parameters
    ----------
    table_a, table_b : DuckDBPyRelation, pandas.DataFrame, polars.DataFrame, or pyspark.sql.DataFrame
        Tabular inputs to compare. If either input is a PySpark DataFrame, or
        if `spark=` is supplied, the comparison runs on Spark and returns
        Spark-backed outputs. Otherwise DuckDB powers the comparison.
    by : sequence of str
        Column names that uniquely identify rows.
    allow_both_na : bool, default True
        Whether to treat NULL/NA values as equal when both sides are missing.
    coerce : bool, default True
        If True, allow DuckDB to coerce compatible types. If False, require
        exact type matches for shared columns.
    table_id : tuple[str, str], default ("a", "b")
        Labels used in outputs for the two tables.
    con : duckdb.DuckDBPyConnection, optional
        DuckDB connection used to register non-Spark inputs and run queries.
    spark : pyspark.sql.SparkSession, optional
        Spark session used when the comparison runs on Spark.
    materialize : {"all", "summary", "none"}, default "all"
        Controls which helper tables are materialized upfront.

    Returns
    -------
    Comparison
        Backend-specific comparison object with summary tables and diff
        helpers.

    Examples
    --------
    >>> from versus import compare, examples
    >>> comparison = compare(
    ...     examples.example_cars_a(),
    ...     examples.example_cars_b(),
    ...     by=["car"],
    ... )
    >>> comparison.summary()
    ┌────────────────┬─────────┐
    │   difference   │  found  │
    │    varchar     │ boolean │
    ├────────────────┼─────────┤
    │ value_diffs    │ true    │
    │ unmatched_cols │ true    │
    │ unmatched_rows │ true    │
    │ type_diffs     │ false   │
    └────────────────┴─────────┘
    """
    if _should_use_spark_backend(table_a, table_b, spark):
        if con is not None:
            raise ComparisonError(
                "`con` is only supported for DuckDB-backed comparisons. "
                "Omit `con` when Spark inputs are involved or when `spark=` "
                "is provided."
            )
        return _compare_spark(
            table_a,
            table_b,
            by=by,
            allow_both_na=allow_both_na,
            coerce=coerce,
            table_id=table_id,
            spark=spark,
            materialize=materialize,
        )
    return _compare_duckdb(
        cast(DuckInput, table_a),
        cast(DuckInput, table_b),
        by=by,
        allow_both_na=allow_both_na,
        coerce=coerce,
        table_id=table_id,
        con=con,
        materialize=materialize,
    )


def _compare_duckdb(
    table_a: DuckInput,
    table_b: DuckInput,
    *,
    by: Sequence[str],
    allow_both_na: bool,
    coerce: bool,
    table_id: Tuple[str, str],
    con: Optional[duckdb.DuckDBPyConnection],
    materialize: Literal["all", "summary", "none"],
) -> Comparison:
    materialize_summary, materialize_keys = v.resolve_materialize(materialize)

    conn = v.resolve_connection(con)
    clean_ids = v.validate_table_id(table_id)
    by_columns = v.normalize_column_list(by, "by", allow_empty=False)
    con_supplied = con is not None
    handles = {
        clean_ids[0]: i.build_table_handle(
            conn, table_a, clean_ids[0], connection_supplied=con_supplied
        ),
        clean_ids[1]: i.build_table_handle(
            conn, table_b, clean_ids[1], connection_supplied=con_supplied
        ),
    }
    v.validate_tables(conn, handles, clean_ids, by_columns, coerce=coerce)

    tables_frame = f.build_tables_frame(conn, handles, clean_ids, materialize_summary)
    by_frame = f.build_by_frame(
        conn, by_columns, handles, clean_ids, materialize_summary
    )
    common_all = [
        col
        for col in handles[clean_ids[0]].columns
        if col in handles[clean_ids[1]].columns
    ]
    value_columns = [col for col in common_all if col not in by_columns]
    unmatched_cols = f.build_unmatched_cols(
        conn, handles, clean_ids, materialize_summary
    )
    diff_table = None
    if materialize_keys:
        diff_table = f.compute_diff_table(
            conn,
            handles,
            clean_ids,
            by_columns,
            value_columns,
            allow_both_na,
        )
    intersection, diff_lookup = f.build_intersection_frame(
        value_columns,
        handles,
        clean_ids,
        by_columns,
        allow_both_na,
        diff_table,
        conn,
        materialize_summary,
    )
    unmatched_keys = f.compute_unmatched_keys(
        conn, handles, clean_ids, by_columns, materialize_keys
    )
    unmatched_rows_rel, unmatched_lookup = f.compute_unmatched_rows_summary(
        conn, unmatched_keys, clean_ids, materialize_summary
    )

    return Comparison(
        connection=conn,
        handles=handles,
        table_id=clean_ids,
        by_columns=by_columns,
        allow_both_na=allow_both_na,
        materialize_mode=materialize,
        tables=tables_frame,
        by=by_frame,
        intersection=intersection,
        unmatched_cols=unmatched_cols,
        unmatched_keys=unmatched_keys,
        unmatched_rows=unmatched_rows_rel,
        common_columns=value_columns,
        table_columns={
            identifier: handle.columns[:] for identifier, handle in handles.items()
        },
        diff_table=diff_table,
        diff_lookup=diff_lookup,
        unmatched_lookup=unmatched_lookup,
    )


def _compare_spark(
    table_a: Union[
        duckdb.DuckDBPyRelation,
        "pandas.DataFrame",
        "polars.DataFrame",
        "DataFrame",
    ],
    table_b: Union[
        duckdb.DuckDBPyRelation,
        "pandas.DataFrame",
        "polars.DataFrame",
        "DataFrame",
    ],
    *,
    by: Sequence[str],
    allow_both_na: bool,
    coerce: bool,
    table_id: Tuple[str, str],
    spark: Optional["SparkSession"],
    materialize: Literal["all", "summary", "none"],
) -> BaseComparison:
    from .._spark.comparison.api import compare as compare_spark

    spark_session = _resolve_spark_session(table_a, table_b, spark)
    spark_table_a = _to_spark_dataframe(table_a, spark_session, "a")
    spark_table_b = _to_spark_dataframe(table_b, spark_session, "b")
    return compare_spark(
        spark_table_a,
        spark_table_b,
        by=by,
        allow_both_na=allow_both_na,
        coerce=coerce,
        table_id=table_id,
        spark=spark_session,
        materialize=materialize,
    )


def _should_use_spark_backend(
    table_a: object,
    table_b: object,
    spark: Optional["SparkSession"],
) -> bool:
    return spark is not None or _is_spark_dataframe(table_a) or _is_spark_dataframe(
        table_b
    )


def _is_spark_dataframe(value: object) -> bool:
    try:
        from pyspark.sql import DataFrame
    except Exception:  # pragma: no cover - dependency missing at runtime
        return False
    return isinstance(value, DataFrame)


def _resolve_spark_session(
    table_a: object,
    table_b: object,
    spark: Optional["SparkSession"],
) -> "SparkSession":
    if spark is not None:
        return spark
    for candidate in (table_a, table_b):
        if _is_spark_dataframe(candidate):
            return cast("DataFrame", candidate).sparkSession
    from .._spark.examples import resolve_spark

    return resolve_spark(None)


def _to_spark_dataframe(
    source: object,
    spark_session: "SparkSession",
    label: str,
) -> "DataFrame":
    if _is_spark_dataframe(source):
        return cast("DataFrame", source)
    if isinstance(source, duckdb.DuckDBPyRelation):
        try:
            source = source.df()
        except Exception as exc:
            raise ComparisonError(
                f"`table_{label}` could not be converted from DuckDB to Spark."
            ) from exc

    module = type(source).__module__
    if module.startswith("pandas"):
        return spark_session.createDataFrame(cast("pandas.DataFrame", source))
    if module.startswith("polars"):
        return spark_session.createDataFrame(
            cast("polars.DataFrame", source).to_pandas()
        )
    raise ComparisonError(
        "Inputs must be DuckDB relations, pandas/polars DataFrames, or "
        "PySpark DataFrames."
    )
