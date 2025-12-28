from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

try:
    from typing import Literal
except ImportError:  # pragma: no cover - Python < 3.8
    from typing_extensions import Literal

import duckdb

from . import _frames as f
from . import _inputs as i
from . import _validation as v
from .comparison import Comparison

if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import polars


def compare(
    table_a: Union[duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"],
    table_b: Union[duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"],
    *,
    by: Sequence[str],
    allow_both_na: bool = True,
    coerce: bool = True,
    table_id: Tuple[str, str] = ("a", "b"),
    connection: Optional[duckdb.DuckDBPyConnection] = None,
    materialize: Literal["all", "summary", "none"] = "all",
) -> Comparison:
    """Compare two DuckDB relations by key columns.

    Parameters
    ----------
    table_a, table_b : DuckDBPyRelation, pandas.DataFrame, or polars.DataFrame
        DuckDB relations or pandas/polars DataFrames to compare.
    by : sequence of str
        Column names that uniquely identify rows.
    allow_both_na : bool, default True
        Whether to treat NULL/NA values as equal when both sides are missing.
    coerce : bool, default True
        If True, allow DuckDB to coerce compatible types. If False, require
        exact type matches for shared columns.
    table_id : tuple[str, str], default ("a", "b")
        Labels used in outputs for the two tables.
    connection : duckdb.DuckDBPyConnection, optional
        DuckDB connection used to register the inputs and run queries.
    materialize : {"all", "summary", "none"}, default "all"
        Controls which helper tables are materialized upfront.

    Returns
    -------
    Comparison
        Comparison object with summary relations and diff helpers.

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
    materialize_summary, materialize_keys = v.resolve_materialize(materialize)

    conn = v.resolve_connection(connection)
    clean_ids = v.validate_table_id(table_id)
    by_columns = v.normalize_column_list(by, "by", allow_empty=False)
    connection_supplied = connection is not None
    handles = {
        clean_ids[0]: i.build_table_handle(
            conn, table_a, clean_ids[0], connection_supplied=connection_supplied
        ),
        clean_ids[1]: i.build_table_handle(
            conn, table_b, clean_ids[1], connection_supplied=connection_supplied
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
