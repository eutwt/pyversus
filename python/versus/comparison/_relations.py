from __future__ import annotations

from typing import Dict, Union

import duckdb

from ._summary import SummaryRelation
from ._types import _TableHandle


def relation_is_empty(
    relation: Union[duckdb.DuckDBPyRelation, SummaryRelation],
) -> bool:
    return relation.limit(1).fetchone() is None


def diff_lookup_from_intersection(
    relation: duckdb.DuckDBPyRelation,
) -> Dict[str, int]:
    rows = relation.fetchall()
    return {row[0]: int(row[1]) for row in rows}


def unmatched_lookup_from_rows(
    relation: duckdb.DuckDBPyRelation,
) -> Dict[str, int]:
    rows = relation.fetchall()
    return {row[0]: int(row[1]) for row in rows}


def table_count(relation: Union[duckdb.DuckDBPyRelation, _TableHandle]) -> int:
    if isinstance(relation, _TableHandle):
        return relation.row_count
    row = relation.count("*").fetchall()[0]
    assert isinstance(row[0], int)
    return row[0]
