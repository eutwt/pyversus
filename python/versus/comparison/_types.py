from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import duckdb

if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import polars

try:
    from typing import TypeAlias
except ImportError:  # pragma: no cover - Python < 3.10
    from typing_extensions import TypeAlias

_Input: TypeAlias = Union[
    duckdb.DuckDBPyRelation, "pandas.DataFrame", "polars.DataFrame"
]


@dataclass
class _TableHandle:
    name: str
    display: str
    relation: duckdb.DuckDBPyRelation
    columns: List[str]
    types: Dict[str, str]
    source_sql: str
    source_is_identifier: bool
    row_count: int

    def __getattr__(self, name: str) -> Any:
        return getattr(self.relation, name)


@dataclass
class VersusState:
    temp_tables: List[str]
    views: List[str]


class VersusConn:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        temp_tables: Optional[List[str]] = None,
        views: Optional[List[str]] = None,
    ) -> None:
        self.raw_connection = connection
        self.versus = VersusState(
            temp_tables if temp_tables is not None else [],
            views if views is not None else [],
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw_connection, name)
