from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple


class Comparison:
    """Shared interface for backend-specific comparison objects.

    `versus.compare()` returns concrete subclasses of this type. DuckDB
    comparisons expose DuckDB relations; Spark comparisons expose Spark
    DataFrames. The helper method names stay the same across backends.
    """

    tables: Any
    by: Any
    intersection: Any
    unmatched_cols: Any
    unmatched_keys: Any
    unmatched_rows: Any
    inputs: Any
    table_id: Tuple[str, str]
    by_columns: List[str]
    common_columns: List[str]
    table_columns: Any
    allow_both_na: bool

    def value_diffs(self, column: str) -> Any:
        """Return rows where one column differs between the inputs."""
        raise NotImplementedError

    def value_diffs_stacked(self, columns: Optional[Sequence[str]] = None) -> Any:
        """Return a stacked view of value differences for multiple columns."""
        raise NotImplementedError

    def weave_diffs_wide(
        self,
        columns: Optional[Sequence[str]] = None,
        suffix: Optional[Tuple[str, str]] = None,
    ) -> Any:
        """Return a wide view of differing rows with split columns."""
        raise NotImplementedError

    def weave_diffs_long(self, columns: Optional[Sequence[str]] = None) -> Any:
        """Return a long view of differing rows stacked by input."""
        raise NotImplementedError

    def slice_diffs(
        self,
        table: str,
        columns: Optional[Sequence[str]] = None,
    ) -> Any:
        """Return rows from one input that differ in the selected columns."""
        raise NotImplementedError

    def slice_unmatched(self, table: str) -> Any:
        """Return rows from one input whose keys are missing in the other."""
        raise NotImplementedError

    def slice_unmatched_both(self) -> Any:
        """Return unmatched rows from both inputs."""
        raise NotImplementedError

    def summary(self) -> Any:
        """Summarize which difference categories are present."""
        raise NotImplementedError

    def close(self) -> None:
        """Release backend-specific cached state for the comparison."""
        raise NotImplementedError
