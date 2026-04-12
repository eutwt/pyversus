"""Tools for comparing tabular data with DuckDB or PySpark."""

from . import examples
from ._base import Comparison
from .comparison import ComparisonError, compare

__all__ = [
    "Comparison",
    "ComparisonError",
    "compare",
    "examples",
]
