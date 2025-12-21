"""DuckDB-powered tools for comparing two relations."""

from .comparison import Comparison, ComparisonError, compare
from . import examples

__all__ = [
    "Comparison",
    "ComparisonError",
    "compare",
    "examples",
]
