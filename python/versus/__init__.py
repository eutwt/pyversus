"""DuckDB-powered tools for comparing two relations."""

from . import examples
from .comparison import Comparison, ComparisonError, compare

__all__ = [
    "Comparison",
    "ComparisonError",
    "compare",
    "examples",
]
