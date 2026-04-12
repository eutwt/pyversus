from __future__ import annotations

from typing import Optional, Sequence, Tuple

from pyspark.sql import DataFrame

from versus._spark.comparison._core import Comparison as _CoreComparison

from . import _slices as l
from . import _summary as s
from . import _value_diffs as d
from . import _weave as w


class Comparison(_CoreComparison):
  """In-memory description of how two PySpark DataFrames differ.

  Provides summary DataFrames plus helper methods to retrieve the exact
  differences without collecting the full inputs back to Python.
  """

  def value_diffs(self, column: str) -> DataFrame:
    """Return rows where a single column differs between the inputs.

    Parameters
    ----------
    column : str
      Column name to compare.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with the differing values plus the `by` columns.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.value_diffs("disp").show()
    +------+------+--------------+
    |disp_a|disp_b|           car|
    +------+------+--------------+
    |   109|   108|    Datsun 710|
    |   259|   258|Hornet 4 Drive|
    +------+------+--------------+
    """
    return d.value_diffs(self, column)

  def value_diffs_stacked(
    self,
    columns: Optional[Sequence[str]] = None,
  ) -> DataFrame:
    """Return a stacked view of value differences for multiple columns.

    Parameters
    ----------
    columns : sequence of str, optional
      Columns to compare. Defaults to all comparable columns.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with `column`, `val_<table_id>`, and `by` columns.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.value_diffs_stacked(["mpg", "disp"]).show()
    +------+-----+-----+--------------+
    |column|val_a|val_b|           car|
    +------+-----+-----+--------------+
    |   mpg| 14.3| 16.3|    Duster 360|
    |   mpg| 24.4| 26.4|     Merc 240D|
    |  disp|  109|  108|    Datsun 710|
    |  disp|  259|  258|Hornet 4 Drive|
    +------+-----+-----+--------------+
    """
    return d.value_diffs_stacked(self, columns)

  def slice_diffs(
    self,
    table: str,
    columns: Optional[Sequence[str]] = None,
  ) -> DataFrame:
    """Return rows from one input that differ in the selected columns.

    Parameters
    ----------
    table : str
      Input identifier to return, one of `table_id`.
    columns : sequence of str, optional
      Columns to check for differences. Defaults to all comparable columns.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with the full schema of the requested input.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.slice_diffs("a", ["mpg"]).show()
    +----------+----+---+----+---+----+----+---+---+
    |       car| mpg|cyl|disp| hp|drat|  wt| vs| am|
    +----------+----+---+----+---+----+----+---+---+
    |Duster 360|14.3|  8| 360|245|3.21|3.57|  0|  0|
    | Merc 240D|24.4|  4| 147| 62|3.69|3.19|  1|  0|
    +----------+----+---+----+---+----+----+---+---+
    """
    return l.slice_diffs(self, table, columns)

  def slice_unmatched(self, table: str) -> DataFrame:
    """Return rows from one input whose keys are missing in the other.

    Parameters
    ----------
    table : str
      Input identifier to return, one of `table_id`.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with unmatched rows from the requested input.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.slice_unmatched("a").show()
    +---------+----+---+----+---+----+----+---+---+
    |      car| mpg|cyl|disp| hp|drat|  wt| vs| am|
    +---------+----+---+----+---+----+----+---+---+
    |Mazda RX4|21.0|  6| 160|110| 3.9|2.62|  0|  1|
    +---------+----+---+----+---+----+----+---+---+
    """
    return l.slice_unmatched(self, table)

  def slice_unmatched_both(self) -> DataFrame:
    """Return unmatched rows from both inputs.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with `table_name` plus key and common columns.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.slice_unmatched_both().show()
    +----------+----------+----+---+----+---+----+----+---+
    |table_name|       car| mpg|cyl|disp| hp|drat|  wt| vs|
    +----------+----------+----+---+----+---+----+----+---+
    |         a| Mazda RX4|21.0|  6| 160|110| 3.9|2.62|  0|
    |         b| Merc 280C|17.8|  6| 168|123|3.92|3.44|  1|
    |         b|Merc 450SE|16.4|  8| 276|180|3.07|4.07|  0|
    +----------+----------+----+---+----+---+----+----+---+
    """
    return l.slice_unmatched_both(self)

  def weave_diffs_wide(
    self,
    columns: Optional[Sequence[str]] = None,
    suffix: Optional[Tuple[str, str]] = None,
  ) -> DataFrame:
    """Return a wide view of differing rows with split columns.

    Parameters
    ----------
    columns : sequence of str, optional
      Columns to compare. Defaults to all comparable columns.
    suffix : tuple[str, str], optional
      Suffixes appended to differing columns from input A and B.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with key columns and common columns, where differing
      columns are split into `<name><suffix>`.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.weave_diffs_wide(["disp"]).show()
    +--------------+----+----+------+------+---+----+----+---+
    |           car| mpg| cyl|disp_a|disp_b| hp|drat|  wt| vs|
    +--------------+----+----+------+------+---+----+----+---+
    |    Datsun 710|22.8|NULL|   109|   108| 93|3.85|2.32|  1|
    |Hornet 4 Drive|21.4|   6|   259|   258|110|3.08|3.22|  1|
    +--------------+----+----+------+------+---+----+----+---+
    """
    return w.weave_diffs_wide(self, columns, suffix)

  def weave_diffs_long(
    self,
    columns: Optional[Sequence[str]] = None,
  ) -> DataFrame:
    """Return a long view of differing rows stacked by input.

    Parameters
    ----------
    columns : sequence of str, optional
      Columns to compare. Defaults to all comparable columns.

    Returns
    -------
    pyspark.sql.DataFrame
      DataFrame with `table_name` plus key and common columns.

    Examples
    --------
    >>> from versus._spark import compare, examples
    >>> comparison = compare(
    ...   examples.example_cars_a(),
    ...   examples.example_cars_b(),
    ...   by=["car"],
    ... )
    >>> comparison.weave_diffs_long(["disp"]).show()
    +----------+--------------+----+----+----+---+----+----+---+
    |table_name|           car| mpg| cyl|disp| hp|drat|  wt| vs|
    +----------+--------------+----+----+----+---+----+----+---+
    |         a|    Datsun 710|22.8|NULL| 109| 93|3.85|2.32|  1|
    |         b|    Datsun 710|22.8|NULL| 108| 93|3.85|2.32|  1|
    |         a|Hornet 4 Drive|21.4|   6| 259|110|3.08|3.22|  1|
    |         b|Hornet 4 Drive|21.4|   6| 258|110|3.08|3.22|  1|
    +----------+--------------+----+----+----+---+----+----+---+
    """
    return w.weave_diffs_long(self, columns)

  def summary(self):
    """Summarize which difference categories are present.

    Returns
    -------
    SummaryDataFrame
      Spark-backed summary frame with `difference` and `found` columns.

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
    return s.summary(self)


__all__ = ["Comparison"]
