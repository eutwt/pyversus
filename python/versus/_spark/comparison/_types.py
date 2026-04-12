from pyspark.sql import DataFrame, SparkSession

from versus._spark.comparison._core import SummaryDataFrame, _TableHandle

_Input = DataFrame
VersusConn = SparkSession

__all__ = [
  "SummaryDataFrame",
  "VersusConn",
  "_Input",
  "_TableHandle",
]
