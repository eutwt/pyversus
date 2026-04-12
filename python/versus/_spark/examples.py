"""Example PySpark DataFrames for quick experimentation."""

from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from versus._spark._spark_values import build_values_frame


def example_cars_a(spark: Optional[SparkSession] = None) -> DataFrame:
  """Return example input A as a PySpark DataFrame.

  Parameters
  ----------
  spark : pyspark.sql.SparkSession, optional
    Spark session used to create the DataFrame. If omitted, a local session
    is created or reused.

  Returns
  -------
  pyspark.sql.DataFrame
    Example DataFrame used throughout the docs and tests.

  Examples
  --------
  >>> from versus._spark import examples
  >>> example_a = examples.example_cars_a()
  >>> example_a.show(3)
  +-------------+----+---+----+---+----+----+---+---+
  |          car| mpg|cyl|disp| hp|drat|  wt| vs| am|
  +-------------+----+---+----+---+----+----+---+---+
  |   Duster 360|14.3|  8| 360|245|3.21|3.57|  0|  0|
  |Mazda RX4 Wag|21.0|  6| 160|110| 3.9|2.88|  0|  1|
  |     Merc 230|22.8|  4| 141| 95|3.92|3.15|  1|  0|
  +-------------+----+---+----+---+----+----+---+---+
  only showing top 3 rows
  """
  session = resolve_spark(spark)
  schema = T.StructType(
    [
      T.StructField("car", T.StringType(), True),
      T.StructField("mpg", T.DoubleType(), True),
      T.StructField("cyl", T.IntegerType(), True),
      T.StructField("disp", T.IntegerType(), True),
      T.StructField("hp", T.IntegerType(), True),
      T.StructField("drat", T.DoubleType(), True),
      T.StructField("wt", T.DoubleType(), True),
      T.StructField("vs", T.IntegerType(), True),
      T.StructField("am", T.IntegerType(), True),
    ]
  )
  rows = [
    ("Duster 360", 14.3, 8, 360, 245, 3.21, 3.57, 0, 0),
    ("Mazda RX4 Wag", 21.0, 6, 160, 110, 3.90, 2.88, 0, 1),
    ("Merc 230", 22.8, 4, 141, 95, 3.92, 3.15, 1, 0),
    ("Datsun 710", 22.8, None, 109, 93, 3.85, 2.32, 1, 1),
    ("Merc 240D", 24.4, 4, 147, 62, 3.69, 3.19, 1, 0),
    ("Hornet 4 Drive", 21.4, 6, 259, 110, 3.08, 3.22, 1, 0),
    ("Mazda RX4", 21.0, 6, 160, 110, 3.90, 2.62, 0, 1),
    ("Valiant", 18.1, 6, 225, 105, 2.76, 3.46, 1, 0),
    ("Merc 280", 19.2, 6, 168, 123, 3.92, 3.44, 1, 0),
  ]
  return build_values_frame(session, rows, schema)


def example_cars_b(spark: Optional[SparkSession] = None) -> DataFrame:
  """Return example input B as a PySpark DataFrame.

  Parameters
  ----------
  spark : pyspark.sql.SparkSession, optional
    Spark session used to create the DataFrame. If omitted, a local session
    is created or reused.

  Returns
  -------
  pyspark.sql.DataFrame
    Example DataFrame used throughout the docs and tests.

  Examples
  --------
  >>> from versus._spark import examples
  >>> example_b = examples.example_cars_b()
  >>> example_b.show(3)
  +----------+----+----+---+---+----+----+----+---+
  |       car|  wt| mpg| hp|cyl|disp|carb|drat| vs|
  +----------+----+----+---+---+----+----+----+---+
  | Merc 240D|3.19|26.4| 62|  4| 147|   2|3.69|  1|
  |   Valiant|3.46|18.1|105|  6| 225|   1|2.76|  1|
  |Duster 360|3.57|16.3|245|  8| 360|   4|3.21|  0|
  +----------+----+----+---+---+----+----+----+---+
  only showing top 3 rows
  """
  session = resolve_spark(spark)
  schema = T.StructType(
    [
      T.StructField("car", T.StringType(), True),
      T.StructField("wt", T.DoubleType(), True),
      T.StructField("mpg", T.DoubleType(), True),
      T.StructField("hp", T.IntegerType(), True),
      T.StructField("cyl", T.IntegerType(), True),
      T.StructField("disp", T.IntegerType(), True),
      T.StructField("carb", T.IntegerType(), True),
      T.StructField("drat", T.DoubleType(), True),
      T.StructField("vs", T.IntegerType(), True),
    ]
  )
  rows = [
    ("Merc 240D", 3.19, 26.4, 62, 4, 147, 2, 3.69, 1),
    ("Valiant", 3.46, 18.1, 105, 6, 225, 1, 2.76, 1),
    ("Duster 360", 3.57, 16.3, 245, 8, 360, 4, 3.21, 0),
    ("Datsun 710", 2.32, 22.8, 93, None, 108, 1, 3.85, 1),
    ("Merc 280C", 3.44, 17.8, 123, 6, 168, 4, 3.92, 1),
    ("Merc 280", 3.44, 19.2, 123, 6, 168, 4, 3.92, 1),
    ("Hornet 4 Drive", 3.22, 21.4, 110, 6, 258, 1, 3.08, 1),
    ("Merc 450SE", 4.07, 16.4, 180, 8, 276, 3, 3.07, 0),
    ("Merc 230", 3.15, 22.8, 95, 4, 141, 2, 3.92, 1),
    ("Mazda RX4 Wag", 2.88, 21.0, 110, 6, 160, 4, 3.90, 0),
  ]
  return build_values_frame(session, rows, schema)


def resolve_spark(spark: Optional[SparkSession]) -> SparkSession:
  """Resolve the Spark session used by the example helpers.

  Parameters
  ----------
  spark : pyspark.sql.SparkSession, optional
    Existing Spark session to reuse.

  Returns
  -------
  pyspark.sql.SparkSession
    The provided session, or a local `versus._spark` session.
  """
  if spark is not None:
    return spark
  os.environ["PYSPARK_PYTHON"] = sys.executable
  os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
  builder = (
    SparkSession.builder.master("local[*]")
    .appName("versus._spark")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
  )
  if sys.platform == "win32":
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    builder = (
      builder.config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.python.worker.reuse", "false")
    )
  return builder.getOrCreate()


__all__ = ["example_cars_a", "example_cars_b", "resolve_spark"]
