from __future__ import annotations

import duckdb
import pytest

from versus import Comparison, ComparisonError, compare, examples


def frame_values(frame, column):
    return [row[column] for row in frame.collect()]


def frame_dicts(frame):
    return [row.asDict(recursive=True) for row in frame.collect()]


def test_compare_dispatches_to_spark_for_spark_inputs(spark):
    comp = compare(
        examples.example_cars_a_spark(spark),
        examples.example_cars_b_spark(spark),
        by=["car"],
    )

    assert isinstance(comp, Comparison)
    assert frame_values(comp.tables, "nrow") == [9, 10]
    diff_row = [row for row in frame_dicts(comp.intersection) if row["column"] == "mpg"]
    assert diff_row[0]["n_diffs"] == 2


def test_compare_dispatches_to_spark_for_mixed_inputs(spark):
    pandas = pytest.importorskip("pandas")

    comp = compare(
        pandas.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]}),
        spark.createDataFrame([(2, 22), (3, 30), (4, 40)], ["id", "value"]),
        by=["id"],
    )

    rows = frame_dicts(comp.intersection)
    value_row = [row for row in rows if row["column"] == "value"][0]
    assert value_row["n_diffs"] == 1
    assert comp.slice_unmatched("a").count() == 1
    assert comp.slice_unmatched("b").count() == 1


def test_compare_dispatches_to_spark_when_spark_session_is_supplied(spark):
    rel = duckdb.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10),
              (2, 20)
          ) AS t(id, value)
        """
    )
    pandas = pytest.importorskip("pandas")
    comp = compare(
        rel,
        pandas.DataFrame({"id": [1, 2], "value": [10, 22]}),
        by=["id"],
        spark=spark,
    )

    assert comp.value_diffs("value").count() == 1


def test_examples_expose_spark_helpers(spark):
    frame = examples.example_cars_a_spark(spark)
    assert frame.count() == 9


def test_compare_rejects_duckdb_connection_when_using_spark_backend(spark):
    with pytest.raises(ComparisonError, match="DuckDB-backed"):
        compare(
            examples.example_cars_a_spark(spark),
            examples.example_cars_b_spark(spark),
            by=["car"],
            con=duckdb.connect(),
        )
