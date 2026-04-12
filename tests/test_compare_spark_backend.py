from typing import Any, cast

import pytest
pytest.importorskip("pyspark")
from pyspark.sql import functions as F
from pyspark.sql import types as T
from versus import ComparisonError, compare, examples
from versus._spark._spark_values import build_values_frame
from versus._spark.comparison import _core as core_helpers
from versus._spark.comparison import _frames as frame_helpers
from versus._spark.comparison import _frames as named_frame_helpers
from versus._spark.comparison import _inputs as input_helpers
from versus._spark.comparison import _relations as relation_helpers
from versus._spark.comparison import _slices as slice_helpers
from versus._spark.comparison import _summary as summary_helpers
from versus._spark.comparison import _value_diffs as value_diff_helpers
from versus.comparison.api import compare as api_compare
from versus._spark.comparison.comparison import Comparison as ComparisonClass


def frame_height(frame):
  return frame.count()


def frame_values(frame, column):
  return [row[column] for row in frame.collect()]


def frame_first(frame, column):
  values = frame_values(frame, column)
  return values[0] if values else None


def frame_dicts(frame):
  return [row.asDict(recursive=True) for row in frame.collect()]


def build_frames(spark):
  schema = T.StructType(
    [
      T.StructField("id", T.IntegerType(), True),
      T.StructField("value", T.IntegerType(), True),
      T.StructField("extra", T.StringType(), True),
    ]
  )
  left = build_values_frame(
    spark,
    [(1, 10, "x"), (2, 20, "y"), (3, 30, "z")],
    schema,
  )
  right = build_values_frame(
    spark,
    [(2, 22, "y"), (3, 30, "z"), (4, 40, "w")],
    schema,
  )
  return left, right


def identical_comparison(spark):
  schema = T.StructType(
    [
      T.StructField("id", T.IntegerType(), True),
      T.StructField("value", T.IntegerType(), True),
    ]
  )
  frame = build_values_frame(spark, [(1, 10), (2, 20)], schema)
  return compare(frame, frame, by=["id"])


def test_compare_summary_and_inputs(spark):
  left, right = build_frames(spark)
  comp = compare(left, right, by=["id"])

  assert frame_values(comp.tables, "nrow") == [3, 3]
  value_row = frame_dicts(comp.intersection.filter(F.col("column") == "value"))[
    0
  ]
  assert value_row["n_diffs"] == 1
  assert set(comp.inputs) == {"a", "b"}
  assert "id" in comp.inputs["a"].columns
  assert api_compare is compare
  assert isinstance(comp, ComparisonClass)


def test_internal_helper_names_are_spark_oriented():
  assert hasattr(frame_helpers, "build_tables_frame")
  assert hasattr(named_frame_helpers, "build_tables_frame")
  assert not hasattr(core_helpers, "_build_tables_frame")
  assert not hasattr(core_helpers, "_compute_diff_table")
  assert hasattr(relation_helpers, "frame_is_empty")
  assert not hasattr(relation_helpers, "relation_is_empty")
  assert hasattr(summary_helpers, "SummaryFrame")
  assert not hasattr(summary_helpers, "SummaryRelation")
  assert hasattr(summary_helpers, "build_rows_frame")
  assert not hasattr(summary_helpers, "build_rows_relation")
  assert hasattr(input_helpers, "assert_dataframe_input")
  assert not hasattr(input_helpers, "assert_relation_connection")
  assert hasattr(slice_helpers, "build_unmatched_keys_selection")
  assert not hasattr(slice_helpers, "build_unmatched_keys_sql")
  assert hasattr(value_diff_helpers, "stack_value_diffs_by_keys")
  assert not hasattr(value_diff_helpers, "stack_value_diffs_sql")


@pytest.mark.parametrize("materialize", ["all", "summary", "none"])
def test_materialize_modes(spark, materialize):
  left, right = build_frames(spark)
  comp = compare(left, right, by=["id"], materialize=materialize)

  assert frame_values(comp.tables, "nrow") == [3, 3]
  assert frame_first(comp.value_diffs("value"), "id") == 2
  assert frame_first(comp.value_diffs_stacked(["value"]), "column") == "value"
  assert frame_first(comp.slice_diffs("a", ["value"]), "id") == 2
  assert "value_a" in comp.weave_diffs_wide(["value"]).columns
  assert "value" in comp.weave_diffs_long(["value"]).columns
  assert frame_first(comp.slice_unmatched("a"), "id") == 1
  assert "table_name" in comp.slice_unmatched_both().columns

  assert comp.intersection.materialized is (materialize in {"all", "summary"})
  assert comp.unmatched_rows.materialized is (materialize in {"all", "summary"})
  assert (comp.diff_table is not None) is (materialize == "all")
  if materialize == "none":
    assert comp._diff_lookup is None
    assert comp._unmatched_lookup is None
    _ = str(comp)
    assert comp._diff_lookup is not None
    assert comp._unmatched_lookup is not None


def test_summary_reports_difference_categories(spark):
  left = build_values_frame(
    spark,
    [(1, 10, 1.5, "only_a"), (2, 20, 2.5, "only_a")],
    T.StructType(
      [
        T.StructField("id", T.IntegerType(), True),
        T.StructField("value", T.IntegerType(), True),
        T.StructField("note", T.DoubleType(), True),
        T.StructField("extra", T.StringType(), True),
      ]
    ),
  )
  right = build_values_frame(
    spark,
    [(1, 99, "1"), (3, 30, "2")],
    T.StructType(
      [
        T.StructField("id", T.IntegerType(), True),
        T.StructField("value", T.IntegerType(), True),
        T.StructField("note", T.StringType(), True),
      ]
    ),
  )
  comp = compare(left, right, by=["id"])

  assert frame_dicts(comp.summary()) == [
    {"difference": "value_diffs", "found": True},
    {"difference": "unmatched_cols", "found": True},
    {"difference": "unmatched_rows", "found": True},
    {"difference": "type_diffs", "found": True},
  ]


def test_duplicate_by_raises(spark):
  dup = build_values_frame(spark, [(1, 10), (1, 11)], ["id", "value"])
  other = build_values_frame(spark, [(1, 10)], ["id", "value"])

  with pytest.raises(ComparisonError):
    compare(dup, other, by=["id"])


def test_compare_examples(spark):
  comp = compare(
    examples.example_cars_a_spark(spark),
    examples.example_cars_b_spark(spark),
    by=["car"],
  )
  rendered = str(comp.summary())
  assert "unmatched_cols" in rendered
  assert "unmatched_rows" in rendered


def test_value_diffs_helpers(spark):
  left = build_values_frame(
    spark,
    [(1, 10, 5, "same"), (2, 20, 6, "same"), (3, 30, 7, "same")],
    ["id", "value", "wind", "note"],
  )
  right = build_values_frame(
    spark,
    [(1, 10, 5, "same"), (2, 25, 8, "same"), (3, 30, 7, "same")],
    ["id", "value", "wind", "note"],
  )
  comp = compare(left, right, by=["id"])

  out = comp.value_diffs("value")
  assert frame_values(out, "id") == [2]
  assert frame_values(out, "value_a") == [20]
  assert frame_values(out, "value_b") == [25]
  assert frame_height(comp.value_diffs("note")) == 0
  assert frame_height(comp.value_diffs_stacked(["note"])) == 0
  assert set(
    frame_values(comp.value_diffs_stacked(["value", "wind"]), "column")
  ) == {
    "value",
    "wind",
  }

  with pytest.raises(ComparisonError):
    comp.value_diffs("missing")
  with pytest.raises(ComparisonError):
    comp.value_diffs(cast(Any, ["value", "wind"]))
  with pytest.raises(ComparisonError):
    comp.value_diffs_stacked(["value", "missing"])
  with pytest.raises(ComparisonError):
    comp.value_diffs_stacked([])


def test_value_diffs_stacked_handles_incompatible_types(spark):
  left = build_values_frame(
    spark,
    [(1, "a", 10), (2, "b", 11)],
    ["id", "alpha", "beta"],
  )
  right = build_values_frame(
    spark,
    [(1, "z", "99"), (2, "c", "77")],
    T.StructType(
      [
        T.StructField("id", T.IntegerType(), True),
        T.StructField("alpha", T.StringType(), True),
        T.StructField("beta", T.StringType(), True),
      ]
    ),
  )
  comp = compare(left, right, by=["id"])
  out = comp.value_diffs_stacked(["alpha", "beta"])
  assert set(frame_values(out, "column")) == {"alpha", "beta"}


def test_value_diffs_respect_custom_table_ids(spark):
  left = build_values_frame(spark, [(1, 10), (2, 20)], ["id", "value"])
  right = build_values_frame(spark, [(1, 15), (2, 20)], ["id", "value"])
  comp = compare(left, right, by=["id"], table_id=("original", "updated"))

  assert {"value_original", "value_updated"}.issubset(
    comp.value_diffs("value").columns
  )


def test_value_diffs_stacked_errors_when_no_value_columns(spark):
  left = build_values_frame(spark, [(1, "x")], ["id", "tag"])
  right = build_values_frame(spark, [(1, "x")], ["id", "tag"])
  comp = compare(left, right, by=["id", "tag"])

  with pytest.raises(ComparisonError):
    comp.value_diffs_stacked()


def test_slice_helpers(spark):
  left = build_values_frame(
    spark,
    [(1, 10, 1, "same"), (2, 20, 1, "same"), (3, 30, 1, "same")],
    ["id", "value", "other", "note"],
  )
  right = build_values_frame(
    spark,
    [(1, 10, 1, "same"), (2, 25, 2, "same"), (3, 35, 1, "same")],
    ["id", "value", "other", "note"],
  )
  comp = compare(left, right, by=["id"])

  rows = comp.slice_diffs("a", ["value"])
  assert sorted(frame_values(rows, "id")) == [2, 3]
  rows = comp.slice_diffs("a", ["value", "other"])
  assert sorted(frame_values(rows, "id")) == [2, 3]
  assert rows.columns == ["id", "value", "other", "note"]
  empty = comp.slice_diffs("a", ["note"])
  assert frame_height(empty) == 0
  assert empty.columns == ["id", "value", "other", "note"]

  with pytest.raises(ComparisonError):
    comp.slice_diffs("missing", ["value"])
  with pytest.raises(ComparisonError):
    comp.slice_diffs("a", ["unknown"])
  with pytest.raises(ComparisonError):
    comp.slice_diffs("a", [])


def test_unmatched_helpers(spark):
  left, right = build_frames(spark)
  comp = compare(left, right, by=["id"])

  assert frame_values(comp.slice_unmatched("a"), "id") == [1]
  both = comp.slice_unmatched_both()
  assert set(frame_values(both, "table_name")) == {"a", "b"}
  assert "id" in both.columns

  with pytest.raises(ComparisonError):
    comp.slice_unmatched("missing")

  renamed = compare(left, right, by=["id"], table_id=("left", "right"))
  assert frame_values(renamed.slice_unmatched("left"), "id") == [1]
  assert set(frame_values(renamed.slice_unmatched_both(), "table_name")) == {
    "left",
    "right",
  }


def test_weave_helpers(spark):
  left = build_values_frame(
    spark,
    [(1, 10, 1), (2, 20, 1)],
    [
      "id",
      "value",
      "wind",
    ],
  )
  right = build_values_frame(
    spark,
    [(1, 10, 2), (2, 25, 1)],
    [
      "id",
      "value",
      "wind",
    ],
  )
  comp = compare(left, right, by=["id"])

  wide = comp.weave_diffs_wide(["value"])
  assert {"value_a", "value_b"}.issubset(wide.columns)
  long = comp.weave_diffs_long(["value"])
  assert set(frame_values(long, "table_name")) == {"a", "b"}
  custom = comp.weave_diffs_wide(["value"], suffix=("_old", "_new"))
  assert {"value_old", "value_new"}.issubset(custom.columns)
  empty_suffix = comp.weave_diffs_wide(["value"], suffix=("", "_new"))
  assert {"value", "value_new"}.issubset(empty_suffix.columns)

  with pytest.raises(ComparisonError):
    comp.weave_diffs_wide(["value"], suffix=("dup", "dup"))
  with pytest.raises(ComparisonError):
    comp.weave_diffs_wide(["value"], suffix=cast(Any, "oops"))


def test_weave_long_empty_and_ordering(spark):
  comp = identical_comparison(spark)
  assert frame_height(comp.weave_diffs_long(["value"])) == 0

  left = build_values_frame(spark, [(1, 10), (2, 20)], ["id", "value"])
  right = build_values_frame(spark, [(1, 11), (2, 25)], ["id", "value"])
  comp = compare(left, right, by=["id"])

  out = comp.weave_diffs_long(["value"])
  assert frame_values(out, "table_name") == ["a", "b", "a", "b"]
  assert frame_values(out, "id") == [1, 1, 2, 2]


def test_weave_and_value_diffs_respect_custom_ids(spark):
  left = build_values_frame(spark, [(1, 10), (2, 20)], ["id", "value"])
  right = build_values_frame(spark, [(1, 15), (2, 20)], ["id", "value"])
  comp = compare(left, right, by=["id"], table_id=("original", "updated"))

  assert {"value_original", "value_updated"}.issubset(
    comp.weave_diffs_wide(["value"]).columns
  )
  assert set(frame_values(comp.weave_diffs_long(["value"]), "table_name")) == {
    "original",
    "updated",
  }


def test_compare_rejects_mixed_sessions(spark):
  other = build_values_frame(spark.newSession(), [(1, 10)], ["id", "value"])
  frame = build_values_frame(spark, [(1, 10)], ["id", "value"])

  with pytest.raises(ComparisonError):
    compare(frame, other, by=["id"])
