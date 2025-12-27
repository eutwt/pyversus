from typing import Any, cast

import duckdb
import pytest
from versus import ComparisonError, compare, examples


def rel_height(rel):
    return rel.aggregate("COUNT(*) AS n").fetchone()[0]


def rel_values(rel, column):
    idx = rel.columns.index(column)
    return [row[idx] for row in rel.fetchall()]


def rel_first(rel, column):
    values = rel_values(rel, column)
    return values[0] if values else None


def rel_dicts(rel):
    cols = rel.columns
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def rel_dtypes(rel):
    return [str(dtype) for dtype in rel.dtypes]


def build_connection():
    con = duckdb.connect()
    rel_a = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10, 'x'),
              (2, 20, 'y'),
              (3, 30, 'z')
          ) AS t(id, value, extra)
        """
    )
    rel_b = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (2, 22, 'y'),
              (3, 30, 'z'),
              (4, 40, 'w')
          ) AS t(id, value, extra)
        """
    )
    return con, rel_a, rel_b


def comparison_from_sql(sql_a: str, sql_b: str, *, by, **kwargs):
    con = duckdb.connect()
    rel_a = con.sql(sql_a)
    rel_b = con.sql(sql_b)
    return compare(rel_a, rel_b, by=by, connection=con, **kwargs)


def identical_comparison():
    sql = """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10),
              (2, 20)
          ) AS t(id, value)
    """
    return comparison_from_sql(sql, sql, by=["id"])


def test_compare_summary():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    assert rel_values(comp.tables, "nrow") == [3, 3]
    value_row = rel_dicts(comp.intersection.filter("\"column\" = 'value'"))[0]
    assert value_row["n_diffs"] == 1


def test_inputs_property_exposes_relations():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    inputs = comp.inputs
    assert isinstance(inputs, dict)
    assert "a" in inputs and "b" in inputs
    assert "id" in inputs["a"].columns
    comp.close()


def test_value_diffs_and_slice():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    diffs = comp.value_diffs("value")
    assert rel_first(diffs, "id") == 2
    rows = comp.slice_diffs("a", ["value"])
    assert rel_first(rows, "id") == 2


def test_weave_wide():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    wide = comp.weave_diffs_wide(["value"])
    assert "value_a" in wide.columns and "value_b" in wide.columns


def test_slice_unmatched():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    unmatched = comp.slice_unmatched("a")
    assert rel_first(unmatched, "id") == 1
    comp.close()


def test_summary_reports_difference_categories():
    con = duckdb.connect()
    rel_a = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10, CAST(1.5 AS DOUBLE), 'only_a'),
              (2, 20, CAST(2.5 AS DOUBLE), 'only_a')
          ) AS t(id, value, note, extra)
        """
    )
    rel_b = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 99, 1),
              (3, 30, 2)
          ) AS t(id, value, note)
        """
    )
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    summary = comp.summary()
    assert summary.fetchall() == [
        ("value_diffs", True),
        ("unmatched_cols", True),
        ("unmatched_rows", True),
        ("type_diffs", True),
    ]
    comp.close()


def test_summary_repr_shows_full_difference_labels():
    con = duckdb.connect()
    comp = compare(
        examples.example_cars_a(con),
        examples.example_cars_b(con),
        by=["car"],
        connection=con,
    )
    rendered = str(comp.summary())
    assert "unmatched_cols" in rendered
    assert "unmatched_rows" in rendered
    comp.close()
    con.close()


def test_duplicate_by_raises():
    con = duckdb.connect()
    rel_dup = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10),
              (1, 11)
          ) AS t(id, value)
        """
    )
    rel_other = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10)
          ) AS t(id, value)
        """
    )
    with pytest.raises(ComparisonError):
        compare(rel_dup, rel_other, by=["id"], connection=con)


def test_examples_available():
    con = duckdb.connect()
    comp = compare(
        examples.example_cars_a(con),
        examples.example_cars_b(con),
        by=["car"],
        connection=con,
    )
    assert rel_dicts(comp.intersection.filter("\"column\" = 'mpg'"))[0]["n_diffs"] == 2
    comp.close()
    con.close()


def test_compare_errors_when_by_column_missing():
    con = duckdb.connect()
    rel_a = con.sql("SELECT 1 AS id, 10 AS value")
    rel_b = con.sql("SELECT 1 AS other_id, 10 AS value")
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], connection=con)


def test_compare_errors_on_relations_from_non_default_connection():
    default_conn = duckdb.connect()
    other_conn = duckdb.connect()
    original_default = duckdb.default_connection
    setattr(duckdb, "default_connection", default_conn)
    try:
        rel_a = other_conn.sql("SELECT 1 AS id, 10 AS value")
        rel_b = other_conn.sql("SELECT 1 AS id, 11 AS value")
        with pytest.raises(ComparisonError, match="non-default DuckDB connection"):
            compare(rel_a, rel_b, by=["id"])
    finally:
        setattr(duckdb, "default_connection", original_default)
        default_conn.close()
        other_conn.close()


def test_compare_errors_when_table_id_invalid_length():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        bad_table_id = cast(Any, ["x"])
        compare(rel_a, rel_b, by=["id"], table_id=bad_table_id, connection=con)


def test_compare_errors_when_table_id_duplicates():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], table_id=("dup", "dup"), connection=con)


def test_compare_errors_when_table_id_blank():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], table_id=(" ", "b"), connection=con)


def test_compare_errors_when_materialize_invalid():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], connection=con, materialize=cast(Any, "nope"))
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], connection=con, materialize=cast(Any, True))
    con.close()


def test_intersection_empty_when_no_value_columns():
    sql = "SELECT * FROM (VALUES (1, 10)) AS t(id, value)"
    comp = comparison_from_sql(sql, sql, by=["id", "value"])
    assert comp.common_columns == []
    assert rel_height(comp.intersection) == 0
    assert comp.intersection.columns == ["column", "n_diffs", "type_a", "type_b"]


def test_compare_coerce_false_detects_type_mismatch():
    with pytest.raises(ComparisonError):
        comparison_from_sql(
            """
            SELECT
              *
            FROM
              (
                VALUES
                  (1, 10),
                  (2, 20)
              ) AS t(id, value)
            """,
            """
            SELECT
              *
            FROM
              (
                VALUES
                  (1, '10'),
                  (2, '20')
              ) AS t(id, value)
            """,
            by=["id"],
            coerce=False,
        )


def test_allow_both_na_controls_diff_detection():
    sql_a = "SELECT * FROM (VALUES (1, NULL), (2, 3)) AS t(id, value)"
    sql_b = "SELECT * FROM (VALUES (1, NULL), (2, NULL)) AS t(id, value)"
    comp_true = comparison_from_sql(sql_a, sql_b, by=["id"], allow_both_na=True)
    comp_false = comparison_from_sql(sql_a, sql_b, by=["id"], allow_both_na=False)
    assert rel_height(comp_true.value_diffs("value")) == 1
    assert rel_height(comp_false.value_diffs("value")) == 2
    comp_true.close()
    comp_false.close()


def test_compare_handles_no_common_rows():
    comp = comparison_from_sql(
        "SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)",
        "SELECT * FROM (VALUES (3, 30), (4, 40)) AS t(id, value)",
        by=["id"],
    )
    assert rel_values(comp.intersection, "n_diffs") == [0]
    assert rel_height(comp.unmatched_keys) == 4
    counts = {
        (row["table"], row["n_unmatched"]) for row in rel_dicts(comp.unmatched_rows)
    }
    assert counts == {("a", 2), ("b", 2)}
    comp.close()


def test_compare_reports_unmatched_columns():
    comp = comparison_from_sql(
        "SELECT * FROM (VALUES (1, 1, 99), (2, 2, 99)) AS t(id, value, extra_a)",
        "SELECT * FROM (VALUES (1, 1, 88), (2, 3, 88)) AS t(id, value, extra_b)",
        by=["id"],
    )
    cols = {(row["table"], row["column"]) for row in rel_dicts(comp.unmatched_cols)}
    assert cols == {("a", "extra_a"), ("b", "extra_b")}
    comp.close()


def test_value_diffs_empty_structure():
    comp = identical_comparison()
    rel = comp.value_diffs("value")
    assert rel_height(rel) == 0
    assert rel.columns == ["value_a", "value_b", "id"]
    assert rel_dtypes(rel) == ["INTEGER", "INTEGER", "INTEGER"]
    comp.close()


def test_value_diffs_stacked_empty_structure():
    comp = identical_comparison()
    rel = comp.value_diffs_stacked()
    assert rel_height(rel) == 0
    assert rel.columns == [
        "column",
        f"val_{comp.table_id[0]}",
        f"val_{comp.table_id[1]}",
        *comp.by_columns,
    ]
    assert rel_dtypes(rel) == ["VARCHAR", "INTEGER", "INTEGER", "INTEGER"]
    comp.close()


def test_slice_diffs_empty_structure():
    comp = identical_comparison()
    rel = comp.slice_diffs("a", ["value"])
    assert rel_height(rel) == 0
    assert rel.columns == ["id", "value"]
    assert rel_dtypes(rel) == ["INTEGER", "INTEGER"]
    comp.close()


def test_weave_wide_empty_structure():
    comp = identical_comparison()
    rel = comp.weave_diffs_wide(["value"])
    assert rel_height(rel) == 0
    assert rel.columns == ["id", "value"]
    assert rel_dtypes(rel) == ["INTEGER", "INTEGER"]
    comp.close()


def test_weave_long_empty_structure():
    comp = identical_comparison()
    rel = comp.weave_diffs_long(["value"])
    assert rel_height(rel) == 0
    assert rel.columns == ["table", "id", "value"]
    assert rel_dtypes(rel) == ["VARCHAR", "INTEGER", "INTEGER"]
    comp.close()


def test_slice_unmatched_empty_structure():
    comp = identical_comparison()
    rel = comp.slice_unmatched("a")
    assert rel_height(rel) == 0
    assert rel.columns == ["id", "value"]
    assert rel_dtypes(rel) == ["INTEGER", "INTEGER"]
    comp.close()


def test_slice_unmatched_both_empty_structure():
    comp = identical_comparison()
    rel = comp.slice_unmatched_both()
    assert rel_height(rel) == 0
    assert rel.columns == ["table", "id", "value"]
    assert rel_dtypes(rel) == ["VARCHAR", "INTEGER", "INTEGER"]
    comp.close()


def test_unmatched_cols_empty_preserves_types():
    comp = identical_comparison()
    assert rel_dtypes(comp.unmatched_cols) == ["VARCHAR", "VARCHAR", "VARCHAR"]
    comp.close()


def test_unmatched_keys_empty_structure():
    comp = identical_comparison()
    assert rel_height(comp.unmatched_keys) == 0
    assert rel_dtypes(comp.unmatched_keys) == ["VARCHAR", "INTEGER"]
    comp.close()


def test_unmatched_rows_empty_structure():
    comp = identical_comparison()
    assert rel_height(comp.unmatched_rows) == 2
    assert rel_dtypes(comp.unmatched_rows) == ["VARCHAR", "BIGINT"]
    counts = {
        (row["table"], row["n_unmatched"]) for row in rel_dicts(comp.unmatched_rows)
    }
    assert counts == {("a", 0), ("b", 0)}
    comp.close()


def test_comparison_repr_snapshot():
    con = duckdb.connect()
    con.execute(
        "CREATE OR REPLACE TABLE foo AS SELECT * FROM (VALUES (1, 10, 'x'), (2, 20, 'y')) AS t(id, value, extra)"
    )
    con.execute(
        "CREATE OR REPLACE TABLE bar AS SELECT * FROM (VALUES (2, 22, 'y'), (3, 30, 'z')) AS t(id, value, extra)"
    )
    comp = compare(con.table("foo"), con.table("bar"), by=["id"], connection=con)
    text = repr(comp)
    assert "Comparison(tables=" in text
    assert "by=" in text
    assert "intersection=" in text
    assert "unmatched_rows=" in text
    comp.close()
