import duckdb
import polars as pl
import pytest

from versus import ComparisonError, compare, examples


def build_connection():
    con = duckdb.connect()
    rel_a = con.sql(
        """
        SELECT * FROM (
            VALUES
                (1, 10, 'x'),
                (2, 20, 'y'),
                (3, 30, 'z')
        ) AS t(id, value, extra)
        """
    )
    rel_b = con.sql(
        """
        SELECT * FROM (
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


def test_compare_summary():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    assert comp.tables["nrows"].to_list() == [3, 3]
    value_row = comp.intersection.filter(pl.col("column") == "value")
    assert value_row["n_diffs"][0] == 1


def test_value_diffs_and_slice():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    diffs = comp.value_diffs("value")
    assert diffs["id"][0] == 2
    rows = comp.slice_diffs("a", ["value"])
    assert rows["id"][0] == 2


def test_weave_wide():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    wide = comp.weave_diffs_wide(["value"])
    assert "value_a" in wide.columns and "value_b" in wide.columns


def test_slice_unmatched():
    con, rel_a, rel_b = build_connection()
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    unmatched = comp.slice_unmatched("a")
    assert unmatched["id"][0] == 1


def test_duplicate_by_raises():
    con = duckdb.connect()
    rel_dup = con.sql(
        """
        SELECT * FROM (
            VALUES (1, 10),
                   (1, 11)
        ) AS t(id, value)
        """
    )
    rel_other = con.sql(
        """
        SELECT * FROM (
            VALUES (1, 10)
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
    assert comp.intersection.filter(pl.col("column") == "mpg")["n_diffs"][0] == 2
    comp.close()
    con.close()


def test_compare_errors_when_by_column_missing():
    con = duckdb.connect()
    rel_a = con.sql("SELECT 1 AS id, 10 AS value")
    rel_b = con.sql("SELECT 1 AS other_id, 10 AS value")
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], connection=con)


def test_compare_errors_when_table_id_invalid_length():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], table_id=["x"], connection=con)


def test_compare_errors_when_table_id_duplicates():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], table_id=("dup", "dup"), connection=con)


def test_compare_errors_when_table_id_blank():
    con, rel_a, rel_b = build_connection()
    with pytest.raises(ComparisonError):
        compare(rel_a, rel_b, by=["id"], table_id=(" ", "b"), connection=con)


def test_compare_coerce_false_detects_type_mismatch():
    with pytest.raises(ComparisonError):
        comparison_from_sql(
            """
            SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)
            """,
            """
            SELECT * FROM (VALUES (1, '10'), (2, '20')) AS t(id, value)
            """,
            by=["id"],
            coerce=False,
        )


def test_allow_both_na_controls_diff_detection():
    sql_a = "SELECT * FROM (VALUES (1, NULL), (2, 3)) AS t(id, value)"
    sql_b = "SELECT * FROM (VALUES (1, NULL), (2, NULL)) AS t(id, value)"
    comp_true = comparison_from_sql(sql_a, sql_b, by=["id"], allow_both_na=True)
    comp_false = comparison_from_sql(sql_a, sql_b, by=["id"], allow_both_na=False)
    assert comp_true.value_diffs("value").height == 1
    assert comp_false.value_diffs("value").height == 2
    comp_true.close()
    comp_false.close()


def test_compare_handles_no_common_rows():
    comp = comparison_from_sql(
        "SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)",
        "SELECT * FROM (VALUES (3, 30), (4, 40)) AS t(id, value)",
        by=["id"],
    )
    assert comp.intersection["n_diffs"].to_list() == [0]
    assert comp.unmatched_rows.height == 4
    comp.close()


def test_compare_reports_unmatched_columns():
    comp = comparison_from_sql(
        "SELECT * FROM (VALUES (1, 1, 99), (2, 2, 99)) AS t(id, value, extra_a)",
        "SELECT * FROM (VALUES (1, 1, 88), (2, 3, 88)) AS t(id, value, extra_b)",
        by=["id"],
    )
    cols = set(zip(comp.unmatched_cols["table"], comp.unmatched_cols["column"]))
    assert cols == {("a", "extra_a"), ("b", "extra_b")}
    comp.close()
