import duckdb
import pytest
from versus import ComparisonError, compare


def rel_values(rel, column):
    idx = rel.columns.index(column)
    return [row[idx] for row in rel.fetchall()]


def rel_height(rel):
    return rel.aggregate("COUNT(*) AS n").fetchone()[0]


@pytest.fixture
def comparison_with_diffs():
    con = duckdb.connect()
    rel_a = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10, 5, 'same'),
              (2, 20, 6, 'same'),
              (3, 30, 7, 'same')
          ) AS t(id, value, wind, note)
        """
    )
    rel_b = con.sql(
        """
        SELECT
          *
        FROM
          (
            VALUES
              (1, 10, 5, 'same'),
              (2, 25, 8, 'same'),
              (3, 30, 7, 'same')
          ) AS t(id, value, wind, note)
        """
    )
    comp = compare(rel_a, rel_b, by=["id"], connection=con)
    yield comp
    comp.close()
    con.close()


def test_value_diffs_reports_rows(comparison_with_diffs):
    out = comparison_with_diffs.value_diffs("value")
    assert rel_values(out, "id") == [2]
    assert rel_values(out, "value_a") == [20]
    assert rel_values(out, "value_b") == [25]


def test_value_diffs_empty_when_no_differences(comparison_with_diffs):
    out = comparison_with_diffs.value_diffs("note")
    assert rel_height(out) == 0


def test_value_diffs_stacked_empty_when_no_differences(comparison_with_diffs):
    out = comparison_with_diffs.value_diffs_stacked(["note"])
    assert rel_height(out) == 0


def test_value_diffs_stacked_errors_when_no_value_columns():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 'x')) AS t(id, tag)"),
        con.sql("SELECT * FROM (VALUES (1, 'x')) AS t(id, tag)"),
        by=["id", "tag"],
        connection=con,
    )
    with pytest.raises(ComparisonError):
        comp.value_diffs_stacked()
    comp.close()
    con.close()


def test_value_diffs_errors_on_unknown_column(comparison_with_diffs):
    with pytest.raises(ComparisonError):
        comparison_with_diffs.value_diffs("missing")


def test_value_diffs_stacked_combines_columns(comparison_with_diffs):
    out = comparison_with_diffs.value_diffs_stacked(["value", "wind"])
    assert set(rel_values(out, "column")) == {"value", "wind"}
    assert rel_height(out) == 2


def test_value_diffs_stacked_handles_incompatible_types():
    con = duckdb.connect()
    comp = compare(
        con.sql(
            "SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 11)) AS t(id, alpha, beta)"
        ),
        con.sql(
            "SELECT * FROM (VALUES (1, 'z', CAST('99' AS VARCHAR)), (2, 'c', CAST('77' AS VARCHAR))) "
            "AS t(id, alpha, beta)"
        ),
        by=["id"],
        connection=con,
    )
    out = comp.value_diffs_stacked(["alpha", "beta"])
    assert set(rel_values(out, "column")) == {"alpha", "beta"}
    comp.close()
    con.close()


def test_value_diffs_respects_custom_table_ids():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (1, 15), (2, 20)) AS t(id, value)"),
        by=["id"],
        table_id=("original", "updated"),
        connection=con,
    )
    out = comp.value_diffs("value")
    assert {"value_original", "value_updated"}.issubset(set(out.columns))
    comp.close()
    con.close()


def test_value_diffs_rejects_multiple_columns(comparison_with_diffs):
    with pytest.raises(ComparisonError):
        comparison_with_diffs.value_diffs(["value", "wind"])


def test_value_diffs_stacked_errors_on_unknown_column(comparison_with_diffs):
    with pytest.raises(ComparisonError):
        comparison_with_diffs.value_diffs_stacked(["value", "missing"])


def test_value_diffs_stacked_rejects_empty_selection(comparison_with_diffs):
    with pytest.raises(ComparisonError):
        comparison_with_diffs.value_diffs_stacked([])
