import duckdb
import pytest
from versus import ComparisonError, compare


def rel_values(rel, column):
    idx = rel.columns.index(column)
    return [row[idx] for row in rel.fetchall()]


@pytest.fixture
def comparison_with_unmatched():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (2, 20), (3, 30), (4, 40)) AS t(id, value)"),
        by=["id"],
        connection=con,
    )
    yield comp
    comp.close()
    con.close()


def test_slice_unmatched_returns_rows(comparison_with_unmatched):
    out = comparison_with_unmatched.slice_unmatched("a")
    assert rel_values(out, "id") == [1]


def test_slice_unmatched_both_includes_table_label(comparison_with_unmatched):
    out = comparison_with_unmatched.slice_unmatched_both()
    assert set(rel_values(out, "table")) == {"a", "b"}
    assert "id" in out.columns


def test_slice_unmatched_errors_on_invalid_table(comparison_with_unmatched):
    with pytest.raises(ComparisonError):
        comparison_with_unmatched.slice_unmatched("missing")


def test_slice_unmatched_respects_custom_table_id():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (2, 20), (3, 30), (4, 40)) AS t(id, value)"),
        by=["id"],
        table_id=("left", "right"),
        connection=con,
    )
    left = comp.slice_unmatched("left")
    assert rel_values(left, "id") == [1]
    both = comp.slice_unmatched_both()
    assert set(rel_values(both, "table")) == {"left", "right"}
    comp.close()
    con.close()
