import duckdb
import pytest

from versus import ComparisonError, compare


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
    assert out["id"].to_list() == [1]


def test_slice_unmatched_both_includes_table_label(comparison_with_unmatched):
    out = comparison_with_unmatched.slice_unmatched_both()
    assert set(out["table"].to_list()) == {"a", "b"}
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
    assert left["id"].to_list() == [1]
    both = comp.slice_unmatched_both()
    assert set(both["table"].to_list()) == {"left", "right"}
    comp.close()
    con.close()
