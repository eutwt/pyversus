import duckdb
import pytest

from versus import ComparisonError, compare


@pytest.fixture
def comparison_for_slice():
    con = duckdb.connect()
    comp = compare(
        con.sql(
            "SELECT * FROM (VALUES (1, 10, 1, 'same'), (2, 20, 1, 'same'), (3, 30, 1, 'same')) "
            "AS t(id, value, other, note)"
        ),
        con.sql(
            "SELECT * FROM (VALUES (1, 10, 1, 'same'), (2, 25, 2, 'same'), (3, 35, 1, 'same')) "
            "AS t(id, value, other, note)"
        ),
        by=["id"],
        connection=con,
    )
    yield comp
    comp.close()
    con.close()


def test_slice_diffs_returns_rows(comparison_for_slice):
    out = comparison_for_slice.slice_diffs("a", ["value"])
    assert sorted(out["id"].to_list()) == [2, 3]


def test_slice_diffs_does_not_duplicate_rows(comparison_for_slice):
    out = comparison_for_slice.slice_diffs("a", ["value", "other"])
    assert sorted(out["id"].to_list()) == [2, 3]
    counts = out.group_by("id").len()
    assert counts["len"][0] == 1 and counts["len"][1] == 1


def test_slice_diffs_only_includes_requested_columns(comparison_for_slice):
    out = comparison_for_slice.slice_diffs("a", ["value", "note"])
    assert out.columns == ["id", "value", "note"]


def test_slice_diffs_empty_when_no_column_diff(comparison_for_slice):
    out = comparison_for_slice.slice_diffs("a", ["note"])
    assert out.height == 0


def test_slice_diffs_errors_on_invalid_table(comparison_for_slice):
    with pytest.raises(ComparisonError):
        comparison_for_slice.slice_diffs("missing", ["value"])


def test_slice_diffs_errors_on_unknown_column(comparison_for_slice):
    with pytest.raises(ComparisonError):
        comparison_for_slice.slice_diffs("a", ["unknown"])


def test_slice_diffs_errors_on_empty_column_selection(comparison_for_slice):
    with pytest.raises(ComparisonError):
        comparison_for_slice.slice_diffs("a", [])
