import duckdb
import pytest

from versus import ComparisonError, compare


def rel_values(rel, column):
    idx = rel.columns.index(column)
    return [row[idx] for row in rel.fetchall()]


def rel_height(rel):
    return rel.aggregate("COUNT(*) AS n").fetchone()[0]


@pytest.fixture
def comparison_for_weave():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10, 1), (2, 20, 1)) AS t(id, value, wind)"),
        con.sql("SELECT * FROM (VALUES (1, 10, 2), (2, 25, 1)) AS t(id, value, wind)"),
        by=["id"],
        connection=con,
    )
    yield comp
    comp.close()
    con.close()


def test_weave_diffs_wide_has_expected_columns(comparison_for_weave):
    out = comparison_for_weave.weave_diffs_wide(["value"])
    assert {"value_a", "value_b"}.issubset(set(out.columns))


def test_weave_diffs_long_contains_both_tables(comparison_for_weave):
    out = comparison_for_weave.weave_diffs_long(["value"])
    assert set(rel_values(out, "table")) == {"a", "b"}


def test_weave_diffs_wide_accepts_custom_suffix(comparison_for_weave):
    out = comparison_for_weave.weave_diffs_wide(["value"], suffix=("_old", "_new"))
    assert {"value_old", "value_new"}.issubset(set(out.columns))


def test_weave_diffs_wide_rejects_invalid_suffix(comparison_for_weave):
    with pytest.raises(ComparisonError):
        comparison_for_weave.weave_diffs_wide(["value"], suffix=("dup", "dup"))
    with pytest.raises(ComparisonError):
        comparison_for_weave.weave_diffs_wide(["value"], suffix="oops")
    with pytest.raises(ComparisonError):
        comparison_for_weave.weave_diffs_wide(["value"], suffix=("", "_new"))


def test_weave_diffs_long_empty_when_no_differences():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (1, 10)) AS t(id, value)"),
        by=["id"],
        connection=con,
    )
    out = comp.weave_diffs_long(["value"])
    assert rel_height(out) == 0
    comp.close()
    con.close()


def test_weave_diffs_long_interleaves_rows():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (1, 11), (2, 25)) AS t(id, value)"),
        by=["id"],
        connection=con,
    )
    out = comp.weave_diffs_long(["value"])
    assert rel_values(out, "table") == ["a", "b", "a", "b"]
    assert rel_values(out, "id") == [1, 1, 2, 2]
    comp.close()
    con.close()


def test_weave_diffs_respects_custom_table_ids():
    con = duckdb.connect()
    comp = compare(
        con.sql("SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, value)"),
        con.sql("SELECT * FROM (VALUES (1, 15), (2, 20)) AS t(id, value)"),
        by=["id"],
        table_id=("original", "updated"),
        connection=con,
    )
    wide = comp.weave_diffs_wide(["value"])
    assert {"value_original", "value_updated"}.issubset(set(wide.columns))
    long = comp.weave_diffs_long(["value"])
    assert set(rel_values(long, "table")) == {"original", "updated"}
    comp.close()
    con.close()
