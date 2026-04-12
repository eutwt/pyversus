from ._exceptions import ComparisonError


def _unsupported(name):
  raise ComparisonError(
    f"Spark backend does not expose DuckDB SQL helper `{name}`."
  )


def ident(name):
  return name


def col(alias, column):
  return f"{alias}.{column}"


def table_ref(handle):
  return handle


def select_cols(columns, alias=None):
  del alias
  return list(columns)


def join_condition(by_columns, left_alias, right_alias):
  del left_alias, right_alias
  return list(by_columns)


def inputs_join_sql(handles, table_id, by_columns):
  del handles, table_id, by_columns
  _unsupported("inputs_join_sql")


def diff_predicate(column, allow_both_na, left_alias, right_alias):
  del column, allow_both_na, left_alias, right_alias
  _unsupported("diff_predicate")


def sql_literal(value):
  return value


def run_sql(conn, sql):
  del conn, sql
  _unsupported("run_sql")


def require_diff_table(comparison):
  if comparison.diff_table is None:
    raise ComparisonError("Diff table is only available for materialize='all'.")
  return comparison.diff_table


def collect_diff_keys(comparison, columns):
  del columns
  return require_diff_table(comparison)


def fetch_rows_by_keys(comparison, table, key_sql, columns=None):
  del key_sql
  return comparison.slice_diffs(table, columns)


def select_zero_from_table(comparison, table, columns=None):
  handle = comparison._handles[table]
  if columns is None:
    return handle.frame.limit(0)
  return handle.frame.select(*list(columns)).limit(0)


__all__ = [
  "col",
  "collect_diff_keys",
  "diff_predicate",
  "fetch_rows_by_keys",
  "ident",
  "inputs_join_sql",
  "join_condition",
  "require_diff_table",
  "run_sql",
  "select_cols",
  "select_zero_from_table",
  "sql_literal",
  "table_ref",
]
