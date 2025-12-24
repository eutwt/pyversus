"""Example DuckDB relations for quick experimentation."""

from __future__ import annotations

from typing import Optional

import duckdb

EXAMPLE_CARS_A_SQL = """
SELECT *
FROM (
    VALUES
        ('Duster 360', 14.3, 8, 360, 245, 3.21, 3.57, 0, 0),
        ('Mazda RX4 Wag', 21.0, 6, 160, 110, 3.90, 2.88, 0, 1),
        ('Merc 230', 22.8, 4, 141, 95, 3.92, 3.15, 1, 0),
        ('Datsun 710', 22.8, NULL, 109, 93, 3.85, 2.32, 1, 1),
        ('Merc 240D', 24.4, 4, 147, 62, 3.69, 3.19, 1, 0),
        ('Hornet 4 Drive', 21.4, 6, 259, 110, 3.08, 3.22, 1, 0),
        ('Mazda RX4', 21.0, 6, 160, 110, 3.90, 2.62, 0, 1),
        ('Valiant', 18.1, 6, 225, 105, 2.76, 3.46, 1, 0),
        ('Merc 280', 19.2, 6, 168, 123, 3.92, 3.44, 1, 0)
) AS t(car, mpg, cyl, disp, hp, drat, wt, vs, am)
"""

EXAMPLE_CARS_B_SQL = """
SELECT *
FROM (
    VALUES
        ('Merc 240D', 3.19, 26.4, 62, 4, 147, 2, 3.69, 1),
        ('Valiant', 3.46, 18.1, 105, 6, 225, 1, 2.76, 1),
        ('Duster 360', 3.57, 16.3, 245, 8, 360, 4, 3.21, 0),
        ('Datsun 710', 2.32, 22.8, 93, NULL, 108, 1, 3.85, 1),
        ('Merc 280C', 3.44, 17.8, 123, 6, 168, 4, 3.92, 1),
        ('Merc 280', 3.44, 19.2, 123, 6, 168, 4, 3.92, 1),
        ('Hornet 4 Drive', 3.22, 21.4, 110, 6, 258, 1, 3.08, 1),
        ('Merc 450SE', 4.07, 16.4, 180, 8, 276, 3, 3.07, 0),
        ('Merc 230', 3.15, 22.8, 95, 4, 141, 2, 3.92, 1),
        ('Mazda RX4 Wag', 2.88, 21.0, 110, 6, 160, 4, 3.90, 0)
) AS t(car, wt, mpg, hp, cyl, disp, carb, drat, vs)
"""


def example_cars_a(
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> duckdb.DuckDBPyRelation:
    """Return the example table A as a DuckDB relation."""
    conn = _resolve_connection(connection)
    return conn.sql(EXAMPLE_CARS_A_SQL)


def example_cars_b(
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> duckdb.DuckDBPyRelation:
    """Return the example table B as a DuckDB relation."""
    conn = _resolve_connection(connection)
    return conn.sql(EXAMPLE_CARS_B_SQL)


__all__ = ["example_cars_a", "example_cars_b"]


def _resolve_connection(
    connection: Optional[duckdb.DuckDBPyConnection],
) -> duckdb.DuckDBPyConnection:
    if connection is not None:
        return connection
    candidate = getattr(duckdb, "default_connection", None)
    if candidate is not None:
        if callable(candidate):
            resolved = None
            try:
                resolved = candidate()
            except TypeError:
                resolved = None
            if resolved is not None:
                return resolved
        elif hasattr(candidate, "sql"):
            return candidate
    return duckdb.connect()
