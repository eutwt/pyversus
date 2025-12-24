#!/usr/bin/env python3
"""Regenerate the README quick-start output from the current package state."""

from __future__ import annotations

from pathlib import Path

import duckdb

from versus import compare, examples


NOTE = (
    "# (The `column` argument only decides which diffs include a row; the returned relation\n"
    "# always keeps the full schema of the requested table.)\n"
)


def _format_block(label: str, obj: object) -> str:
    lines = str(obj).splitlines()
    body = "\n".join(f"# {line}" for line in lines)
    return f"{label}\n{body}\n\n"


def _collect_blocks() -> str:
    con = duckdb.connect()
    comp = None
    try:
        rel_a = examples.example_cars_a(con)
        rel_b = examples.example_cars_b(con)
        comp = compare(rel_a, rel_b, by=["car"], connection=con)
        sections = [
            ("comparison", comp),
            ('comparison.value_diffs("disp")', comp.value_diffs("disp")),
            (
                'comparison.value_diffs_stacked(["mpg", "disp"])',
                comp.value_diffs_stacked(["mpg", "disp"]),
            ),
            (
                'comparison.weave_diffs_wide(["mpg", "disp"])',
                comp.weave_diffs_wide(["mpg", "disp"]),
            ),
            ('comparison.weave_diffs_long("disp")', comp.weave_diffs_long("disp")),
            ('comparison.slice_diffs("a", "mpg")', comp.slice_diffs("a", "mpg")),
            ('comparison.slice_unmatched("b")', comp.slice_unmatched("b")),
            ("comparison.slice_unmatched_both()", comp.slice_unmatched_both()),
        ]
        sections.append(("comparison.summary()", comp.summary()))
        parts = []
        for label, rel in sections:
            parts.append(_format_block(label, rel))
            if label == 'comparison.slice_diffs("a", "mpg")':
                parts.append(NOTE + "\n")
        return "".join(parts).rstrip() + "\n"
    finally:
        if comp is not None:
            try:
                comp.close()
            except Exception:
                pass
        con.close()


def main() -> None:
    readme = Path("README.md")
    text = readme.read_text()
    start_marker = "comparison\n# Comparison"
    end_marker = "```\n\n## Notes"
    try:
        start = text.index(start_marker)
        end = text.index(end_marker)
    except ValueError as exc:
        raise SystemExit("README markers were not found; manual update required.") from exc
    block = _collect_blocks()
    readme.write_text(text[:start] + block + text[end:])


if __name__ == "__main__":
    main()
