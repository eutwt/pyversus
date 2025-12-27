#!/usr/bin/env python3
"""Render README.md from README.qmd with Quarto."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def _resolve_quarto() -> str:
    candidate = shutil.which("quarto")
    if candidate is not None:
        return candidate
    local_quarto = (
        Path(__file__).resolve().parents[1] / ".tools" / "quarto" / "bin" / "quarto"
    )
    if local_quarto.exists():
        return str(local_quarto)
    raise SystemExit(
        "Quarto is not installed. Install it and ensure `quarto` is on PATH, "
        "or place it at .tools/quarto/bin/quarto."
    )


def main() -> None:
    quarto = _resolve_quarto()
    result = subprocess.run(
        [quarto, "render", "README.qmd", "--to", "gfm"],
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            "Quarto render failed. Ensure `quarto` is installed and R (with knitr/"
            "reticulate) is available."
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
