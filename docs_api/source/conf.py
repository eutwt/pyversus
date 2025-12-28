from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

project = "pyversus"
author = "versus authors"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "numpydoc",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]

default_role = "code"
autosummary_generate = True
numpydoc_show_class_members = False
maximum_signature_line_length = 88
python_maximum_signature_line_length = 88

copybutton_prompt_text = r">>> |\.\.\. "
copybutton_prompt_is_regexp = True

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_show_sourcelink = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
