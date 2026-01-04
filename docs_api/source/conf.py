from __future__ import annotations

import sys
from pathlib import Path

from markupsafe import Markup

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
html_theme_options = {
    "navbar_center": [],
}
html_sidebars = {
    "**": ["sidebar-nav-pyversus.html"],
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}


def _shorten_nav_labels(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return html

    soup = BeautifulSoup(str(html), "html.parser")
    for link in soup.select("a.reference"):
        for text_node in link.find_all(string=True, recursive=True):
            if "versus." in text_node:
                text_node.replace_with(text_node.replace("versus.", ""))
    return Markup(str(soup))


def _add_nav_shortener(app, pagename, templatename, context, doctree):
    context["shorten_nav_labels"] = _shorten_nav_labels


def setup(app):
    app.connect("html-page-context", _add_nav_shortener)
