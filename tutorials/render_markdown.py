#!/usr/bin/env python3
"""
Render the tutorial notebooks as Markdown
=========================================

The notebooks in ``tutorials/en`` and ``tutorials/cn`` are the source: CI
executes them. ``tutorials/markdown/<lang>/<name>.md`` is a rendering for
reading without Jupyter. Markdown cells are copied as they are, code cells
become ``python`` fences, and relative links are rewritten so they resolve
from the Markdown tree (a link to a notebook points at its Markdown page).

Edit the notebook, then rerun this script; do not edit the Markdown.

Usage:
    python tutorials/render_markdown.py           # rewrite the Markdown
    python tutorials/render_markdown.py --check   # fail if any page is stale
"""

import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
LANGS = ("en", "cn")
OUT = os.path.join(ROOT, "markdown")

# [text](target) where target is not absolute, not a URL and not an anchor.
LINK = re.compile(r"\]\((?![a-z][a-z0-9+.-]*:|/|#)([^)\s]+)\)")


def md_path(nb_path):
    lang = os.path.basename(os.path.dirname(nb_path))
    name = os.path.splitext(os.path.basename(nb_path))[0] + ".md"
    return os.path.join(OUT, lang, name)


def relink(text, nb_path, out_path):
    def fix(m):
        target, _, anchor = m.group(1).partition("#")
        resolved = os.path.normpath(os.path.join(os.path.dirname(nb_path), target))
        if resolved.endswith(".ipynb") and os.path.dirname(os.path.dirname(resolved)) == ROOT:
            resolved = md_path(resolved)
        new = os.path.relpath(resolved, os.path.dirname(out_path)).replace(os.sep, "/")
        return "](" + new + ("#" + anchor if anchor else "") + ")"

    return LINK.sub(fix, text)


def render(nb_path):
    nb_path = os.path.abspath(nb_path)
    with open(nb_path, encoding="utf-8") as f:
        nb = json.load(f)
    out_path = md_path(nb_path)
    source = os.path.relpath(nb_path, os.path.dirname(out_path)).replace(os.sep, "/")
    parts = [f"<!-- Generated from {source} by tutorials/render_markdown.py. Edit the notebook, not this file. -->"]
    for cell in nb["cells"]:
        src = "".join(cell["source"]).strip("\n")
        if cell["cell_type"] == "code":
            parts.append("```python\n" + src + "\n```")
        else:
            parts.append(relink(src, nb_path, out_path))
    return "\n\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="report stale pages instead of writing them")
    args = parser.parse_args()

    expected = {}
    for lang in LANGS:
        folder = os.path.join(ROOT, lang)
        for name in sorted(os.listdir(folder)):
            if name.endswith(".ipynb"):
                nb_path = os.path.join(folder, name)
                expected[md_path(nb_path)] = render(nb_path)

    existing = set()
    for lang in LANGS:
        folder = os.path.join(OUT, lang)
        if os.path.isdir(folder):
            existing.update(os.path.join(folder, n) for n in os.listdir(folder) if n.endswith(".md"))

    problems = []
    for path, text in expected.items():
        current = None
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                current = f.read()
        if current == text:
            continue
        if args.check:
            problems.append(("stale" if current is not None else "missing", path))
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            print("wrote", os.path.relpath(path, ROOT))
    for path in sorted(existing - set(expected)):
        if args.check:
            problems.append(("no notebook", path))
        else:
            os.remove(path)
            print("removed", os.path.relpath(path, ROOT))

    for kind, path in problems:
        print(f"{kind}: {os.path.relpath(path, ROOT)}")
    if problems:
        print("Run `python tutorials/render_markdown.py` and commit the result.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
