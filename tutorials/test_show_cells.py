"""No tutorial cell ends in ``show(...);`` (#313).

``LTSeq.show()`` returns ``None``, so a cell that ends in ``t.show()`` shows
the table once and needs no ``;`` to stop Jupyter rendering a return value.
"""

import json
import re
from pathlib import Path

import pytest

HERE = Path(__file__).parent
NOTEBOOKS = sorted(HERE.glob("en/*.ipynb")) + sorted(HERE.glob("cn/*.ipynb"))
SHOW_WITH_SEMICOLON = re.compile(r"\.show\([^()]*\)\s*;\s*$")


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_cell_ends_in_show_semicolon(path):
    nb = json.loads(path.read_text(encoding="utf-8"))
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            assert not SHOW_WITH_SEMICOLON.search("".join(cell["source"])), "".join(cell["source"])
