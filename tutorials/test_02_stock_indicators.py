"""
Case 02's "Plugging in real market data" section tells readers to assign
their own data to ``stock`` in place of the sample. Running the notebook
cannot check that: a step that rereads the sample file still runs and prints
the expected output. This test replaces ``stock`` with rows the sample does
not contain, deletes the sample file, and checks that the indicators are
computed from the replacement.
"""

import json
import os
from pathlib import Path

import pytest

from ltseq import LTSeq

HERE = Path(__file__).parent


def code_cells(lang):
    path = HERE / lang / "02_stock_indicators.ipynb"
    nb = json.loads(path.read_text(encoding="utf-8"))
    return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]


@pytest.mark.parametrize("lang", ["en", "cn"])
def test_indicators_are_computed_from_stock(lang, tmp_path, monkeypatch):
    sample, *steps = code_cells(lang)
    monkeypatch.chdir(tmp_path)
    namespace = {}
    exec(sample, namespace)
    os.remove("stock.csv")
    namespace["stock"] = LTSeq.from_dict(
        {"date": ["2030-01-03", "2030-01-02", "2030-01-04"], "close": [11.0, 10.0, 12.1]}
    )
    for cell in steps:
        exec(cell, namespace)

    rows = namespace["indicators"].select("date", "close").to_dicts()
    assert rows == [
        {"date": "2030-01-02", "close": 10.0},
        {"date": "2030-01-03", "close": 11.0},
        {"date": "2030-01-04", "close": 12.1},
    ]
