"""Differential cases against the outcomes recorded on main (#203).

Every case in ``cases/`` runs on the current build. An unlisted case must
reproduce main's outcome from ``expected/``; a case listed in
``classification.toml`` must produce its ``expect`` outcome (an intended
change or a fixed bug) or main's outcome (a known pre-existing bug). To
see two builds side by side, use ``tools/diffprobe/diffprobe.py``.
"""

import json
import tomllib
from pathlib import Path

import pytest

from . import harness

STATUSES = {"INTENDED_CHANGE", "BUG_FIXED", "PREEXISTING_BUG", "REGRESSION", "UNDECIDED"}
CLASSIFICATION = tomllib.loads(Path(__file__).with_name("classification.toml").read_text())


def _recorded():
    recorded = {}
    for path in sorted(harness.EXPECTED_DIR.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if "id" in record:
                case_id = record.pop("id")
                recorded[f"{path.stem}::{case_id}"] = record
    return recorded


RECORDED = _recorded()


def _cases():
    cases = []
    for path in harness.case_files():
        module = harness.load_module(path)
        for case_id, fn in harness.case_functions(module):
            cases.append(pytest.param(f"{path.stem}::{case_id}", fn, id=f"{path.stem}::{case_id}"))
    return cases


CASES = _cases()


def test_classification_names_known_cases():
    known = {param.values[0] for param in CASES}
    assert sorted(set(CLASSIFICATION) - known) == []
    for name, entry in CLASSIFICATION.items():
        assert entry["status"] in STATUSES, name
        assert entry.get("ref"), name
        if entry["status"] in {"INTENDED_CHANGE", "BUG_FIXED"}:
            json.loads(entry["expect"])


def test_every_case_has_a_recorded_outcome():
    assert sorted({param.values[0] for param in CASES} - set(RECORDED)) == []


@pytest.mark.parametrize("name, fn", CASES)
def test_case(name, fn):
    entry = CLASSIFICATION.get(name, {"status": "UNCHANGED"})
    status = entry["status"]
    if status in {"REGRESSION", "UNDECIDED"}:
        pytest.fail(f"{name} is classified {status}: {entry.get('note', '')}")
    if status in {"INTENDED_CHANGE", "BUG_FIXED"}:
        expected = json.loads(entry["expect"])
    else:
        expected = harness.comparable(RECORDED[name])
    assert harness.comparable(harness.run_case(fn)) == expected
