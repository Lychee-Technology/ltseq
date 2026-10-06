"""Scaffolding for the PR-5 literal redesign (#225).

``literal_redesign_xfail.txt`` lists the tests the branch does not pass
yet, each tagged with the phase of the design review
(https://github.com/Lychee-Technology/ltseq/pull/225#issuecomment-6007628376)
expected to fix it. They run as strict xfails, so a phase that fixes a
listed test fails the run until it removes the line. The list is empty
at the end of the redesign, and this file is deleted with it.
"""

from pathlib import Path

import pytest

_XFAIL_LIST = Path(__file__).with_name("literal_redesign_xfail.txt")


def _pending() -> dict[str, str]:
    """Node id -> phase tag, for every listed test."""
    pending = {}
    for line in _XFAIL_LIST.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        phase, node_id = line.split(maxsplit=1)
        pending[node_id] = phase
    return pending


def pytest_collection_modifyitems(config, items):
    pending = _pending()
    for item in items:
        phase = pending.get(item.nodeid)
        if phase is not None:
            item.add_marker(
                pytest.mark.xfail(
                    strict=True,
                    reason=f"literal redesign (#225): expected to pass after {phase}",
                )
            )
