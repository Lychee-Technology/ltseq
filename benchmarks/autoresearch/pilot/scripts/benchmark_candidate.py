#!/usr/bin/env python3
"""Capture candidate benchmark artifacts for one target under reports/candidates/<target>/."""

from __future__ import annotations

from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pilot.common import capture_main
else:
    from ..common import capture_main


def main(argv: list[str] | None = None) -> int:
    return capture_main("candidate", argv)


if __name__ == "__main__":
    raise SystemExit(main())
