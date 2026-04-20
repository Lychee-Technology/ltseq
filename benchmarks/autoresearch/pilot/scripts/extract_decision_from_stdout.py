#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path

START = "AUTORESEARCH_DECISION_BEGIN"
END = "AUTORESEARCH_DECISION_END"


def extract(text: str) -> str | None:
    pattern = re.compile(rf"{START}\n(.*?)(?:\n{END})", re.DOTALL)
    match = pattern.search(text)
    if not match:
        return None
    return match.group(1).strip() + "\n"


def main() -> int:
    if len(sys.argv) != 3:
        return 1
    stdout_path = Path(sys.argv[1])
    decision_path = Path(sys.argv[2])
    payload = extract(stdout_path.read_text(encoding="utf-8", errors="replace"))
    if not payload:
        return 1
    decision_path.write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
