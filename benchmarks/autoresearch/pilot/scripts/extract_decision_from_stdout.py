#!/usr/bin/env python3
"""Extract and validate decision artifacts from OpenCode stdout.

Exit codes:
  0 — decision extracted and validated successfully
  1 — usage error
  2 — no decision block found in stdout (markers missing)
  3 — decision block found but empty
  4 — decision block found but missing required fields
  5 — decision block found but has malformed fields
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

START = "AUTORESEARCH_DECISION_BEGIN"
END = "AUTORESEARCH_DECISION_END"

REQUIRED_FIELDS = {"status", "scenario", "reason", "evidence"}

VALID_STATUSES = {"keep", "discard"}


def extract(text: str) -> str | None:
    """Extract the decision block between markers, or None if not found."""
    pattern = re.compile(rf"{START}\n(.*?)(?:\n{END})", re.DOTALL)
    match = pattern.search(text)
    if not match:
        return None
    return match.group(1).strip() + "\n"


def classify_payload(payload: str) -> tuple[str, str]:
    """Validate a decision payload and return (status, detail).

    status is one of: ok, empty, missing_fields, malformed
    detail explains the specific issue for non-ok statuses.
    """
    lines = [l for l in payload.strip().splitlines() if l.strip()]
    if not lines:
        return ("empty", "decision block contains no key=value lines")

    fields: dict[str, str] = {}
    for line in lines:
        if "=" not in line:
            return ("malformed", f"line lacks '=' separator: {line!r}")
        key, _, value = line.partition("=")
        fields[key.strip()] = value.strip()

    missing = REQUIRED_FIELDS - set(fields)
    if missing:
        return (
            "missing_fields",
            f"required fields absent: {', '.join(sorted(missing))}",
        )

    status_val = fields.get("status", "")
    if status_val not in VALID_STATUSES:
        return (
            "malformed",
            f"status={status_val!r} is not one of: {', '.join(sorted(VALID_STATUSES))}",
        )

    return ("ok", "")


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: extract_decision_from_stdout.py <stdout_path> <decision_path>", file=sys.stderr)
        return 1

    stdout_path = Path(sys.argv[1])
    decision_path = Path(sys.argv[2])

    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    payload = extract(text)

    if payload is None:
        print("no_decision_block", file=sys.stderr)
        return 2

    status, detail = classify_payload(payload)
    if status != "ok":
        print(f"{status}: {detail}", file=sys.stderr)
        if status == "empty":
            return 3
        if status == "missing_fields":
            return 4
        if status == "malformed":
            return 5
        return 1

    decision_path.write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
