#!/usr/bin/env python3
"""Validate integrity of autoresearch results.tsv and issues.tsv ledgers.

Run from the repository root:
    python benchmarks/autoresearch/pilot/scripts/validate_ledgers.py

Exit code 0 means all checks passed; non-zero means one or more integrity
violations were found.
"""

from __future__ import annotations

import sys
from pathlib import Path

PILOT_DIR = Path(__file__).resolve().parent.parent
RESULTS_TSV = PILOT_DIR / "results.tsv"
ISSUES_TSV = PILOT_DIR / "issues.tsv"

RESULTS_HEADER = [
    "base_ref",
    "target",
    "model_status",
    "recommendation",
    "hypothesis",
    "target_win",
    "protected_status",
    "evidence",
    "run_dir",
    "patch_path",
]

ISSUES_HEADER = [
    "id",
    "category",
    "target",
    "file",
    "title",
    "evidence",
    "suggested_fix",
    "status",
    "run_date",
]


def validate_results_tsv() -> list[str]:
    errors: list[str] = []

    if not RESULTS_TSV.exists():
        errors.append(f"results.tsv not found at {RESULTS_TSV}")
        return errors

    lines = RESULTS_TSV.read_text(encoding="utf-8").splitlines()
    if not lines:
        errors.append("results.tsv is empty")
        return errors

    # Check header
    actual_header = lines[0].split("\t")
    if actual_header != RESULTS_HEADER:
        errors.append(
            f"results.tsv header mismatch: expected {RESULTS_HEADER}, got {actual_header}"
        )

    seen_run_dirs: set[str] = set()
    for i, line in enumerate(lines[1:], start=2):
        fields = line.split("\t")
        if len(fields) != len(RESULTS_HEADER):
            errors.append(
                f"results.tsv line {i}: expected {len(RESULTS_HEADER)} fields, got {len(fields)}"
            )
            continue

        row = dict(zip(RESULTS_HEADER, fields))

        # Required non-empty fields
        for key in ("base_ref", "target", "recommendation"):
            if not row.get(key):
                errors.append(f"results.tsv line {i}: empty required field '{key}'")

        # Referential integrity: run_dir and patch_path must exist
        run_dir = row.get("run_dir", "")
        patch_path = row.get("patch_path", "")
        if run_dir:
            if run_dir in seen_run_dirs:
                errors.append(f"results.tsv line {i}: duplicate run_dir '{run_dir}'")
            seen_run_dirs.add(run_dir)
            if not Path(run_dir).exists():
                errors.append(f"results.tsv line {i}: run_dir does not exist: {run_dir}")
        if patch_path and not Path(patch_path).exists():
            errors.append(f"results.tsv line {i}: patch_path does not exist: {patch_path}")

    return errors


def validate_issues_tsv() -> list[str]:
    errors: list[str] = []

    if not ISSUES_TSV.exists():
        # issues.tsv is optional — only created if issues were recorded
        return errors

    lines = ISSUES_TSV.read_text(encoding="utf-8").splitlines()
    if not lines:
        errors.append("issues.tsv is empty")
        return errors

    # Check header
    actual_header = lines[0].split("\t")
    if actual_header != ISSUES_HEADER:
        errors.append(
            f"issues.tsv header mismatch: expected {ISSUES_HEADER}, got {actual_header}"
        )

    seen_ids: set[str] = set()
    for i, line in enumerate(lines[1:], start=2):
        fields = line.split("\t")
        if len(fields) != len(ISSUES_HEADER):
            errors.append(
                f"issues.tsv line {i}: expected {len(ISSUES_HEADER)} fields, got {len(fields)}"
            )
            continue

        row = dict(zip(ISSUES_HEADER, fields))

        # Required non-empty fields
        for key in ("id", "category", "target", "title"):
            if not row.get(key):
                errors.append(f"issues.tsv line {i}: empty required field '{key}'")

        # Deduplication check
        issue_id = row.get("id", "")
        if issue_id:
            if issue_id in seen_ids:
                errors.append(f"issues.tsv line {i}: duplicate issue id '{issue_id}'")
            seen_ids.add(issue_id)

    return errors


def main() -> None:
    all_errors: list[str] = []
    all_errors.extend(validate_results_tsv())
    all_errors.extend(validate_issues_tsv())

    if all_errors:
        print("Ledger integrity validation FAILED:")
        for err in all_errors:
            print(f"  - {err}")
        raise SystemExit(1)
    else:
        print("Ledger integrity validation passed.")
        if RESULTS_TSV.exists():
            data_lines = RESULTS_TSV.read_text(encoding="utf-8").splitlines()
            print(f"  results.tsv: {len(data_lines) - 1} run(s)")
        if ISSUES_TSV.exists():
            data_lines = ISSUES_TSV.read_text(encoding="utf-8").splitlines()
            print(f"  issues.tsv: {len(data_lines) - 1} issue(s)")
        else:
            print("  issues.tsv: not present (no issues recorded)")


if __name__ == "__main__":
    main()
