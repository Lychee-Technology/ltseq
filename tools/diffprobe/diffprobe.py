"""Run the differential cases under several ltseq builds (#203).

Each build is an interpreter whose ltseq is the build to probe, usually a
worktree's ``.venv/bin/python`` after ``maturin develop``. Pass the
worktree's ``py-ltseq`` directory as the build's root and the run stops
if that interpreter imports ltseq from anywhere else.

Compare builds side by side:

    python tools/diffprobe/diffprobe.py compare \\
        --build main=../main/.venv/bin/python@../main/py-ltseq \\
        --build branch=.venv/bin/python@py-ltseq [--case composition] [--filter eq]

Record a baseline for py-ltseq/tests/differential/test_differential.py
(one build; ``--commit`` is stored in the file header):

    python tools/diffprobe/diffprobe.py record \\
        --build main=../main/.venv/bin/python@../main/py-ltseq --commit 68d6114
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HARNESS_DIR = REPO / "py-ltseq" / "tests" / "differential"
WORKER = Path(__file__).with_name("worker.py")


def parse_build(text):
    label, _, rest = text.partition("=")
    python, _, root = rest.partition("@")
    if not label or not python:
        raise SystemExit(f"--build must be LABEL=PYTHON[@ROOT], got {text!r}")
    return label, python, (os.path.abspath(root) if root else None)


def run(python, root, case_file, only):
    args = [python, "-I", str(WORKER), str(HARNESS_DIR), str(case_file)]
    if only:
        args.append(only)
    proc = subprocess.run(args, capture_output=True, text=True, cwd="/")
    lines = proc.stdout.splitlines()
    if not lines:
        raise SystemExit(f"{python} produced no output for {case_file.name}:\n{proc.stderr[-2000:]}")
    imported = json.loads(lines[0])["_ltseq"]
    if root and not os.path.abspath(imported).startswith(root + os.sep):
        raise SystemExit(f"{python} imported ltseq from {imported}, not from {root}")
    records = [json.loads(line) for line in lines[1:]]
    if proc.returncode != 0:
        raise SystemExit(f"{python} failed on {case_file.name}:\n{proc.stderr[-2000:]}")
    return {record.pop("id"): record for record in records}


def show(outcome):
    if outcome is None:
        return "<missing>"
    if "error" in outcome:
        error = outcome["error"]
        return f"ERR[{error['stage']}] {error['class']}: {error['msg']}"
    result = outcome["result"]
    if isinstance(result, dict) and all(isinstance(v, dict) and "type" in v for v in result.values()):
        return "; ".join(f"{name}:{col['type']} {col['values']}" for name, col in result.items())
    return repr(result)


def case_files(selected):
    files = sorted((HARNESS_DIR / "cases").glob("*.py"))
    if selected:
        files = [f for f in files if f.stem in selected]
    return files


def compare(args):
    sys.path.insert(0, str(HARNESS_DIR))
    import harness

    builds = [parse_build(b) for b in args.build]
    for case_file in case_files(args.case):
        results = {label: run(python, root, case_file, args.filter) for label, python, root in builds}
        ids = list(dict.fromkeys(i for outcomes in results.values() for i in outcomes))
        for case_id in ids:
            outcomes = {label: results[label].get(case_id) for label in results}
            same = len({json.dumps(harness.comparable(o), sort_keys=True) if o else None for o in outcomes.values()}) == 1
            if args.changed and same:
                continue
            print(f"== {case_file.stem}::{case_id}{'   (same)' if same else ''}")
            for label, outcome in outcomes.items():
                print(f"   {label:<8} {show(outcome)}")
                if same:
                    break


def record(args):
    if len(args.build) != 1:
        raise SystemExit("record takes exactly one --build")
    label, python, root = parse_build(args.build[0])
    out_dir = HARNESS_DIR / "expected"
    out_dir.mkdir(exist_ok=True)
    for case_file in case_files(args.case):
        outcomes = run(python, root, case_file, None)
        with open(out_dir / f"{case_file.stem}.jsonl", "w") as out:
            out.write(json.dumps({"_meta": {"build": label, "commit": args.commit}}) + "\n")
            for case_id, outcome in outcomes.items():
                out.write(json.dumps({"id": case_id, **outcome}, sort_keys=True) + "\n")
        print(f"recorded {len(outcomes)} cases from {case_file.name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("compare", "record"):
        p = sub.add_parser(name)
        p.add_argument("--build", action="append", required=True, help="LABEL=PYTHON[@ROOT]")
        p.add_argument("--case", action="append", help="case module stem (repeatable)")
    sub.choices["compare"].add_argument("--filter", help="only case ids containing this text")
    sub.choices["compare"].add_argument("--changed", action="store_true", help="only cases that differ")
    sub.choices["record"].add_argument("--commit", required=True, help="the commit the build was made from")
    args = parser.parse_args()
    {"compare": compare, "record": record}[args.command](args)


if __name__ == "__main__":
    main()
