"""#296's runtime-item and volatile set rewrites, asked of DataFusion itself
(#411, review finding R1-F1).

`repro_296.py` asks its questions through LTSeq, whose `is_in` takes literal
items only (#294); a column item reaches the simplifier there only as `==`
joined by `|`, which stays a chain of equalities, and LTSeq names a volatile
value once, so neither of #296's other two rewrites is reached. This script
asks them directly, in SQL, of a probe built against each engine
(`df_probe/df55`, `df_probe/df56`, one shared source and a Cargo.lock each):

- IN lists of four items (above the three-item unfolding threshold) holding
  a column, combined by AND / OR, so that 55's `inlist_intersection`,
  `inlist_except` and `inlist_union` compare a column item as an expression;
- IN lists over `CAST(random() * 10 AS BIGINT)`, written twice, so that 55
  takes the two draws for one expression. (Not `floor(random() * 10)`: 55
  also rewrites a `floor` tested against integers into range comparisons,
  each with a `random()` of its own, which would blur the shares.)

Each runtime case is asked as a projection and as a filter, and compared with
SQL's three-valued logic computed here. Each volatile case is asked over
200,000 rows; the share of TRUE rows is compared with that of two
independent draws and that of one merged draw.

    python benchmarks/datafusion56/probes.py --emit /tmp/cases.txt
    (cd benchmarks/datafusion56/df_probe/df55 && cargo run --locked -q -- /tmp/cases.txt) > results/probe-55.jsonl
    (cd benchmarks/datafusion56/df_probe/df56 && cargo run --locked -q -- /tmp/cases.txt) > results/probe-56.jsonl
    python benchmarks/datafusion56/probes.py --report results/probe-55.jsonl results/probe-56.jsonl > results/probes.md
"""

from __future__ import annotations

import argparse
import json

# --- Three-valued reference -------------------------------------------------


def equal(a, b):
    return None if a is None or b is None else a == b


def is_in(v, items):
    answers = [equal(v, i) for i in items]
    if True in answers:
        return True
    return None if None in answers else False


def neg(a):
    return None if a is None else not a


def both(a, b):
    if a is False or b is False:
        return False
    return None if a is None or b is None else True


def either(a, b):
    if a is True or b is True:
        return True
    return None if a is None or b is None else False


# --- Runtime items ----------------------------------------------------------

# (k, x, y): x is tested, y is a column item that varies by row.
ROWS = [
    (0, 1, 1),
    (1, 5, 5),
    (2, 1, 2),
    (3, 2, 1),
    (4, 5, 7),
    (5, 6, 6),
    (6, 1, None),
    (7, None, 1),
    (8, 7, 7),
    (9, 42, None),
]


def sv(v):
    return None if v is None else f"v{v}"


# id: (SQL with {x} {y} {a}..., reference(x, y, lit), what 55 does)
RUNTIME = {
    "R1 in and in": (
        "{x} IN ({y}, {5}, {50}, {500}) AND {x} IN ({1}, {6}, {60}, {600})",
        lambda x, y, L: both(is_in(x, [y, L(5), L(50), L(500)]), is_in(x, [L(1), L(6), L(60), L(600)])),
        "inlist_intersection keeps no item: FALSE",
    ),
    "R2 in and not in": (
        "{x} IN ({y}, {5}, {50}, {500}) AND {x} NOT IN ({y}, {6}, {60}, {600})",
        lambda x, y, L: both(is_in(x, [y, L(5), L(50), L(500)]), neg(is_in(x, [y, L(6), L(60), L(600)]))),
        "inlist_except removes the column item from both: x IN (5, 50, 500)",
    ),
    "R3 not in and in": (
        "{x} NOT IN ({y}, {6}, {60}, {600}) AND {x} IN ({1}, {2}, {20}, {200})",
        lambda x, y, L: both(neg(is_in(x, [y, L(6), L(60), L(600)])), is_in(x, [L(1), L(2), L(20), L(200)])),
        "inlist_except keeps the IN list whole: x IN (1, 2, 20, 200)",
    ),
    "R4 not in or not in": (
        "{x} NOT IN ({y}, {5}, {50}, {500}) OR {x} NOT IN ({1}, {6}, {60}, {600})",
        lambda x, y, L: either(neg(is_in(x, [y, L(5), L(50), L(500)])), neg(is_in(x, [L(1), L(6), L(60), L(600)]))),
        "negated inlist_intersection keeps no item: TRUE",
    ),
    "C1 not in and not in (control)": (
        "{x} NOT IN ({y}, {5}, {50}, {500}) AND {x} NOT IN ({y}, {6}, {60}, {600})",
        lambda x, y, L: both(neg(is_in(x, [y, L(5), L(50), L(500)])), neg(is_in(x, [y, L(6), L(60), L(600)]))),
        "inlist_union, a valid rewrite even with a column item",
    ),
    "C2 in and in, shared column item (control)": (
        "{x} IN ({y}, {5}, {50}, {500}) AND {x} IN ({y}, {6}, {60}, {600})",
        lambda x, y, L: both(is_in(x, [y, L(5), L(50), L(500)]), is_in(x, [y, L(6), L(60), L(600)])),
        "inlist_intersection keeps the shared column item: x = y, valid",
    ),
}

TYPES = {
    # name: (tested column, item column, literal as SQL, literal as value)
    "int64": ("x", "y", str, lambda n: n),
    "string": ("s", "u", lambda n: f"'v{n}'", lambda n: f"v{n}"),
}


def runtime_sql(case_id: str, type_name: str) -> str:
    template = RUNTIME[case_id][0]
    x, y, lit_sql, _ = TYPES[type_name]
    out = template.replace("{x}", x).replace("{y}", y)
    for n in (1, 2, 5, 6, 20, 50, 60, 200, 500, 600):
        out = out.replace("{" + str(n) + "}", lit_sql(n))
    return out


def runtime_reference(case_id: str, type_name: str) -> list:
    _, _, _, lit_value = TYPES[type_name]
    ref = RUNTIME[case_id][1]
    conv = (lambda v: v) if type_name == "int64" else sv
    return [ref(conv(x), conv(y), lit_value) for _, x, y in ROWS]


# --- Volatile values --------------------------------------------------------

VOLATILE_ROWS = 200_000
DRAW = "CAST(random() * 10 AS BIGINT)"
A = "(0, 1, 2, 3, 4)"
B = "(3, 4, 5, 6, 7)"

# id: (SQL with {f} for a draw, share with two independent draws, share with one merged draw)
VOLATILE = {
    "V1 in and in": (f"{{f}} IN {A} AND {{f}} IN {B}", 0.25, 0.2),
    "V2 in and not in": (f"{{f}} IN {A} AND {{f}} NOT IN {B}", 0.25, 0.3),
    "V3 not in and not in": (f"{{f}} NOT IN {A} AND {{f}} NOT IN {B}", 0.25, 0.2),
    "V4 not in or not in": (f"{{f}} NOT IN {A} OR {{f}} NOT IN {B}", 0.75, 0.8),
    "V5 in or in": (f"{{f}} IN {A} OR {{f}} IN {B}", 0.75, 0.8),
}
TOLERANCE = 0.01  # about ten standard deviations of a share over 200,000 rows


def emit(path: str) -> None:
    rows = ", ".join(
        f"({k}, {'NULL' if x is None else x}, {'NULL' if y is None else y}, "
        f"{'NULL' if x is None else repr(sv(x))}, {'NULL' if y is None else repr(sv(y))})"
        for k, x, y in ROWS
    )
    lines = [
        "setup\tCREATE TABLE t AS SELECT CAST(column1 AS BIGINT) AS k, CAST(column2 AS BIGINT) AS x, "
        f"CAST(column3 AS BIGINT) AS y, CAST(column4 AS VARCHAR) AS s, CAST(column5 AS VARCHAR) AS u FROM (VALUES {rows})",
        f"setup\tCREATE TABLE r AS SELECT value AS i FROM generate_series(1, {VOLATILE_ROWS})",
        # The control: one draw, stored, then tested twice.
        f"setup\tCREATE TABLE r1 AS SELECT i, {DRAW} AS v FROM r",
    ]
    for type_name in TYPES:
        for case_id in RUNTIME:
            pred = runtime_sql(case_id, type_name)
            qid = f"{type_name} | {case_id}"
            lines.append(f"query\t{qid} | projection\tSELECT k, {pred} AS p FROM t ORDER BY k")
            lines.append(f"query\t{qid} | filter\tSELECT k FROM t WHERE {pred} ORDER BY k")
    for case_id, (template, _, _) in VOLATILE.items():
        inline = template.replace("{f}", DRAW)
        staged = template.replace("{f}", "v")
        lines.append(f"query\tvolatile | {case_id} | inline | projection\tSELECT {inline} AS p FROM r")
        lines.append(f"query\tvolatile | {case_id} | inline | filter\tSELECT count(*) AS n FROM r WHERE {inline}")
        lines.append(f"query\tvolatile | {case_id} | stored | projection\tSELECT {staged} AS p FROM r1")
        lines.append(f"query\tvolatile | {case_id} | stored | filter\tSELECT count(*) AS n FROM r1 WHERE {staged}")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")


# --- Report -----------------------------------------------------------------


def load(path: str) -> dict:
    with open(path) as f:
        return {row["id"]: row for row in map(json.loads, f)}


def as_bool(cell):
    return None if cell is None else {"true": True, "false": False}[cell]


def show(values) -> str:
    return "[" + ", ".join("NULL" if v is None else str(v).upper() if isinstance(v, bool) else str(v) for v in values) + "]"


def plan_block(title: str, text: str) -> str:
    return f"{title}:\n\n```text\n{text.rstrip()}\n```\n"


def report(before_path: str, after_path: str, labels=("55.0.0", "56.0.0-rc1")) -> str:
    runs = (load(before_path), load(after_path))
    out = [
        "# #296's runtime-item and volatile rewrites, asked of DataFusion directly (`probes.py`)\n",
        "Generated by `probes.py --report` from the probe's output on each engine. "
        "The probe is `df_probe/probe.rs`, built against each engine with its own locked graph "
        "(`df_probe/df55/Cargo.lock`, `df_probe/df56/Cargo.lock`); the SQL and the expected answers are this script's.\n",
        "## Items that vary by row\n",
        "Rows `(k, x, y)`: " + ", ".join(f"({k}, {'NULL' if x is None else x}, {'NULL' if y is None else y})" for k, x, y in ROWS)
        + "; the string columns `s`, `u` hold `'v<n>'` for each value. Expected values are SQL's three-valued logic computed in Python; a filter keeps the rows whose expected value is TRUE.\n",
        f"| Case | Type | Expected projection | Kept rows expected | {labels[0]} | {labels[1]} |",
        "|---|---|---|---|---|---|",
    ]
    details = []
    for type_name in TYPES:
        for case_id, (_, _, rewrite) in RUNTIME.items():
            qid = f"{type_name} | {case_id}"
            want = runtime_reference(case_id, type_name)
            want_kept = [k for (k, _, _), v in zip(ROWS, want) if v is True]
            cells = []
            for run in runs:
                proj, filt = run[f"{qid} | projection"], run[f"{qid} | filter"]
                if "error" in proj or "error" in filt:
                    cells.append(f"error: {proj.get('error') or filt.get('error')}")
                    continue
                got = [as_bool(r[1]) for r in proj["rows"]]
                kept = [int(r[0]) for r in filt["rows"]]
                verdict = []
                verdict.append("projection right" if got == want else f"projection **wrong** {show(got)}")
                verdict.append("filter right" if kept == want_kept else f"filter **wrong**, kept {kept}")
                cells.append("; ".join(verdict))
            sql = runtime_sql(case_id, type_name)
            out.append(f"| {case_id} | {type_name} | {show(want)} | {want_kept} | {cells[0]} | {cells[1]} |")
            details.append(f"### {type_name}: {case_id}\n")
            details.append(f"`{sql}`. 55.0.0's rule: {rewrite}.\n")
            for label, run in zip(labels, runs):
                for what in ("projection", "filter"):
                    row = run[f"{qid} | {what}"]
                    if "error" in row:
                        details.append(f"{label}, {what}: error `{row['error']}`\n")
                        continue
                    details.append(plan_block(f"{label}, {what}, optimized logical plan", row["logical"]))
                details.append(plan_block(f"{label}, filter, physical plan", run[f"{qid} | filter"]["physical"]))
    out.append("")
    out.append("## Volatile values\n")
    out.append(
        f"`{DRAW}` written twice, over {VOLATILE_ROWS:,} rows; the share of rows whose test is TRUE (projection) and the share kept (filter). "
        f"Two independent draws give the first expected share, one merged draw the second; a share within {TOLERANCE} of one of them is labelled so. "
        "`stored` is the control: one draw stored in a column `v` of a table, then tested twice, where one draw is right. The draws are not seeded, so a rerun gives other shares within the tolerance.\n"
    )
    out.append(f"| Case | Form | Two draws | One draw | {labels[0]} projection / filter | {labels[1]} projection / filter |")
    out.append("|---|---|---|---|---|---|")
    vdetails = []
    for case_id, (template, two, one) in VOLATILE.items():
        for form in ("inline", "stored"):
            cells = []
            for run in runs:
                proj = run[f"volatile | {case_id} | {form} | projection"]
                filt = run[f"volatile | {case_id} | {form} | filter"]
                if "error" in proj or "error" in filt:
                    cells.append(f"error: {proj.get('error') or filt.get('error')}")
                    continue
                trues = sum(n for v, n in proj["counts"] if v == "true")
                p_share = trues / proj["n_rows"]
                f_share = int(filt["rows"][0][0]) / VOLATILE_ROWS
                cells.append(f"{p_share:.3f} ({judge(p_share, two, one)}) / {f_share:.3f} ({judge(f_share, two, one)})")
            out.append(f"| {case_id} `{template.replace('{f}', 'f').replace('|', chr(92) + '|')}` | {form} | {two} | {one} | {cells[0]} | {cells[1]} |")
        vdetails.append(f"### {case_id}, inline\n")
        for label, run in zip(labels, runs):
            for what in ("projection", "filter"):
                row = run[f"volatile | {case_id} | inline | {what}"]
                if "error" not in row:
                    vdetails.append(plan_block(f"{label}, {what}, optimized logical plan", row["logical"]))
    out.append("")
    out.append("## Plans: items that vary by row\n")
    out.extend(details)
    out.append("## Plans: volatile values\n")
    out.extend(vdetails)
    return "\n".join(out) + "\n"


def judge(share: float, two: float, one: float) -> str:
    if abs(share - two) <= TOLERANCE:
        return "two draws"
    if abs(share - one) <= TOLERANCE:
        return "one draw"
    return "neither"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--emit", metavar="CASES", help="write the probe's case file")
    parser.add_argument("--report", nargs=2, metavar=("BEFORE", "AFTER"), help="compare the probe's output on two engines as Markdown")
    args = parser.parse_args()
    if args.emit:
        emit(args.emit)
    if args.report:
        print(report(*args.report), end="")


if __name__ == "__main__":
    main()
