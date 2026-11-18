"""Two small witnesses beside the matrix (#411, review findings R1-F3 and
R1-F6), run against whichever ltseq build is installed.

- **B1 as filtered rows.** A run-end-encoded value tested against an IN list
  of four or more items holding a NULL gives FALSE on 56 where SQL gives
  NULL. A bare filter on that test hides the fault: FALSE and NULL both drop
  the row, and 56 folds the negated filter to an empty relation. A filter
  that turns the test into a value first (`if_else`, `is_null`) keeps the
  wrong rows.
- **v0.5 §8.5's membership contract.** `is_in` is to become a membership
  test that never returns NULL: a NULL receiver is TRUE if `None` is among
  the items and FALSE otherwise, a value matching no item is FALSE even if
  `None` is among them, and an empty list is FALSE for every row. Neither
  engine gives that today (LTSeq ships SQL's three-valued `IN`, and refuses
  an empty list); these rows record the gap on both, as an existing
  contract gap, not a 56 regression. Nothing here changes shipped behavior.

    python benchmarks/datafusion56/witnesses.py --out results/witnesses-55.json
    python benchmarks/datafusion56/witnesses.py --compare results/witnesses-55.json results/witnesses-56.json
"""

from __future__ import annotations

import argparse
import json
import math

import pyarrow as pa

from ltseq import LTSeq, if_else

NAN = math.nan


def column(values, arrow_type, run_end=False):
    a = pa.array(values, arrow_type)
    if run_end:
        return pa.RunEndEncodedArray.from_arrays(pa.array(range(1, len(values) + 1), pa.int32()), a)
    return a


def table(values, arrow_type, run_end=False) -> LTSeq:
    return LTSeq.from_arrow(
        pa.table({"k": pa.array(range(len(values)), pa.int64()), "x": column(values, arrow_type, run_end)})
    )


B1_VALUES = [1, 42, None]
B1_ITEMS = [None, 1, 2, 3]

# name: (encoding, what, predicate, expected projection or expected kept rows, why)
B1 = {
    "x.is_in([None, 1, 2, 3])": ("projection", lambda r: r.x.is_in(B1_ITEMS), [True, None, None]),
    "~x.is_in([None, 1, 2, 3])": ("projection", lambda r: ~r.x.is_in(B1_ITEMS), [False, None, None]),
    "filter ~x.is_in([None, 1, 2, 3])": ("filter", lambda r: ~r.x.is_in(B1_ITEMS), []),
    "filter if_else(~x.is_in([None, 1, 2, 3]), 1, 2) == 1": (
        "filter",
        lambda r: if_else(~r.x.is_in(B1_ITEMS), 1, 2) == 1,
        [],
    ),
    "filter (~x.is_in([None, 1, 2, 3])).is_null()": ("filter", lambda r: (~r.x.is_in(B1_ITEMS)).is_null(), [1, 2]),
}

# §8.5. name: (values, type, what, predicate, today's SQL answer, §8.5's answer)
CONTRACT = {
    "NULL receiver, None among the items: is_in([None, 1])": (
        [None, 1, 7], pa.int64(), lambda r: r.x.is_in([None, 1]), [None, True, None], [True, True, False]
    ),
    "NULL receiver, no None: is_in([1, 2])": (
        [None, 1, 7], pa.int64(), lambda r: r.x.is_in([1, 2]), [None, True, False], [False, True, False]
    ),
    "nonmatching receiver, None among the items: is_in([None, 2])": (
        [None, 1, 7], pa.int64(), lambda r: r.x.is_in([None, 2]), [None, None, None], [True, False, False]
    ),
    "negated: ~is_in([None, 1])": (
        [None, 1, 7], pa.int64(), lambda r: ~r.x.is_in([None, 1]), [None, False, None], [False, False, True]
    ),
    "empty list: is_in([])": ([None, 1, 7], pa.int64(), lambda r: r.x.is_in([]), "error", [False, False, False]),
    "NaN among the items: is_in([nan, 1.0])": (
        [NAN, None, 1.0, 2.0], pa.float64(), lambda r: r.x.is_in([NAN, 1.0]), [True, None, True, False], [True, False, True, False]
    ),
}


def ask(t: LTSeq, pred):
    try:
        projection = t.derive(p=pred).to_arrow().column("p").to_pylist()
    except Exception as e:  # noqa: BLE001 - recorded, not raised
        return {"error": f"{type(e).__name__}: {str(e).splitlines()[0]}"}
    filtered = t.filter(pred)
    kept = sorted(filtered.to_arrow().column("k").to_pylist())
    logical, _ = filtered.explain_plan()
    return {"projection": projection, "kept": kept, "filter_plan": logical.strip()}


def run() -> dict:
    out = {"b1": {}, "contract": {}}
    for encoding, run_end in (("run-end", True), ("plain", False)):
        t = table(B1_VALUES, pa.int64(), run_end)
        for name, (_, pred, _) in B1.items():
            out["b1"][f"{encoding} | {name}"] = ask(t, pred)
    for name, (values, arrow_type, pred, _, _) in CONTRACT.items():
        out["contract"][name] = ask(table(values, arrow_type), pred)
    return out


def show(values) -> str:
    if not isinstance(values, list):
        return str(values)
    return "[" + ", ".join("NULL" if v is None else str(v).upper() if isinstance(v, bool) else str(v) for v in values) + "]"


def kept_of(values) -> list:
    return [k for k, v in enumerate(values) if v is True]


def cell(got: dict, what: str) -> str:
    if "error" in got:
        return f"error: `{got['error']}`"
    return show(got["projection"]) if what == "projection" else f"kept {got['kept']}"


def compare(before_path: str, after_path: str) -> str:
    with open(before_path) as f:
        before = json.load(f)
    with open(after_path) as f:
        after = json.load(f)
    bl, al = before["label"], after["label"]
    out = ["# B1 as filtered rows, and §8.5's membership contract (`witnesses.py`)\n"]
    out.append(f"## B1: run-end-encoded Int64 `x = {show(B1_VALUES)}`, IN list `{show(B1_ITEMS)}`\n")
    out.append("Expected is SQL's three-valued logic, which LTSeq ships today; a filter keeps the rows (by position `k`) whose value is TRUE. The plain column is the control.\n")
    out.append(f"| Encoding | Question | Expected | {bl} | {al} |")
    out.append("|---|---|---|---|---|")
    for key in [f"{e} | {n}" for e in ("run-end", "plain") for n in B1]:
        encoding, name = key.split(" | ", 1)
        what, _, want = B1[name]
        want_cell = show(want) if what == "projection" else f"kept {want}"
        cells = []
        for run in (before, after):
            got = run["b1"][key]
            c = cell(got, what)
            right = "error" not in got and (got["projection"] == want if what == "projection" else got["kept"] == want)
            cells.append(c if right else f"**{c}** (wrong)")
        out.append(f"| {encoding} | `{name}` | {want_cell} | {cells[0]} | {cells[1]} |")
    out.append("\nFilter plans, run-end-encoded:\n")
    for key in [f"run-end | {n}" for n in B1]:
        if key.startswith("run-end | filter"):
            for label, run in ((bl, before), (al, after)):
                got = run["b1"][key]
                if "filter_plan" in got:
                    out.append(f"`{key.split(' | ', 1)[1]}`, {label}:\n\n```text\n{got['filter_plan']}\n```\n")
    out.append("## v0.5 §8.5: `is_in` never returns NULL (not shipped)\n")
    out.append(
        "Today's shipped answer is SQL's `IN` (with NaN equal to NaN, as LTSeq compares); §8.5's is the later contract. Both engines are compared with both. "
        "Rows where an engine differs from §8.5 are existing contract gaps on 55.0.0 already, not 56 regressions; "
        "this spike changes no shipped semantics.\n"
    )
    out.append(f"| Question (Int64 `x = [NULL, 1, 7]` unless noted) | Today's SQL answer / kept | §8.5 answer / kept | {bl} | {al} | 56 against 55 |")
    out.append("|---|---|---|---|---|---|")
    for name, (values, _, _, today, future) in CONTRACT.items():
        label = f"`{name}`" if values[0] is None and len(values) == 3 else f"`{name}`, float64 `x = {show(values)}`"
        today_cell = "error" if today == "error" else f"{show(today)} / kept {kept_of(today)}"
        future_cell = f"{show(future)} / kept {kept_of(future)}"
        cells = []
        for run in (before, after):
            got = run["contract"][name]
            if "error" in got:
                cells.append(f"error: `{got['error']}`")
            else:
                tags = []
                tags.append("= SQL" if got["projection"] == today else "≠ SQL")
                tags.append("= §8.5" if got["projection"] == future and got["kept"] == kept_of(future) else "≠ §8.5")
                cells.append(f"{show(got['projection'])} / kept {got['kept']} ({', '.join(tags)})")
        same = before["contract"][name].get("projection", before["contract"][name].get("error")) == after["contract"][name].get(
            "projection", after["contract"][name].get("error")
        ) and before["contract"][name].get("kept") == after["contract"][name].get("kept")
        out.append(f"| {label} | {today_cell} | {future_cell} | {cells[0]} | {cells[1]} | {'same' if same else '**differs**'} |")
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", help="write this run's answers as JSON")
    parser.add_argument("--label", default=None, help="the run's name in a comparison, such as 55.0.0")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"), help="compare two runs as Markdown")
    args = parser.parse_args()
    if args.compare:
        print(compare(*args.compare), end="")
        return
    result = {"label": args.label, **run()}
    text = json.dumps(result, indent=1, sort_keys=True)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    main()
