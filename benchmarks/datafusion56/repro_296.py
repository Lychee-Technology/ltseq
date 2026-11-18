"""#296's counterexamples as a matrix, run against whichever ltseq build is
installed (#411).

DataFusion's simplifier rewrites two membership tests of one value joined
by AND or OR into one: the union, intersection or difference of their
lists. Up to 55.0.0 it compared the items as expressions, so it lost the
NULL of `x NOT IN (NULL, ...)` (apache/datafusion#24258), took `-0.0` and
`0.0` for different items, intersected lists whose items are evaluated
only when the query runs, and merged two copies of a volatile value into
one draw (apache/datafusion#24952). This script asks today's API every
such question it can build and compares each answer with SQL's
three-valued logic computed in Python, as a projection (a derived Boolean
column) and as a filter (the rows kept), over an unsorted table and over
the same table sorted by `k`.

    python benchmarks/datafusion56/repro_296.py --label 55.0.0 --out results/repro-55.json.gz
    python benchmarks/datafusion56/repro_296.py --compare results/repro-55.json.gz results/repro-56.json.gz

A run records, for each case, whether each answer matched, and the
optimized logical plan's filter, so that a comparison of two runs can tell
a case one engine fixes (both tests still in the plan, the answer right)
from one whose plan merely changed shape.
"""

from __future__ import annotations

import argparse
import collections
import functools
import gzip
import hashlib
import itertools
import json
import math
import operator
import platform
import sys
from dataclasses import dataclass

import pyarrow as pa

from ltseq import LTSeq, if_else, rand
from ltseq.expr.core_types import LiteralExpr

NAN = math.nan


# --- Three-valued reference -------------------------------------------------


def equal(a, b):
    """`a == b` as the contract compares: NULL if either is NULL, NaN equal
    to NaN, -0.0 equal to 0.0."""
    if a is None or b is None:
        return None
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b


def member(v, items):
    """SQL's `v IN (items)`."""
    if v is None:
        return None
    answers = [equal(v, i) for i in items]
    if True in answers:
        return True
    return None if None in answers else False


def chained(v, items, literal_none):
    """`v == i1 | v == i2 | ...` (and `~(v != i1 & ...)`, the same in
    three-valued logic). The DSL reads `x == None` as `x IS NULL`, so a
    literal `None` item is a NULL test there, unlike in `is_in`."""
    out = False
    for item, is_literal_none in zip(items, literal_none):
        answer = (v is None) if is_literal_none else equal(v, item)
        out = either(out, answer)
    return out


def negation(a):
    return None if a is None else not a


def both(a, b):
    if a is False or b is False:
        return False
    return None if a is None or b is None else True


def either(a, b):
    if a is True or b is True:
        return True
    return None if a is None or b is None else False


# --- The data ---------------------------------------------------------------


@dataclass(frozen=True)
class Col:
    """A list item read from the row's column `c`, which varies by row."""


@dataclass(frozen=True)
class Staged:
    """A list item read from a column derived from the constant `value`,
    which DataFusion writes in the column's place when it moves a filter
    below the projection."""

    value: object


COL = Col()

INTS = [1, 2, None, 5, 3, 0, 7, 9]
INT_C = [1, 7, 1, None, 3, 0, 2, 9]
FLOATS = [0.0, -0.0, 1.0, None, NAN, 2.5, -0.0, 0.0, math.inf, 3.0]
FLOAT_C = [-0.0, 0.0, 0.0, 1.0, NAN, None, -0.0, 2.5, math.inf, -0.0]


def s(v):
    return None if v is None else f"s{v}"


# name: (value type, x, c, encoding of x). A dictionary- or run-end-encoded
# `x` is compared as its decoded column (#292); DataFusion's IN list over a
# run-end-encoded array is a separate code path.
TYPES = {
    "int64": (pa.int64(), INTS, INT_C, None),
    "string": (pa.string(), [s(v) for v in INTS], [s(v) for v in INT_C], None),
    "float64": (pa.float64(), FLOATS, FLOAT_C, None),
    "float32": (pa.float32(), FLOATS, FLOAT_C, None),
    "float16": (pa.float16(), FLOATS, FLOAT_C, None),
    "int64 dictionary": (pa.int64(), INTS, INT_C, "dictionary"),
    "int64 run-end": (pa.int64(), INTS, INT_C, "run_end"),
    "float64 dictionary": (pa.float64(), FLOATS, FLOAT_C, "dictionary"),
    "float64 run-end": (pa.float64(), FLOATS, FLOAT_C, "run_end"),
}

# Pairs of lists. Three items is DataFusion's threshold: it unfolds a list of
# up to three items on a column into `=`, and merges four or more `=` on a
# column back into a list.
INT_LEFT = [[1, 2, 5], [1, 2, 5, 9], [None, 1, 2, 5], [COL, 2, 10, 11], [Staged(1), 2, 10, 11]]
INT_RIGHT = [[None, 3, 4, 6], [1, 3, 4, 6], [5, 12, 13], [COL, 5, 12, 13], [Staged(1), 5, 12, 13]]
FLOAT_LEFT = [
    [0.0, 1.0, 2.5, 7.0],
    [-0.0, 1.0, 2.5],
    [None, 0.0, 1.0, 2.5],
    [COL, 1.0, 2.5, 7.0],
    [Staged(-0.0), 1.0, 2.5, 7.0],
]
FLOAT_RIGHT = [
    [None, 1.0, 2.5, 7.0],
    [-0.0, 3.0, 4.0, 5.0],
    [0.0, 3.0],
    [COL, 3.0, 4.0, 5.0],
    [Staged(0.0), 3.0, 4.0, 5.0],
]


def lists(type_name: str):
    if type_name.startswith("int64"):
        return INT_LEFT, INT_RIGHT
    if type_name == "string":
        conv = lambda items: [i if isinstance(i, Col) else Staged(s(i.value)) if isinstance(i, Staged) else s(i) for i in items]
        return [conv(x) for x in INT_LEFT], [conv(x) for x in INT_RIGHT]
    return FLOAT_LEFT, FLOAT_RIGHT


def staged_values(type_name: str) -> list:
    left, right = lists(type_name)
    # Keyed by repr: -0.0 == 0.0, so a set of the values would keep one zero.
    found = {repr(i.value): i.value for items in left + right for i in items if isinstance(i, Staged)}
    return [found[key] for key in sorted(found)]


def staged_name(value) -> str:
    return "z" + "".join(ch if ch.isalnum() else "_" for ch in repr(value))


def literal_only(items) -> bool:
    return not any(isinstance(i, (Col, Staged)) for i in items)


# --- The tests --------------------------------------------------------------

# How a membership test of `x` is written. `is_in` takes literal items only
# (#294), so a list with a column or a staged item is written as `==` joined
# by `|`, or as the negation of `!=` joined by `&`; the simplifier merges
# both into the same lists.
FORMS = ["is_in", "eq|", "~ne&"]


def item_expr(r, item):
    if isinstance(item, Col):
        return r.c
    if isinstance(item, Staged):
        return getattr(r, staged_name(item.value))
    return item


def membership(r, form: str, items):
    x = r.x
    if form == "is_in":
        return x.is_in(list(items))
    exprs = [item_expr(r, i) for i in items]
    if form == "eq|":
        return functools.reduce(operator.or_, [x == e for e in exprs])
    return ~functools.reduce(operator.and_, [x != e for e in exprs])


def item_value(item, c):
    if isinstance(item, Col):
        return c
    if isinstance(item, Staged):
        return item.value
    return item


@dataclass(frozen=True)
class Case:
    type_name: str
    left_form: str
    left: tuple
    join: str
    right_form: str
    right: tuple
    negate_right: bool
    negate_all: bool

    @property
    def name(self) -> str:
        def show(form, items):
            return f"{form}{[('c' if isinstance(i, Col) else f'z({i.value!r})' if isinstance(i, Staged) else i) for i in items]}"

        body = f"{show(self.left_form, self.left)} {self.join} {'~' * self.negate_right}{show(self.right_form, self.right)}"
        return f"{'~' * self.negate_all}({body})"

    def predicate(self, r):
        a = membership(r, self.left_form, self.left)
        b = membership(r, self.right_form, self.right)
        if self.negate_right:
            b = ~b
        joined = (a & b) if self.join == "&" else (a | b)
        return ~joined if self.negate_all else joined

    def reference(self, xs, cs) -> list:
        out = []
        def test(form, items, x, c):
            values = [item_value(i, c) for i in items]
            if form == "is_in":
                return member(x, values)
            return chained(x, values, [i is None for i in items])

        for x, c in zip(xs, cs):
            a = test(self.left_form, self.left, x, c)
            b = test(self.right_form, self.right, x, c)
            if self.negate_right:
                b = negation(b)
            joined = both(a, b) if self.join == "&" else either(a, b)
            out.append(negation(joined) if self.negate_all else joined)
        return out


def cases(type_name: str) -> list[Case]:
    left_lists, right_lists = lists(type_name)
    out = []
    for lf, left, rf, right, join, neg_r, neg_all in itertools.product(
        FORMS, left_lists, FORMS, right_lists, ["&", "|"], [False, True], [False, True]
    ):
        if (lf == "is_in" and not literal_only(left)) or (rf == "is_in" and not literal_only(right)):
            continue
        out.append(Case(type_name, lf, tuple(left), join, rf, tuple(right), neg_r, neg_all))
    return out


def column(values, arrow_type, encoding=None):
    a = pa.array(values, pa.float64() if pa.types.is_floating(arrow_type) else arrow_type).cast(arrow_type)
    if encoding == "dictionary":
        return a.dictionary_encode()
    if encoding == "run_end":
        return pa.RunEndEncodedArray.from_arrays(pa.array(range(1, len(values) + 1), pa.int32()), a)
    return a


def base_table(type_name: str, sort: bool) -> LTSeq:
    arrow_type, xs, cs, encoding = TYPES[type_name]
    t = LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array(range(len(xs)), pa.int64()),
                "x": column(xs, arrow_type, encoding),
                "c": column(cs, arrow_type),
            }
        )
    )
    t = t.derive(**{staged_name(v): (lambda r, v=v: LiteralExpr(v)) for v in staged_values(type_name)})
    return t.sort("k") if sort else t


def logical_plan(t: LTSeq) -> str:
    """The optimized logical plan, one node per line."""
    logical, _ = t.explain_plan()
    return "\n".join(line.rstrip() for line in logical.splitlines() if line.strip())


def digest(plan: str) -> str:
    return hashlib.sha256(plan.encode()).hexdigest()[:16]


# Cases whose plans a run records in full, whatever their answers: the
# rewrites #296 names, each on Int64 and on float64.
SHOWN = [
    "unsorted int64 (is_in[1, 2, 5] & ~is_in[None, 3, 4, 6])",
    "unsorted int64 (is_in[1, 2, 5, 9] & is_in[1, 3, 4, 6])",
    "unsorted int64 (eq|['c', 2, 10, 11] & eq|['c', 5, 12, 13])",
    "unsorted int64 (eq|['z(1)', 2, 10, 11] & eq|['z(1)', 5, 12, 13])",
    "unsorted string (is_in['s1', 's2', 's5'] & ~is_in[None, 's3', 's4', 's6'])",
    "unsorted float64 (eq|['z(-0.0)', 1.0, 2.5, 7.0] & eq|['z(0.0)', 3.0, 4.0, 5.0])",
    "unsorted float64 (is_in[None, 0.0, 1.0, 2.5] & ~is_in[None, 1.0, 2.5, 7.0])",
    "unsorted int64 run-end (is_in[1, 2, 5, 9] | is_in[None, 3, 4, 6])",
]


def run_matrix(chunk: int = 150) -> dict:
    results = {}
    for type_name in TYPES:
        _, xs, cs, _ = TYPES[type_name]
        all_cases = cases(type_name)
        for layout in ("unsorted", "sorted"):
            t = base_table(type_name, sort=layout == "sorted")
            # Projections, many columns per derive.
            projected = {}
            for start in range(0, len(all_cases), chunk):
                part = all_cases[start : start + chunk]
                cols = {f"q{n}": (lambda r, case=case: case.predicate(r)) for n, case in enumerate(part)}
                got = t.derive(**cols).to_arrow()
                for n, case in enumerate(part):
                    projected[case] = got.column(f"q{n}").to_pylist()
            for case in all_cases:
                want = case.reference(xs, cs)
                filtered_t = t.filter(case.predicate)
                kept = sorted(filtered_t.to_arrow().column("k").to_pylist())
                want_kept = [k for k, v in enumerate(want) if v is True]
                name = f"{layout} {type_name} {case.name}"
                ok = projected[case] == want and kept == want_kept
                plan = logical_plan(filtered_t)
                results[name] = {
                    "type": type_name,
                    "layout": layout,
                    "projection_ok": projected[case] == want,
                    "filter_ok": kept == want_kept,
                    "projection": projected[case] if projected[case] != want else None,
                    "kept": kept if kept != want_kept else None,
                    "want": want if not ok else None,
                    "plan_digest": digest(plan),
                    "plan": plan if (not ok or name in SHOWN) else None,
                }
    return results


# --- Volatile values --------------------------------------------------------

VOLATILE_ROWS = 2000


def volatile_cases() -> dict:
    """Compound tests of two separate draws of `rand()`, each tested once.
    Two draws are independent, so the share of TRUE rows is the product of
    the two tests' shares; merged into one draw, it is 0 or 1. The staged
    column, one draw for both tests, is the control: there the merge is
    right."""
    f01 = lambda: rand().round(0)  # 0.0 or 1.0
    i01 = lambda: if_else(rand() < 0.5, 0, 1)  # 0 or 1, Int64
    z = lambda: (rand() - 0.5) * 0.0  # -0.0 or 0.0
    return {
        # name: (draw, test_a, test_b, join, share with two draws, share with one)
        "float in[0,5,6,7] & in[1,5,6,7]": (f01, [0.0, 5.0, 6.0, 7.0], [1.0, 5.0, 6.0, 7.0], "&", 0.25, 0.0),
        "float in[0,5,6,7] | in[1,5,6,7]": (f01, [0.0, 5.0, 6.0, 7.0], [1.0, 5.0, 6.0, 7.0], "|", 0.75, 1.0),
        "int in[0,5,6,7] & in[1,5,6,7]": (i01, [0, 5, 6, 7], [1, 5, 6, 7], "&", 0.25, 0.0),
        "int in[0,5,6,7] | in[1,5,6,7]": (i01, [0, 5, 6, 7], [1, 5, 6, 7], "|", 0.75, 1.0),
        "int in[0,5,6,7] & ~in[0,8,9,10]": (i01, [0, 5, 6, 7], [0, 8, 9, 10], "&~", 0.25, 0.0),
        "zero in[0.0,5,6,7] & in[-0.0,5,6,7]": (z, [0.0, 5.0, 6.0, 7.0], [-0.0, 5.0, 6.0, 7.0], "&", 1.0, 1.0),
    }


def run_volatile() -> dict:
    out = {}
    base = LTSeq.from_arrow(pa.table({"k": range(VOLATILE_ROWS)}))
    for name, (draw, a, b, join, two, one) in volatile_cases().items():

        def inline(r, draw=draw, a=a, b=b, join=join):
            ta, tb = draw().is_in(a), draw().is_in(b)
            return (ta & tb) if join == "&" else (ta | tb) if join == "|" else (ta & ~tb)

        def staged(r, a=a, b=b, join=join):
            ta, tb = r.v.is_in(a), r.v.is_in(b)
            return (ta & tb) if join == "&" else (ta | tb) if join == "|" else (ta & ~tb)

        row = {"two_draws": two, "one_draw": one}
        for form, t, pred in (
            ("inline", base, inline),
            ("staged", base.derive(v=lambda r, draw=draw: draw()), staged),
        ):
            got = t.derive(p=pred).to_arrow().column("p").to_pylist()
            share = sum(1 for v in got if v is True) / len(got)
            kept = t.filter(pred).count() / VOLATILE_ROWS
            row[form] = {"projection_share": share, "filter_share": kept, "plan": logical_plan(t.filter(pred))}
        out[name] = row
    return out


def judge_volatile(row: dict, form: str) -> str:
    """Whether a share is that of two independent draws or one merged draw."""
    want = row["two_draws"] if form == "inline" else row["one_draw"]
    ok = all(abs(row[form][k] - want) <= 0.06 for k in ("projection_share", "filter_share"))
    return "ok" if ok else "WRONG"


# --- PR #402's unsorted reproduction (test_is_in_float_zero.py) -------------


def run_xfail_cases() -> dict:
    """`test_two_columns_holding_zeros_of_either_sign`, strict xfail for
    #296, on unsorted and sorted inputs. A sort reads its input as one
    stream (#268) and plans a filter pushed into it apart from the derive,
    so sorted, the two lists never meet: a pass there proves nothing."""
    out = {}
    xs = [0.0, -0.0, 1.0, None, NAN, 2.5, -0.0, 0.0, math.inf, 3.0]
    raw = pa.table({"k": pa.array(range(len(xs)), pa.int64()), "x": pa.array(xs, pa.float64())})
    want = [k for k, v in enumerate(xs) if v == 0.0]

    def any_equal(x, items):
        return functools.reduce(operator.or_, [x == i for i in items])

    for layout in ("unsorted", "sorted"):
        t = LTSeq.from_arrow(raw)
        if layout == "sorted":
            t = t.sort("k")
        t = t.derive(a=lambda r: LiteralExpr(-0.0), b=lambda r: LiteralExpr(0.0))
        kept_t = t.filter(lambda r: any_equal(r.x, [r.a, 1.0, 2.5, 7.0]) & any_equal(r.x, [r.b, 3.0, 4.0, 5.0]))
        kept = sorted(kept_t.to_arrow().column("k").to_pylist())
        out[layout] = {"kept": kept, "want": want, "ok": kept == want, "plan": logical_plan(kept_t)}
    return out


# --- Reports ----------------------------------------------------------------


def environment() -> dict:
    import ltseq

    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "pyarrow": pa.__version__,
        "ltseq": getattr(ltseq, "__file__", "?"),
    }


def summarize(matrix: dict) -> dict:
    """Wrong answers counted by type, layout and what was asked."""
    counts = collections.Counter()
    for row in matrix.values():
        key = (row["type"], row["layout"])
        counts[key + ("cases",)] += 1
        counts[key + ("projection wrong",)] += not row["projection_ok"]
        counts[key + ("filter wrong",)] += not row["filter_ok"]
    table = {}
    for (type_name, layout, what), n in counts.items():
        table.setdefault(f"{layout} {type_name}", {})[what] = n
    return table


def load(path: str) -> dict:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        return json.load(f)


def slim(run: dict) -> dict:
    """Drops what a right answer does not need: the expected values, and
    the plan unless the case is one of `SHOWN`."""
    for name, row in run["matrix"].items():
        if row["projection_ok"] and row["filter_ok"]:
            row["want"] = None
            if name not in SHOWN:
                row["plan"] = None
    return run


def ok(row: dict) -> bool:
    return row["projection_ok"] and row["filter_ok"]


def compare(before_path: str, after_path: str) -> str:
    before, after = load(before_path), load(after_path)
    lines = []
    b_names, a_names = before.get("label", before_path), after.get("label", after_path)
    lines.append(f"## Matrix: {b_names} against {a_names}\n")
    lines.append(f"| Layout and type | Cases | Projection wrong ({b_names}) | Filter wrong ({b_names}) | Projection wrong ({a_names}) | Filter wrong ({a_names}) |")
    lines.append("|---|---|---|---|---|---|")
    bs, as_ = summarize(before["matrix"]), summarize(after["matrix"])
    for key in bs:
        b, a = bs[key], as_.get(key, {})
        lines.append(
            f"| {key} | {b['cases']} | {b['projection wrong']} | {b['filter wrong']} | {a.get('projection wrong', '?')} | {a.get('filter wrong', '?')} |"
        )
    bm, am = before["matrix"], after["matrix"]
    fixed = [n for n in bm if not ok(bm[n]) and ok(am[n])]
    broken = [n for n in bm if ok(bm[n]) and not ok(am[n])]
    still = [n for n in bm if not ok(bm[n]) and not ok(am[n])]
    same_plan = [n for n in fixed if before["matrix"][n]["plan_digest"] == after["matrix"][n]["plan_digest"]]
    lines.append("")
    filter_fixed = [n for n in fixed if not bm[n]["filter_ok"]]
    filter_same = [n for n in filter_fixed if n in same_plan]
    lines.append(
        f"- Wrong under {b_names}, right under {a_names}: {len(fixed)} cases. {len(filter_fixed)} of them had a wrong filter;"
        f" the filter's optimized plan differs under {a_names} for {len(filter_fixed) - len(filter_same)}, and is the same for"
        f" {len(filter_same)}. The plan recorded is the filter's only, and over a sorted table the scan plans a filter pushed"
        " into it in a plan of its own (`TableOrder::scan`), which `explain_plan` does not show."
    )
    lines.append(f"- Right under {b_names}, wrong under {a_names}: {len(broken)} cases.")
    lines.append(f"- Wrong under both: {len(still)} cases.")
    for title, names in ((f"Right under {b_names}, wrong under {a_names}", broken), ("Wrong under both", still)):
        if names:
            kinds = collections.Counter(am[n]["type"] for n in names)
            lines.append(f"\n### {title}: by type {dict(kinds)}; the first 12\n")
            for n in names[:12]:
                lines.append(f"- `{n}`: projection {after['matrix'][n]['projection']}, kept {after['matrix'][n]['kept']}, want {after['matrix'][n]['want']}")
    lines.append(f"\n### Plans of the cases #296 names\n")
    for n in SHOWN:
        if n not in before["matrix"]:
            continue
        lines.append(f"`{n}`\n")
        for label, run in ((b_names, before), (a_names, after)):
            row = run["matrix"][n]
            verdict = "right" if row["projection_ok"] and row["filter_ok"] else f"wrong (projection {row['projection']}, kept {row['kept']}, want {row['want']})"
            lines.append(f"{label}, {verdict}:\n\n```text\n{row['plan']}\n```\n")
    lines.append("\n## Volatile values\n")
    lines.append(f"Share of TRUE rows of {VOLATILE_ROWS}, projection / filter. Inline, each test draws once and the two draws are independent; staged, both tests read one draw. A tolerance of 0.06 separates the two (about six standard deviations).\n")
    lines.append(f"| Test | Two draws | One draw | {b_names} inline | {b_names} staged | {a_names} inline | {a_names} staged |")
    lines.append("|---|---|---|---|---|---|---|")
    for name in before["volatile"]:
        cells = []
        for run in (before, after):
            row = run["volatile"][name]
            for form in ("inline", "staged"):
                r = row[form]
                cells.append(f"{r['projection_share']:.2f} / {r['filter_share']:.2f} {judge_volatile(row, form)}")
        row = before["volatile"][name]
        lines.append(f"| `{name}` | {row['two_draws']} | {row['one_draw']} | " + " | ".join(cells) + " |")
    lines.append("\n## `test_two_columns_holding_zeros_of_either_sign` (strict xfail, #296; PR #402's unsorted input)\n")
    lines.append(f"| Layout | {b_names} | {a_names} |")
    lines.append("|---|---|---|")
    for layout in ("unsorted", "sorted"):
        b, a = before["xfail"][layout], after["xfail"][layout]
        show = lambda r: "right" if r["ok"] else f"wrong: kept {r['kept']}"
        lines.append(f"| {layout} | {show(b)} | {show(a)} |")
    for layout in ("unsorted", "sorted"):
        for label, run in ((b_names, before), (a_names, after)):
            lines.append(f"\n{layout}, {label}:\n\n```text\n{run['xfail'][layout]['plan']}\n```")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", help="write this run's results as JSON")
    parser.add_argument("--label", default=None, help="the run's name in a comparison, such as 55.0.0")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"), help="compare two runs as Markdown")
    args = parser.parse_args()
    if args.compare:
        print(compare(*args.compare), end="")
        return
    run = {
        "label": args.label,
        "environment": environment(),
        "matrix": run_matrix(),
        "volatile": run_volatile(),
        "xfail": run_xfail_cases(),
    }
    for key, row in summarize(run["matrix"]).items():
        print(key, row)
    for name, row in run["volatile"].items():
        print(name, {form: judge_volatile(row, form) for form in ("inline", "staged")})
    print("xfail", {k: v["ok"] for k, v in run["xfail"].items()})
    if args.out:
        opener = gzip.open if args.out.endswith(".gz") else open
        with opener(args.out, "wt") as f:
            json.dump(slim(run), f, sort_keys=True)


if __name__ == "__main__":
    main()
