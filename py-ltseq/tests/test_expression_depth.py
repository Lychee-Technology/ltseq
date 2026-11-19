"""Expressions too deep to plan raise instead of crashing (#335).

Planning an expression recursed once per level of it with no limit and no
stack growth, so a few hundred levels killed the interpreter on a 2 MiB
thread in a debug build, and fewer on a smaller one. An expression may now
be at most 1000 levels deep (``MAX_EXPR_DEPTH`` in ``src/stack.rs``); a
deeper one raises ``ValueError`` while it is deserialized, and one within
the limit plans and runs on any thread.

Python serializes a lambda's expression recursively, so a test that builds
one 1000 levels deep raises the recursion limit. A crash kills the process
it happens in, so the small-thread cases run in a subprocess.
"""

import subprocess
import sys
import textwrap

import pytest

from ltseq import LTSeq, rand

LIMIT = 1000
TOO_DEEP = "nested more than 1000 levels deep"
# Deep enough to crash a 2 MiB thread before #335 (300 levels did in a debug
# build), and shallow enough to plan quickly in one: planning time grows
# faster than the depth, and 1000 levels take seconds per operation.
DEEP = 500


@pytest.fixture
def deep_recursion():
    previous = sys.getrecursionlimit()
    sys.setrecursionlimit(max(previous, 20_000))
    yield
    sys.setrecursionlimit(previous)


def sum_of_depth(depth):
    """``r.a + 1 + 1 + ...``, ``depth`` levels deep."""

    def build(r):
        e = r.a
        for _ in range(depth - 1):
            e = e + 1
        return e

    return build


def predicate_of_depth(depth):
    """``(r.a > 0) | (r.a == 0) | (r.a == 1) | ...``, ``depth`` levels deep."""

    def build(r):
        e = r.a > 0
        for i in range(depth - 2):
            e = e | (r.a == i)
        return e

    return build


def nested_dict(depth):
    """The serialized ``r.a + 1 + 1 + ...``, ``depth`` levels deep, built
    without recursion."""
    one = {"type": "Literal", "dtype": "Int64", "value": 1}
    node = {"type": "Column", "name": "a"}
    for _ in range(depth - 1):
        node = {"type": "BinOp", "op": "Add", "left": node, "right": one}
    return node


def run_on_a_thread(stack, body):
    """Run ``body`` on a thread of ``stack`` bytes, for the subprocess
    scripts below. Returns the tracebacks of what escaped it, or a note that
    it did not finish: an exception ends only the thread, and the script
    must not then report ``ok`` (R2-F3 of #335's review)."""
    import threading
    import traceback

    escaped = []
    finished = []

    def target():
        try:
            body()
        except BaseException:
            escaped.append(traceback.format_exc())
        else:
            finished.append(True)

    threading.stack_size(stack)
    try:
        thread = threading.Thread(target=target)
        thread.start()
        thread.join()
    finally:
        threading.stack_size(0)
    return escaped or ([] if finished else ["the thread did not finish"])


def test_run_on_a_thread_reports_what_escapes():
    def fail():
        raise RuntimeError("boom")

    (escaped,) = run_on_a_thread(256 << 10, fail)
    assert "RuntimeError: boom" in escaped
    assert run_on_a_thread(256 << 10, lambda: None) == []


def test_expression_at_the_limit_plans_and_runs(deep_recursion):
    t = LTSeq.from_dict({"a": [1, 2]})
    out = t.derive(s=sum_of_depth(LIMIT)).to_arrow()
    assert out.column("s").to_pylist() == [LIMIT, LIMIT + 1]


@pytest.mark.parametrize(
    "op",
    [
        lambda t, depth: t.derive(s=sum_of_depth(depth)),
        lambda t, depth: t.select(sum_of_depth(depth)),
        lambda t, depth: t.filter(predicate_of_depth(depth)),
        lambda t, depth: t.sort(sum_of_depth(depth)),
        lambda t, depth: t.update(predicate_of_depth(depth), a=0),
        lambda t, depth: t.agg(by=sum_of_depth(depth), n=lambda r: r.a.sum()),
        lambda t, depth: t.sort("a").derive(
            s=lambda r: sum_of_depth(depth - 1)(r) + r.a.shift(1)
        ),
        lambda t, depth: t.sort("a").group_ordered(sum_of_depth(depth)).flatten(),
    ],
    ids=["derive", "select", "filter", "sort", "update", "agg", "window", "group_ordered"],
)
def test_one_level_more_raises(deep_recursion, op):
    t = LTSeq.from_dict({"a": [1, 2]})
    with pytest.raises(ValueError, match=TOO_DEEP):
        op(t, LIMIT + 1)


def test_group_predicate_one_level_more_raises(deep_recursion):
    def predicate(g):
        e = g.count() > 0
        for i in range(LIMIT - 1):
            e = e | (g.count() == i)
        return e

    grouped = LTSeq.from_dict({"a": [1, 2], "b": [1, 1]}).sort("a").group_ordered(
        lambda r: r.b
    )
    with pytest.raises(ValueError, match=TOO_DEEP):
        grouped.filter(predicate)


@pytest.mark.parametrize("depth", [LIMIT + 1, 100_000])
def test_deserializer_refuses_a_deeper_dict_without_recursing(depth):
    t = LTSeq.from_dict({"a": [1, 2]})
    with pytest.raises(ValueError, match=TOO_DEEP):
        t._inner.derive({"s": nested_dict(depth)})


SMALL_THREAD = textwrap.dedent(
    """
    import sys
    sys.path[:0] = [{tests!r}]
    from test_expression_depth import (
        DEEP, LIMIT, TOO_DEEP, predicate_of_depth, run_on_a_thread, sum_of_depth,
    )
    from ltseq import LTSeq

    sys.setrecursionlimit(20_000)
    failures = []

    def check(name, run, depth):
        try:
            run(depth)
        except ValueError as e:
            if depth <= LIMIT or TOO_DEEP not in str(e):
                failures.append(f"{{name}} at {{depth}}: {{e}}")
        else:
            if depth > LIMIT:
                failures.append(f"{{name}} at {{depth}} did not raise")

    def gpred(depth):
        def build(g):
            e = g.count() > 0
            for i in range(depth - 2):
                e = e | (g.count() == i)
            return e
        return build

    def body():
        t = LTSeq.from_dict({{"a": [1, 2, 3], "b": [1, 1, 2]}})
        ops = {{
            "derive": lambda d: t.derive(s=sum_of_depth(d)).to_arrow(),
            "filter": lambda d: t.filter(predicate_of_depth(d)).to_arrow(),
            "window": lambda d: t.sort("a").derive(
                s=lambda r: sum_of_depth(d - 1)(r) + r.a.shift(1)
            ).to_arrow(),
            "group_filter": lambda d: t.sort("a").group_ordered(lambda r: r.b)
                .filter(gpred(d)).flatten().to_arrow(),
        }}
        for name, run in ops.items():
            for depth in (DEEP, LIMIT + 1):
                check(name, run, depth)

    failures += run_on_a_thread({stack}, body)
    print(failures or "ok")
    """
)


@pytest.mark.parametrize("stack_kib", [256, 2048])
def test_deep_expressions_on_a_small_thread(stack_kib):
    import os

    script = SMALL_THREAD.format(
        tests=os.path.dirname(os.path.abspath(__file__)), stack=stack_kib << 10
    )
    run = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=600
    )
    assert run.returncode == 0, run.stderr[-2000:]
    assert run.stdout.strip() == "ok", run.stdout


# A table whose plan holds an expression within the limit is used by every
# later operation, and an operation that takes no expression still clones,
# orders, joins, prints and drops that plan, each recursing through it.
# Before R1-F1 of #335's review these crashed a 256 KiB thread (and `slice`
# and `explain_plan` a 2 MiB one) after `derive` had accepted the
# expression. DEEP crashed them in a debug build; a release build needs
# more levels to crash, which `LTSEQ_DEPTH_TEST_LEVELS` sets.
DOWNSTREAM = textwrap.dedent(
    """
    import sys
    sys.path[:0] = [{tests!r}]
    import pyarrow as pa
    from test_expression_depth import run_on_a_thread, sum_of_depth
    from ltseq import LTSeq

    sys.setrecursionlimit(20_000)
    depth = {depth}
    failures = []

    def body():
        t = LTSeq.from_dict({{"a": [1, 2, 3], "b": [1, 1, 2]}})
        s = [depth, depth + 1, depth + 2]
        for base, d in (
            ("sorted", t.sort("a").derive(s=sum_of_depth(depth))),
            ("unsorted", t.derive(s=sum_of_depth(depth))),
        ):
            checks = {{
                "count": (lambda: d.count(), 3),
                "slice": (
                    lambda: d.slice(1, 1).to_arrow().column("s").to_pylist(),
                    [depth + 1],
                ),
                "explain_plan": (lambda: all(d.explain_plan()), True),
                "rename": (
                    lambda: d.rename(s="z").to_arrow().column("z").to_pylist(), s
                ),
                "distinct": (lambda: d.distinct().count(), 3),
                "union": (
                    lambda: sorted(d.union(d).to_arrow().column("s").to_pylist()),
                    sorted(s * 2),
                ),
                "except_": (lambda: d.except_(d).count(), 0),
                "is_subset": (lambda: d.is_subset(d), True),
                "join": (lambda: d.join(t, on=lambda x, y: x.a == y.a).count(), 3),
                "partition": (
                    lambda: sorted(p.count() for p in d.partition("b").values()),
                    [1, 2],
                ),
                "align": (
                    lambda: d.align([3, 1], key=lambda r: r.a)
                    .to_arrow().column("s").to_pylist(),
                    [depth + 2, depth],
                ),
                "pyarrow": (lambda: sorted(pa.table(d).column("s").to_pylist()), s),
            }}
            for name, (run, want) in checks.items():
                try:
                    got = run()
                except Exception as e:
                    failures.append(f"{{base}} {{name}}: {{type(e).__name__}}: {{e}}")
                else:
                    if got != want:
                        failures.append(f"{{base}} {{name}}: {{got!r}} != {{want!r}}")

    failures += run_on_a_thread(256 << 10, body)
    print(failures or "ok")
    """
)


def test_operations_on_a_deep_plan_on_a_small_thread():
    import os

    depth = int(os.environ.get("LTSEQ_DEPTH_TEST_LEVELS", DEEP))
    script = DOWNSTREAM.format(
        tests=os.path.dirname(os.path.abspath(__file__)), depth=depth
    )
    run = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=900
    )
    assert run.returncode == 0, run.stderr[-2000:]
    assert run.stdout.strip() == "ok", run.stdout[-4000:]


def sum_and_draw_of_depth(depth):
    """``r.a + 1 + ... + rand()``, ``depth`` levels deep."""

    def build(r):
        return sum_of_depth(depth - 1)(r) + rand()

    return build


@pytest.mark.parametrize(
    "op",
    [
        lambda t, depth: t.select(sum_and_draw_of_depth(depth)),
        lambda t, depth: t.agg(by=sum_and_draw_of_depth(depth), n=lambda g: g.a.count()),
    ],
    ids=["select", "agg"],
)
def test_a_staged_draw_takes_no_level_of_the_limit(deep_recursion, op):
    # R1-F2 of #335's review: staging rand() renames the output afterwards
    # rather than wrapping the expression in an alias one level deeper.
    t = LTSeq.from_dict({"a": [1, 2]})
    out = op(t, LIMIT).to_arrow()
    name = out.schema.names[0]
    assert name.endswith("+ random()") and "__ltseq_draw_" not in name
    # a + (LIMIT - 2) + one draw in [0, 1) per row.
    values = sorted(out.column(0).to_pylist())
    assert [int(v) for v in values] == [LIMIT - 1, LIMIT], values
    with pytest.raises(ValueError, match=TOO_DEEP):
        op(t, LIMIT + 1)


CYCLE = textwrap.dedent(
    """
    import sys
    sys.path[:0] = [{tests!r}]
    from ltseq import LTSeq
    from ltseq.expr.base import Expr

    class Raw(Expr):
        def __init__(self, tree):
            self.tree = tree

        def serialize(self):
            return self.tree

    t = LTSeq.from_dict({{"a": [1, 2]}})
    one = {{"type": "Literal", "dtype": "Int64", "value": 1}}
    shared = {{"type": "BinOp", "op": "Add", "left": {{"type": "Column", "name": "a"}},
              "right": one}}
    reused = {{"type": "BinOp", "op": "Add", "left": shared, "right": shared}}
    got = t.select(lambda r: Raw(reused)).to_arrow().column(0).to_pylist()
    assert got == [4, 6], got
    assert t.derive(s=lambda r: Raw(reused)).to_arrow().column("s").to_pylist() == [4, 6]

    cyclic = {{"type": "BinOp", "op": "Add", "left": {{"type": "Column", "name": "a"}}}}
    cyclic["right"] = cyclic
    # select() walks it for rand() (_draws), derive() for lookups.
    for name, op in (
        ("select", lambda: t.select(lambda r: Raw(cyclic))),
        ("lookup", lambda: t._contains_lookup(cyclic)),
    ):
        try:
            op()
        except ValueError as e:
            assert "contains itself" in str(e), (name, e)
        else:
            raise AssertionError(name + " accepted a cyclic expression")
    print("ok")
    """
)


def test_cyclic_serialized_expression_raises_and_reused_subtree_plans():
    # R1-F3 of #335's review: the iterative walks looped forever on a
    # serialized expression that contains itself. A timeout bounds a hang.
    import os

    script = CYCLE.format(tests=os.path.dirname(os.path.abspath(__file__)))
    run = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
    )
    assert run.returncode == 0, run.stderr[-2000:]
    assert run.stdout.strip() == "ok", run.stdout


# An operation parses its expressions one after another before it plans
# them. When a later one fails to parse (or a later check fails), the earlier
# ones are dropped, and dropping a deep one recurses through it: before
# R2-F1 of #335's review that drop ran on the caller's thread and crashed a
# 64 KiB one. Each case gives a valid expression at the limit, then one that
# fails; the operation must raise, and a case marked `ok` may also succeed
# (it drops its parsed expression after running). The script prints each
# case before running it, so a crash names the case.
ERROR_PATH = textwrap.dedent(
    """
    import sys
    sys.path[:0] = [{tests!r}]
    from test_expression_depth import LIMIT, nested_dict, run_on_a_thread
    from ltseq import LTSeq

    # Built here: a 64 KiB thread is too small for pyarrow's first import,
    # and for CPython to free a 1000-level dict.
    deep = nested_dict(LIMIT)
    bads = {{"unknown": {{"type": "NoSuchNode"}}, "too deep": nested_dict(LIMIT + 1)}}
    t = LTSeq.from_dict({{"a": [1, 2, 3]}})._inner
    failures = []

    def body():
        cases = []
        for kind, bad in bads.items():
            cases += [
                (f"derive, {{kind}}", lambda bad=bad: t.derive({{"s": deep, "z": bad}})),
                (
                    f"window derive, {{kind}}",
                    lambda bad=bad: t.derive_with_window_functions({{"s": deep, "z": bad}}),
                ),
                (f"select, {{kind}}", lambda bad=bad: t.select([deep, bad])),
                (
                    f"sort, {{kind}}",
                    lambda bad=bad: t.sort([deep, bad], [False, False], [True, True]),
                ),
                (f"cum_sum, {{kind}}", lambda bad=bad: t.cum_sum([deep, bad])),
                (f"agg key, {{kind}}", lambda bad=bad: t.agg(deep, {{"n": bad}})),
                (
                    f"agg value, {{kind}}",
                    lambda bad=bad: t.agg(None, {{"n": deep, "m": bad}}),
                ),
                (
                    f"join, {{kind}}",
                    lambda bad=bad: t.join(t, deep, bad, "inner", "_r"),
                ),
                (f"semi_join, {{kind}}", lambda bad=bad: t.semi_join(t, deep, bad)),
                (
                    f"search_pattern, {{kind}}",
                    lambda bad=bad: t.search_pattern([deep, bad], None),
                ),
            ]
        cases += [
            ("update values", lambda: t.conditional_update(deep, {{"no_such": 1}})),
            ("distinct key", lambda: t.distinct([deep])),
            ("intersect key", lambda: t.intersect(t, deep)),
        ]
        for name, run in cases:
            print(name, flush=True)
            try:
                run()
            except Exception:
                pass
            else:
                failures.append(f"{{name}} did not raise")
        # The parsed expressions outlive the work here: they must drop on
        # the guarded stack too.
        for name, run in (
            ("search_pattern_count", lambda: t.search_pattern_count([deep], None)),
            ("group_ordered_count", lambda: t.group_ordered_count(deep)),
        ):
            print(name, flush=True)
            try:
                run()
            except Exception:
                pass

    failures += run_on_a_thread(64 << 10, body)
    print(failures or "ok")
    """
)


def test_a_failing_expression_after_a_deep_one_raises_on_a_small_thread():
    import os

    script = ERROR_PATH.format(tests=os.path.dirname(os.path.abspath(__file__)))
    run = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=600
    )
    lines = run.stdout.strip().splitlines()
    assert run.returncode == 0, (lines[-1:], run.stderr[-2000:])
    assert lines[-1] == "ok", run.stdout[-4000:]


@pytest.mark.parametrize(
    "key, agg_name",
    [
        (lambda r: rand(), "__ltseq_draw_0"),
        (lambda r: (rand() * 2).floor(), "floor(__ltseq_draw_0 * Int64(2))"),
        (lambda r: (rand() * 2).floor(), "floor(__ltseq_draw_1 * Int64(2))"),
    ],
    ids=["bare", "named-like-the-key", "named-like-the-next-key"],
)
def test_an_aggregate_may_take_the_name_of_a_staged_key(key, agg_name):
    # R2-F2 of #335's review: the staged key is renamed after the aggregate,
    # so it must not carry an aggregate's name meanwhile.
    t = LTSeq.from_dict({"a": [1, 2, 3]})
    want = t.agg(by=key, n=lambda g: g.a.sum()).to_arrow().schema.names[0]
    out = t.agg(by=key, **{agg_name: lambda g: g.a.sum()}).to_arrow()
    assert out.schema.names == [want, agg_name]
    assert sum(out.column(agg_name).to_pylist()) == 6


@pytest.mark.parametrize(
    "other",
    ["__ltseq_draw_0 * Int64(2)", "__ltseq_draw_1 * Int64(2)"],
    ids=["named-like-the-staged-select", "named-like-the-next-staged-select"],
)
def test_a_selected_column_may_take_the_name_of_a_staged_select(other):
    # R3-F1 of #335's review: a staged expression is renamed after the
    # projection, so meanwhile it must not carry another output's name.
    t = LTSeq.from_dict({other: [3, 4]})
    out = t.select(other, lambda r: rand() * 2).to_arrow()
    assert out.schema.names == [other, "random() * Int64(2)"]
    assert out.column(other).to_pylist() == [3, 4]
    assert all(0 <= v < 2 for v in out.column(1).to_pylist())


def test_selecting_a_column_twice_beside_a_draw_still_terminates():
    # Duplicates the caller wrote are not the staging's to resolve.
    t = LTSeq.from_dict({"a": [1, 2]})
    try:
        t.select("a", "a", lambda r: rand()).to_arrow()
    except Exception:
        pass


@pytest.mark.parametrize(
    "build",
    [
        lambda x: (lambda r: [x, x]),
        lambda x: (lambda r: [x, r.a, x]),
    ],
    ids=["twice", "twice-around-a-column"],
)
def test_selecting_a_pinned_draw_twice_terminates(build):
    # R4-F1 of #335's review: both copies share the pinned draw, so no draw
    # name tells them apart; the staging must not retry forever. Run on a
    # thread with a deadline so a regression fails rather than hangs.
    import threading

    t = LTSeq.from_dict({"a": [1, 2]})
    x = rand().between(0, 1)
    outcome: dict[str, object] = {}

    def run():
        try:
            outcome["names"] = t.select(build(x)).to_arrow().schema.names
        except Exception as exc:  # the projection's own duplicate error
            outcome["error"] = exc

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=30)
    assert not worker.is_alive(), "select of a duplicated pinned draw hung"
    assert outcome
    without_draw = {}
    try:
        without_draw["names"] = t.select(lambda r: [r.a, r.a]).to_arrow().schema.names
    except Exception as exc:
        without_draw["error"] = exc
    # It behaves as a duplicate without a draw does: both fail, or both pass.
    assert ("error" in outcome) == ("error" in without_draw)
