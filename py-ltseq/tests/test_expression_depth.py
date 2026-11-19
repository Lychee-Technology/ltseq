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

from ltseq import LTSeq

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
    import sys, threading
    sys.path[:0] = [{tests!r}]
    from test_expression_depth import (
        DEEP, LIMIT, TOO_DEEP, predicate_of_depth, sum_of_depth,
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

    threading.stack_size({stack})
    thread = threading.Thread(target=body)
    thread.start()
    thread.join()
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
