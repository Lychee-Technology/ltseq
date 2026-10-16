"""
Tests for `is None` / `is not None` in two-table condition lambdas (issue #184).

A join condition (`on=` of join, semi_join, anti_join, asof_join, link and a
chained link) is called on a pair of proxies, where Python evaluates an
identity test instead of capturing it: a proxy column is never `None`, so
`and` / `or` / a conditional expression used to drop the test silently. The
lambda is never rewritten (a null test selects rows rather than pairing
them); every entry point refuses it with a `TypeError` at the call that
points to `filter` with `is_null()` / `is_not_null()`.
"""

import ast
import functools
from pathlib import Path

import pandas as pd
import pytest

from ltseq import LTSeq

GUIDANCE = r"compares a value with None using 'is' / 'is not'"


@pytest.fixture
def left():
    return LTSeq.from_pandas(
        pd.DataFrame({"id": [1, 2, 3], "v": [10, None, 30], "t": [1, 2, 3]})
    )


@pytest.fixture
def right():
    return LTSeq.from_pandas(
        pd.DataFrame({"id": [1, 2, 4], "x": [None, "p", None], "t": [0, 2, 4]})
    )


def _eq_key(l, r):
    return l.id == r.id


def _asof_key(l, r):
    return l.t >= r.t


# Each public entry point that takes a condition lambda, with the key shape
# it accepts (equality conjunctions, or one inequality for asof_join).
ENTRY_POINTS = {
    "join": (lambda a, b, on: a.join(b, on=on), _eq_key),
    "join_merge": (
        lambda a, b, on: a.sort("id").join(b.sort("id"), on=on, strategy="merge"),
        _eq_key,
    ),
    "semi_join": (lambda a, b, on: a.semi_join(b, on=on), _eq_key),
    "anti_join": (lambda a, b, on: a.anti_join(b, on=on), _eq_key),
    "asof_join": (lambda a, b, on: a.asof_join(b, on=on), _asof_key),
    "link": (lambda a, b, on: a.link(b, on=on, alias="b"), _eq_key),
    "chained_link": (
        lambda a, b, on: a.link(b, on=_eq_key, alias="p").link(b, on=on, alias="q"),
        _eq_key,
    ),
}

# The spellings of the issue's acceptance criteria, testing either table.
SPELLINGS = {
    "and": lambda key: lambda l, r: (r.x is not None) and key(l, r),
    "or": lambda key: lambda l, r: (r.x is None) or key(l, r),
    "bitand_left": lambda key: lambda l, r: (r.x is not None) & key(l, r),
    "bitand_right": lambda key: lambda l, r: key(l, r) & (r.x is not None),
    "conditional": lambda key: lambda l, r: key(l, r) if l.v is not None else key(l, r),
    "reversed": lambda key: lambda l, r: (None is not l.v) and key(l, r),
}


class TestEveryEntryPointRejects:
    @pytest.mark.parametrize("spelling", SPELLINGS)
    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_null_identity_test_raises_at_the_call(self, left, right, entry, spelling):
        call, key = ENTRY_POINTS[entry]
        with pytest.raises(TypeError, match=GUIDANCE):
            call(left, right, SPELLINGS[spelling](key))

    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_message_points_to_filter(self, left, right, entry):
        call, key = ENTRY_POINTS[entry]
        with pytest.raises(TypeError) as info:
            call(left, right, SPELLINGS["and"](key))
        message = str(info.value)
        assert "Join conditions support only comparisons" in message
        assert ".filter(lambda r: r.col.is_not_null())" in message
        assert ".filter(lambda r: r.col.is_null())" in message

    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_same_condition_without_the_null_test_runs(self, left, right, entry):
        # Control: the key the rejected spellings wrap is accepted on its own,
        # so the rejection comes from the null test alone.
        call, key = ENTRY_POINTS[entry]
        expected = {"anti_join": 1, "asof_join": 3}.get(entry, 2)
        assert len(call(left, right, lambda l, r: key(l, r))) == expected


class TestEveryCallableFormRejects:
    """The check reads bytecode, so it needs no source and sees through the
    wrappers row lambdas are checked through."""

    def test_lambda_without_source(self, left, right):
        fn = eval("lambda l, r: (r.x is not None) and (l.id == r.id)")
        with pytest.raises(TypeError, match=GUIDANCE):
            left.join(right, on=fn)

    def test_plain_def_with_branch(self, left, right):
        def cond(l, r):
            if r.x is None:
                return l.id == r.id
            return l.v == r.id

        with pytest.raises(TypeError, match=r"'.*cond' compares a value with None"):
            left.join(right, on=cond)

    def test_partial(self, left, right):
        def cond(col, l, r):
            return (getattr(r, col) is not None) and (l.id == r.id)

        with pytest.raises(TypeError, match=GUIDANCE):
            left.join(right, on=functools.partial(cond, "x"))

    def test_decorated_function(self, left, right):
        def passthrough(fn):
            @functools.wraps(fn)
            def wrapper(*args):
                return fn(*args)

            return wrapper

        @passthrough
        def cond(l, r):
            return (r.x is not None) and (l.id == r.id)

        with pytest.raises(TypeError, match=GUIDANCE):
            left.join(right, on=cond)

    def test_callable_instance(self, left, right):
        class Cond:
            def __call__(self, l, r):
                return (r.x is None) or (l.id == r.id)

        with pytest.raises(TypeError, match=GUIDANCE):
            left.semi_join(right, on=Cond())

    def test_nested_lambda(self, left, right):
        with pytest.raises(TypeError, match=GUIDANCE):
            left.join(
                right, on=lambda l, r: (lambda c: c is not None)(r.x) and (l.id == r.id)
            )


class TestBoundary:
    def test_python_value_test_is_rejected_too(self, left, right):
        # Without rewriting, a test on a Python value cannot be told apart
        # from one on a column; the message says to test it outside.
        th = None
        with pytest.raises(TypeError, match=r"before building the condition"):
            left.join(right, on=lambda l, r: (l.id == r.id) if th is None else (l.v == r.id))

    def test_identity_test_between_other_values_runs(self, left, right):
        exact = object()
        mode = exact
        joined = left.join(
            right, on=lambda l, r: (l.id == r.id) if mode is exact else (l.v == r.id)
        )
        assert joined.to_pandas()["id"].tolist() == [1, 2]

    def test_none_default_argument_runs(self, left, right):
        joined = left.join(right, on=lambda l, r, unused=None: l.id == r.id)
        assert joined.to_pandas()["id"].tolist() == [1, 2]


def test_only_the_capture_helpers_build_schema_proxies():
    """A user lambda reaches a SchemaProxy only through `_invoke_row_lambda`
    or `_invoke_condition_lambda`, which apply the null identity check; an
    API that built its own proxy would bypass it."""
    package = Path(__file__).resolve().parents[1] / "ltseq"
    builders = set()
    for path in sorted(package.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), str(path))):
            if isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
                if name == "SchemaProxy":
                    builders.add(path.relative_to(package).as_posix())
    assert builders == {"expr/transforms.py"}
