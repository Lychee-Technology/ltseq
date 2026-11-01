"""
Tests for `is None` / `is not None` in group lambdas (issue #300).

`group_ordered(...).filter()`, `.derive()` and `.agg()` call the user's group
lambda on a group proxy, where Python evaluates an identity test instead of
capturing it: `g.first().b` is an expression, never `None`, so `and`, `or`,
`&`, `|` and conditional expressions used to drop the test silently. Group
lambdas are not rewritten (owner decision OD-L5, recorded in #321): every
entry point refuses such a lambda with a `TypeError` at the call that points
to `.is_null()` / `.is_not_null()`, through the check of #184/#185, also when
the test is in a helper the lambda calls by name.
"""

import ast
import functools
from pathlib import Path

import pandas as pd
import pytest

from ltseq import LTSeq

GUIDANCE = r"compares a value with None using 'is' / 'is not'"


@pytest.fixture
def groups():
    t = LTSeq.from_pandas(
        pd.DataFrame(
            {
                "k": [1, 1, 2, 2, 3],
                "b": [None, "x", "y", "z", None],
                "a": [1, 2, 3, 4, 5],
            }
        )
    ).sort("a")
    return t.group_ordered(lambda r: r.k)


def _not_null(x):
    return x is not None


def _first_b_is_null(g):
    return g.first().b is None


# Each entry point, with a group expression of the shape it accepts: a
# predicate for filter, a value for derive and agg.
ENTRY_POINTS = {
    "filter": (
        lambda groups, fn: groups.filter(fn),
        lambda g: g.count() > 0,
    ),
    "derive": (
        lambda groups, fn: groups.derive(lambda g: {"n": fn(g)}),
        lambda g: g.count(),
    ),
    "agg": (
        lambda groups, fn: groups.agg(lambda g: {"n": fn(g)}),
        lambda g: g.count(),
    ),
}

# The spellings of the issue's acceptance criteria, with the test on either
# side of the operator.
SPELLINGS = {
    "direct": lambda key: lambda g: g.first().b is not None,
    "and": lambda key: lambda g: (g.first().b is not None) and key(g),
    "and_right": lambda key: lambda g: key(g) and (g.first().b is not None),
    "or": lambda key: lambda g: (g.first().b is None) or key(g),
    "or_right": lambda key: lambda g: key(g) or (g.first().b is None),
    "bitand_left": lambda key: lambda g: (g.first().b is not None) & key(g),
    "bitand_right": lambda key: lambda g: key(g) & (g.first().b is not None),
    "bitor_left": lambda key: lambda g: (g.last().b is None) | key(g),
    "bitor_right": lambda key: lambda g: key(g) | (g.last().b is None),
    "conditional": lambda key: lambda g: key(g) if g.first().b is not None else key(g),
    "reversed": lambda key: lambda g: (None is not g.first().b) and key(g),
    "helper": lambda key: lambda g: _not_null(g.first().b) and key(g),
    "helper_right": lambda key: lambda g: key(g) & _not_null(g.first().b),
    "helper_on_group": lambda key: lambda g: _first_b_is_null(g) or key(g),
}


def _call(entry, groups, spelling):
    # derive and agg call the spelling from the dict lambda they are given,
    # as a closure variable; TestIssueReproduction writes the test in place.
    call, key = ENTRY_POINTS[entry]
    return call(groups, SPELLINGS[spelling](key))


class TestEveryEntryPointRejects:
    @pytest.mark.parametrize("spelling", SPELLINGS)
    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_null_identity_test_raises_at_the_call(self, groups, entry, spelling):
        with pytest.raises(TypeError, match=GUIDANCE):
            _call(entry, groups, spelling)

    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_message_points_to_the_null_methods(self, groups, entry):
        with pytest.raises(TypeError) as info:
            _call(entry, groups, "and")
        message = str(info.value)
        assert "Group lambdas" in message
        assert "g.first().col.is_null()" in message
        assert "g.first().col.is_not_null()" in message

    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_helper_is_named(self, groups, entry):
        named = r"'_not_null' \(reached by name from '.*<lambda>'\) "
        with pytest.raises(TypeError, match=named + GUIDANCE):
            _call(entry, groups, "helper")

    @pytest.mark.parametrize("entry", ENTRY_POINTS)
    def test_same_lambda_without_the_null_test_runs(self, groups, entry):
        # Control: the key the rejected spellings wrap is accepted on its
        # own, so the rejection comes from the null test alone.
        call, key = ENTRY_POINTS[entry]
        expected = {"filter": 5, "derive": 5, "agg": 3}[entry]
        assert len(call(groups, key)) == expected


class TestIssueReproduction:
    """The reproduction of the issue body, written in place: each used to
    return a result with the null test dropped."""

    def test_filter(self, groups):
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.filter(lambda g: (g.first().b is not None) and (g.count() > 0))
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.filter(lambda g: (g.count() > 0) & (g.first().b is not None))

    def test_agg(self, groups):
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.agg(lambda g: {"n": (g.first().b is not None) and g.count()})

    def test_derive(self, groups):
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.derive(lambda g: {"f": (g.first().b is not None) and g.count()})


class TestNullMethodsKeepWorking:
    def test_filter(self, groups):
        kept = groups.filter(lambda g: g.first().b.is_not_null() & (g.count() > 0))
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]
        kept = groups.filter(lambda g: g.first().b.is_null())
        assert sorted(kept.flatten().to_pandas()["a"]) == [1, 2, 5]
        kept = groups.filter(lambda g: g.all(lambda r: r.b.is_not_null()))
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]

    def test_filter_equality_with_none(self, groups):
        kept = groups.filter(lambda g: (g.first().b != None) & (g.count() > 0))  # noqa: E711
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]

    def test_derive(self, groups):
        out = groups.derive(lambda g: {"n": g.count()}).filter(lambda r: r.b.is_null())
        assert sorted(out.to_pandas()["a"]) == [1, 5]

    def test_agg(self, groups):
        out = groups.agg(lambda g: {"first_b": g.first().b, "n": g.count()})
        assert out.to_pandas()["n"].tolist() == [2, 2, 1]


class TestEveryCallableFormRejects:
    """The check reads bytecode, so it needs no source and sees through
    wrappers, nested lambdas and quantifier row lambdas."""

    def test_lambda_without_source(self, groups):
        fn = eval("lambda g: (g.first().b is not None) and (g.count() > 0)")
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.filter(fn)

    def test_plain_def_with_branch(self, groups):
        def pred(g):
            if g.first().b is None:
                return g.count() > 1
            return g.count() > 0

        with pytest.raises(TypeError, match=r"'.*pred' compares a value with None"):
            groups.filter(pred)

    def test_partial(self, groups):
        def mapper(col, g):
            return {"n": (getattr(g.first(), col) is not None) and g.count()}

        with pytest.raises(TypeError, match=GUIDANCE):
            groups.derive(functools.partial(mapper, "b"))

    def test_nested_lambda(self, groups):
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.filter(
                lambda g: (lambda c: c is not None)(g.first().b) and (g.count() > 0)
            )

    def test_quantifier_row_lambda(self, groups):
        # The row lambda of g.all/any/none is part of the group lambda's
        # code, and no form is partly rewritten: it is refused with the rest.
        with pytest.raises(TypeError, match=GUIDANCE):
            groups.filter(lambda g: g.all(lambda r: r.b is not None))


class TestBoundary:
    def test_python_value_test_is_rejected_too(self, groups):
        # Without rewriting, a test on a Python value cannot be told apart
        # from one on a group value; the message says to test it outside.
        th = None
        with pytest.raises(TypeError, match=r"before\s+building the lambda"):
            groups.filter(lambda g: (g.count() > 0) if th is None else (g.count() > th))

    def test_identity_test_between_other_values_runs(self, groups):
        exact = object()
        mode = exact
        kept = groups.filter(lambda g: (g.count() > 1) if mode is exact else (g.count() > 0))
        assert sorted(kept.flatten().to_pandas()["a"]) == [1, 2, 3, 4]

    def test_none_default_argument_runs(self, groups):
        out = groups.agg(lambda g, unused=None: {"n": g.count()})
        assert out.to_pandas()["n"].tolist() == [2, 2, 1]


def _calls_named(package: Path, name: str) -> list[tuple[str, ast.Call, ast.AST | None]]:
    """Every call of ``name`` in ``package``, with the node it is an
    argument of (the enclosing call, or ``None``)."""
    found = []
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
                if called == name:
                    found.append((path.relative_to(package).as_posix(), node, parents.get(node)))
    return found


@pytest.mark.parametrize("proxy", ["FilterGroupProxy", "DeriveGroupProxy"])
def test_group_proxies_reach_user_lambdas_only_through_the_capture_helper(proxy):
    """A group proxy is built only as the argument of `_invoke_group_lambda`,
    which applies the null identity check; an API that called a group lambda
    on a proxy of its own would bypass it."""
    package = Path(__file__).resolve().parents[1] / "ltseq"
    sites = _calls_named(package, proxy)
    assert sites, f"no construction of {proxy} found"
    for path, node, parent in sites:
        assert isinstance(parent, ast.Call) and node in parent.args, (path, node.lineno)
        func = parent.func
        called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        assert called == "_invoke_group_lambda", (path, node.lineno)
