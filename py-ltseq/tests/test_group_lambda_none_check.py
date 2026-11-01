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
import inspect
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from ltseq import LTSeq, if_else

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
        assert "filter, use g.first().col.is_null() / .is_not_null()" in message
        # derive and agg have no group null test; the message says so and
        # gives the row-level replacement (TestDeriveAggNullChecks runs it).
        assert "group values have no null test" in message
        assert "r.first_b.is_not_null()" in message

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


class TestFilterNullChecks:
    """filter is the only group entry point whose group values have a null
    test: .is_null() / .is_not_null(), and == None / != None."""

    def test_null_methods(self, groups):
        kept = groups.filter(lambda g: g.first().b.is_not_null() & (g.count() > 0))
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]
        kept = groups.filter(lambda g: g.first().b.is_null())
        assert sorted(kept.flatten().to_pandas()["a"]) == [1, 2, 5]
        kept = groups.filter(lambda g: g.last().b.is_null())
        assert sorted(kept.flatten().to_pandas()["a"]) == [5]
        kept = groups.filter(lambda g: g.all(lambda r: r.b.is_not_null()))
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]
        kept = groups.filter(lambda g: g.count().is_not_null() & g.max("b").is_not_null())
        assert sorted(kept.flatten().to_pandas()["a"]) == [1, 2, 3, 4]

    def test_equality_with_none(self, groups):
        kept = groups.filter(lambda g: (g.first().b != None) & (g.count() > 0))  # noqa: E711
        assert sorted(kept.flatten().to_pandas()["a"]) == [3, 4]
        kept = groups.filter(lambda g: g.first().b == None)  # noqa: E711
        assert sorted(kept.flatten().to_pandas()["a"]) == [1, 2, 5]


MAPPERS = {
    "derive": lambda groups, fn: groups.derive(fn),
    "agg": lambda groups, fn: groups.agg(fn),
}


class TestDeriveAggNullChecks:
    """derive and agg group values have no null test (R1-F1 of PR #375):
    the guidance sends users to a row-level check on an emitted column, and
    every spelling that would look like a group null test fails loudly."""

    @pytest.mark.parametrize("method", ["is_null", "is_not_null"])
    @pytest.mark.parametrize("entry", MAPPERS)
    def test_null_methods_do_not_exist(self, groups, entry, method):
        with pytest.raises(AttributeError, match=method):
            MAPPERS[entry](groups, lambda g: {"x": getattr(g.first().b, method)()})

    @pytest.mark.parametrize(
        "spelling",
        [
            lambda g: {"n": (g.first().b != None) and g.count()},  # noqa: E711
            lambda g: {"n": g.count() if g.first().b == None else g.count()},  # noqa: E711
            lambda g: {"n": (None != g.last().b) or g.count()},  # noqa: E711
            lambda g: {"x": g.first().b == None},  # noqa: E711
            lambda g: {"x": g.sum("a") != None},  # noqa: E711
        ],
    )
    @pytest.mark.parametrize("entry", MAPPERS)
    def test_equality_with_none_raises(self, groups, entry, spelling):
        # It used to be a constant Python bool: `and` returned the counts.
        with pytest.raises(TypeError, match=r"compared with None using '[=!]='") as info:
            MAPPERS[entry](groups, spelling)
        assert "r.first_b.is_not_null()" in str(info.value)

    def test_other_comparisons_keep_their_behavior(self):
        # Only a comparison with None changed: equality with anything else
        # is still Python's identity fallback, and group values stay hashable.
        from ltseq.grouping.proxies.derive_proxy import DeriveGroupProxy

        g = DeriveGroupProxy()
        count = g.count()
        assert count == count
        assert count != g.count()
        assert (count == 3) is False
        assert count in {count}

    def test_derive_replacement(self, groups):
        out = (
            groups.derive(lambda g: {"first_b": g.first().b, "n": g.count()})
            .derive(lambda r: {"n_if_b": if_else(r.first_b.is_not_null(), r.n, None)})
            .to_pandas()
        )
        assert out["a"].tolist() == [1, 2, 3, 4, 5]
        assert out["first_b"].isna().tolist() == [True, True, False, False, True]
        assert out["n_if_b"].isna().tolist() == [True, True, False, False, True]
        assert out["n_if_b"].dropna().tolist() == [2, 2]

    def test_derive_replacement_as_a_row_filter(self, groups):
        out = groups.derive(lambda g: {"first_b": g.first().b}).filter(
            lambda r: r.first_b.is_null()
        )
        assert sorted(out.to_pandas()["a"]) == [1, 2, 5]

    def test_agg_replacement(self, groups):
        out = groups.agg(lambda g: {"first_b": g.first().b, "n": g.count()})
        kept = out.filter(lambda r: r.first_b.is_not_null()).to_pandas()
        assert kept["first_b"].tolist() == ["y"]
        assert kept["n"].tolist() == [2]
        masked = out.derive(
            lambda r: {"n_if_b": if_else(r.first_b.is_not_null(), r.n, None)}
        ).to_pandas()
        assert masked["n_if_b"].isna().tolist() == [True, False, True]
        assert masked["n_if_b"].dropna().tolist() == [2]

    @staticmethod
    def _displayed_snippets(text):
        """The multi-line examples of ``text``, each as the lines displayed.

        A snippet starts at a line opening with ``(grouped.`` and ends where
        its parentheses close. Its lines are kept verbatim, newlines and all,
        and only their common indentation is removed, as when copying the
        block into a REPL or a script, so a line continuation that Python
        cannot parse fails to compile here as it would for the user.
        """
        snippets, current, depth = [], None, 0
        for line in text.splitlines():
            if current is None and line.lstrip().startswith("(grouped."):
                current, depth = [], 0
            if current is not None:
                current.append(line)
                depth += line.count("(") - line.count(")")
                if depth == 0:
                    snippets.append(textwrap.dedent("\n".join(current)))
                    current = None
        assert current is None, "unterminated example"
        return snippets

    @staticmethod
    def _guidance_texts(groups):
        from ltseq.grouping.expr import GROUP_NULL_CHECK_MIGRATION
        from ltseq.grouping.nested_table import NestedTable

        def raised(fn):
            with pytest.raises(TypeError) as info:
                fn()
            return str(info.value)

        return {
            "constant": GROUP_NULL_CHECK_MIGRATION,
            "is None error": raised(
                lambda: groups.derive(lambda g: {"n": (g.first().b is not None) and g.count()})
            ),
            "== None error": raised(
                lambda: groups.agg(lambda g: {"n": (g.first().b != None) and g.count()})  # noqa: E711
            ),
            "derive docstring": inspect.getdoc(NestedTable.derive),
            "agg docstring": inspect.getdoc(NestedTable.agg),
        }

    @pytest.mark.parametrize(
        "source",
        ["constant", "is None error", "== None error", "derive docstring", "agg docstring"],
    )
    def test_guidance_examples_run_as_displayed(self, groups, source):
        # The replacement code in the TypeError and the docstrings is compiled
        # and run as displayed, without joining its lines (R2-F1 of PR #375).
        snippets = self._displayed_snippets(self._guidance_texts(groups)[source])
        expected = {
            "constant": ["derive", "agg"],
            "is None error": ["derive", "agg"],
            "== None error": ["derive", "agg"],
            "derive docstring": ["derive"],
            "agg docstring": ["agg"],
        }[source]
        assert [s.split("(")[1].split(".")[1] for s in snippets] == expected
        for snippet in snippets:
            assert "\n" in snippet  # a multi-line example, kept multi-line
            code = compile(snippet, f"<{source} example>", "eval")
            result = eval(code, {"grouped": groups, "if_else": if_else})
            assert len(result.to_pandas()) > 0

    def test_unparenthesized_continuation_is_caught(self):
        # The check above fails on the form it replaced: a chained call
        # continued on the next line outside parentheses does not compile.
        old = (
            "  grouped.agg(lambda g: {})\n"
            "         .filter(lambda r: r.x)\n"
        )
        with pytest.raises(SyntaxError):
            compile(textwrap.dedent(old), "<old example>", "exec")
        assert self._displayed_snippets("  (grouped.agg(lambda g: {})\n      .filter(lambda r: r.x))") == [
            "(grouped.agg(lambda g: {})\n    .filter(lambda r: r.x))"
        ]


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
