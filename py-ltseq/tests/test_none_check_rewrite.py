"""
Tests for the `is None` / `is not None` lambda rewrite (issue #144).

The rewrite must:
- produce the same rows as `.is_null()` / `.is_not_null()` (not `== None`);
- keep the lambda's module globals and closure cells;
- be independent of source layout (several lambdas on one line, f-strings,
  triple-quoted strings, multi-line lambdas, nested lambdas);
- keep Python semantics for operands that are not expressions;
- locate lambdas defined at module scope and in modules that use
  `from __future__` imports, not only lambdas nested in functions;
- never let the injected helper shadow a user name;
- fail closed with a guidance TypeError (never a NameError, never a silent
  wrong or partial result) when a null identity check cannot be rewritten:
  no source, a plain def, or source that no longer matches the lambda;
- see through wrappers: a functools.partial of a lambda is rewritten, and a
  check behind partial, lru_cache, staticmethod or a decorator fails closed;
- refuse, without rewriting it, a null identity check in a user function the
  lambda calls by name, directly or through other helpers; functions of
  ltseq, the standard library and installed packages are not checked (#185).
"""

import functools
import importlib.util
import os
import subprocess
import sys
import textwrap
import types
from json import loads

import pandas as pd
import pytest

from ltseq import LTSeq
from ltseq.expr.base import if_else, log
from ltseq.expr.transforms import (
    _NULL_CHECK_HELPER,
    _tests_identity_with_none,
    _transform_lambda_for_none_checks,
)

MODULE_THRESHOLD = 2

# Module-scope lambdas are compiled without CO_NESTED, unlike lambdas defined
# inside a function (which is where every other lambda in this file lives).
MODULE_PRED = lambda r: r.b is None  # noqa: E731
MODULE_PRED_WITH_GLOBAL = lambda r: (r.b is not None) & (r.a > MODULE_THRESHOLD)  # noqa: E731
MODULE_DERIVE = lambda r: {"missing": r.b is None}  # noqa: E731

# A user binding that happens to share the injected helper's name.
__ltseq_null_check__ = 5
assert _NULL_CHECK_HELPER == "__ltseq_null_check__"
MODULE_PRED_WITH_HELPER_NAME = lambda r: (r.b is None) & (__ltseq_null_check__ > 0)  # noqa: E731


# Helpers that lambdas call by name (#185). LTSeq never rewrites them, so on
# an expression `r.b is None` is the Python value False.
def missing(r):
    return r.b is None


def missing_via_helper(r):
    return missing(r)


def missing_via_two_helpers(r):
    return missing_via_helper(r)


def above_one(r):
    return r.a > 1


def apply(check, r):
    return check(r)


def ignores_check(r, check):
    return r.a > 1


# Recursive in their code, not when called: the walk of names must end.
def ping(r, depth=0):
    return pong(r, depth + 1) if depth < 1 else r.a > 2


def pong(r, depth):
    return ping(r, depth)


@pytest.fixture
def t():
    return LTSeq.from_pandas(
        pd.DataFrame(
            {
                "a": [1, 2, 3, 4],
                "b": [None, "x", None, "y"],
                "c": [10.0, None, 30.0, None],
            }
        )
    )


def rows(table):
    return table.to_pandas().to_dict("records")


def a_values(table):
    return [r["a"] for r in rows(table)]


class TestNullSemantics:
    def test_is_none_matches_is_null(self, t):
        assert a_values(t.filter(lambda r: r.b is None)) == [1, 3]
        assert a_values(t.filter(lambda r: r.b is None)) == a_values(
            t.filter(lambda r: r.b.is_null())
        )

    def test_is_not_none_matches_is_not_null(self, t):
        assert a_values(t.filter(lambda r: r.b is not None)) == [2, 4]
        assert a_values(t.filter(lambda r: r.b is not None)) == a_values(
            t.filter(lambda r: r.b.is_not_null())
        )

    def test_reversed_operands(self, t):
        assert a_values(t.filter(lambda r: None is r.b)) == [1, 3]
        assert a_values(t.filter(lambda r: None is not r.b)) == [2, 4]

    def test_numeric_column(self, t):
        assert a_values(t.filter(lambda r: r.c is None)) == [2, 4]

    def test_derive_dict_value(self, t):
        out = rows(t.derive(lambda r: {"missing": r.b is None}))
        assert [r["missing"] for r in out] == [True, False, True, False]

    def test_if_else_condition(self, t):
        out = rows(
            t.derive(
                lambda r: {
                    "label": if_else(r.b is None, "gap", "ok")  # pyright: ignore[reportArgumentType]
                }
            )
        )
        assert [r["label"] for r in out] == ["gap", "ok", "gap", "ok"]

    def test_combined_with_other_predicate(self, t):
        assert a_values(t.filter(lambda r: (r.b is not None) & (r.a > 2))) == [4]
        assert a_values(t.filter(lambda r: (r.b is None) | (r.a == 4))) == [1, 3, 4]


class TestEnvironmentPreserved:
    def test_module_global(self, t):
        assert a_values(
            t.filter(lambda r: (r.b is not None) & (r.a > MODULE_THRESHOLD))
        ) == [4]

    def test_closure_variable(self, t):
        th = 2
        assert a_values(t.filter(lambda r: (r.b is not None) & (r.a > th))) == [4]

    def test_closure_and_global_together(self, t):
        offset = 1
        pred = lambda r: (r.c is None) & (r.a > MODULE_THRESHOLD + offset)  # noqa: E731
        assert a_values(t.filter(pred)) == [4]

    def test_default_argument(self, t):
        th = 3
        assert a_values(t.filter(lambda r, k=th: (r.b is None) & (r.a >= k))) == [3]

    def test_closure_cell_is_shared_not_snapshotted(self):
        th = 1
        fn = lambda r: (r.a is not None) & (r.a > th)  # noqa: E731
        rewritten = _transform_lambda_for_none_checks(fn)
        assert rewritten is not fn
        th = 100  # the rewritten lambda must see the new binding
        from ltseq.expr import ColumnExpr

        class Proxy:
            a = ColumnExpr("a")

        expr = rewritten(Proxy()).serialize()
        assert expr["right"]["right"]["value"] == 100

    def test_non_expr_operand_keeps_python_semantics(self, t):
        th = None
        assert a_values(t.filter(lambda r: r.a > th if th is not None else r.a > 3)) == [4]
        th = 1
        assert a_values(t.filter(lambda r: r.a > th if th is not None else r.a > 3)) == [
            2,
            3,
            4,
        ]

    def test_is_between_non_none_python_values_untouched(self, t):
        sentinel = object()
        flag = sentinel
        assert a_values(t.filter(lambda r: r.b is None if flag is sentinel else r.b is not None)) == [
            1,
            3,
        ]


class TestSourceLayout:
    def test_two_lambdas_on_one_line(self, t):
        assert a_values(t.filter(lambda r: r.b is None).filter(lambda r: r.a > 1)) == [3]
        assert a_values(t.filter(lambda r: r.a > 1).filter(lambda r: r.b is not None)) == [2, 4]

    def test_two_none_lambdas_on_one_line(self, t):
        assert a_values(t.filter(lambda r: r.b is None).filter(lambda r: r.c is not None)) == [1, 3]

    def test_fstring_and_triple_quotes_on_the_line(self, t):
        label = f"gap{'(' * 2}"
        out = rows(
            t.derive(
                lambda r: {
                    "lbl": if_else(r.b is None, label, """ok)""")  # pyright: ignore[reportArgumentType]
                }
            )
        )
        assert [r["lbl"] for r in out] == ["gap((", "ok)", "gap((", "ok)"]

    def test_multiline_lambda(self, t):
        result = t.filter(
            lambda r: (r.b is not None)
            & (r.a > 1)
            & (r.a < 4)
        )
        assert a_values(result) == [2]

    def test_lambda_split_from_call(self, t):
        pred = (
            lambda r: r.b
            is None
        )
        assert a_values(t.filter(pred)) == [1, 3]

    def test_nested_lambda(self, t):
        pred = lambda r: (lambda col: col is None)(r.b)  # noqa: E731
        assert a_values(t.filter(pred)) == [1, 3]

    def test_lambda_built_in_a_loop(self, t):
        preds = [lambda r, col=col: getattr(r, col) is None for col in ("b", "c")]
        assert a_values(t.filter(preds[0])) == [1, 3]
        assert a_values(t.filter(preds[1])) == [2, 4]

    def test_lambda_inside_class_method(self, t):
        class Filters:
            def missing(self, table):
                return table.filter(lambda r: r.b is None)

        assert a_values(Filters().missing(t)) == [1, 3]


def _import_module_from_source(tmp_path, name, source):
    path = tmp_path / f"{name}.py"
    path.write_text(textwrap.dedent(source))
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[name]
        raise
    return module


class TestCompilationContext:
    """The lambda is matched by compiled identity, so it must be compiled in
    the same context as the original: module scope versus nested scope, and
    the defining module's ``from __future__`` features."""

    def test_module_scope_lambda_in_filter(self, t):
        assert a_values(t.filter(MODULE_PRED)) == [1, 3]

    def test_module_scope_lambda_with_global(self, t):
        assert a_values(t.filter(MODULE_PRED_WITH_GLOBAL)) == [4]

    def test_module_scope_lambda_in_derive(self, t):
        out = rows(t.derive(MODULE_DERIVE))
        assert [r["missing"] for r in out] == [True, False, True, False]

    def test_class_body_lambda(self, t):
        class Preds:
            missing = staticmethod(lambda r: r.b is None)

        assert a_values(t.filter(Preds.missing)) == [1, 3]

    def test_private_name_in_class_method(self, t):
        class Filters:
            def __init__(self):
                self.__threshold = 2

            def missing_above(self, table):
                # ``self.__threshold`` is mangled to ``self._Filters__threshold``
                return table.filter(lambda r: (r.b is None) & (r.a > self.__threshold))

            def missing_above_private_local(self, table):
                __th = 2
                return table.filter(lambda r: (r.b is None) & (r.a > __th))

        assert a_values(Filters().missing_above(t)) == [3]
        assert a_values(Filters().missing_above_private_local(t)) == [3]

    def test_private_name_in_nested_class_uses_innermost_class(self, t):
        class Outer:
            class Inner:
                def __init__(self):
                    self.__threshold = 2

                def missing_above(self, table):
                    return table.filter(lambda r: (r.b is None) & (r.a > self.__threshold))

        assert a_values(Outer.Inner().missing_above(t)) == [3]

    def test_module_with_future_import(self, t, tmp_path):
        mod = _import_module_from_source(
            tmp_path,
            "ltseq_none_check_future_mod",
            """
            from __future__ import annotations

            PRED = lambda r: r.b is None  # noqa: E731

            def make(th):
                return lambda r: (r.b is None) & (r.a > th)
            """,
        )
        try:
            assert a_values(t.filter(mod.PRED)) == [1, 3]
            assert a_values(t.filter(mod.make(1))) == [3]
        finally:
            del sys.modules[mod.__name__]


class TestHelperNameCollision:
    """The injected helper must not change what a user identifier resolves to."""

    def test_global_with_helper_name(self, t):
        pred = lambda r: (r.b is None) & (__ltseq_null_check__ > 0)  # noqa: E731
        assert a_values(t.filter(pred)) == [1, 3]

    def test_module_scope_lambda_with_global_helper_name(self, t):
        assert a_values(t.filter(MODULE_PRED_WITH_HELPER_NAME)) == [1, 3]

    def test_closure_with_helper_name(self, t):
        __ltseq_null_check__ = 3
        pred = lambda r: (r.b is None) & (r.a >= __ltseq_null_check__)  # noqa: E731
        assert a_values(t.filter(pred)) == [3]

    def test_parameter_with_helper_name(self, t):
        pred = lambda r, __ltseq_null_check__=3: (r.b is None) & (r.a >= __ltseq_null_check__)  # noqa: E731
        assert a_values(t.filter(pred)) == [3]

    def test_suffixed_helper_name_is_also_avoided(self, t):
        __ltseq_null_check__ = 1
        __ltseq_null_check__1 = 3
        pred = lambda r: (r.b is None) & (r.a >= __ltseq_null_check__ + __ltseq_null_check__1 - 1)  # noqa: E731
        assert a_values(t.filter(pred)) == [3]


class TestPublicBoundaries:
    """Every public API that takes ``lambda r: ...`` applies the rewrite, not
    only the ones that happen to route through ``_lambda_to_expr``."""

    def test_select_expression(self, t):
        out = rows(t.select(lambda r: r.b is None))
        assert [next(iter(r.values())) for r in out] == [True, False, True, False]
        assert out == rows(t.select(lambda r: r.b.is_null()))

    def test_select_list(self, t):
        out = rows(t.select(lambda r: [r.a, r.b is not None]))
        assert [list(r.values()) for r in out] == [
            [1, False],
            [2, True],
            [3, False],
            [4, True],
        ]

    def test_select_module_scope_lambda(self, t):
        assert rows(t.select(MODULE_PRED)) == rows(t.select(lambda r: r.b.is_null()))

    def test_select_plain_def_gives_guidance(self, t):
        def col(r):
            return r.b is None

        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.select(col)

    def test_delete_removes_null_rows(self, t):
        assert a_values(t.delete(lambda r: r.b is None)) == [2, 4]
        assert a_values(t.delete(lambda r: r.b is not None)) == [1, 3]

    def test_delete_with_closure(self, t):
        th = 2
        assert a_values(t.delete(lambda r: (r.b is not None) & (r.a > th))) == [1, 2, 3]

    def test_delete_module_scope_lambda(self, t):
        assert a_values(t.delete(MODULE_PRED)) == [2, 4]

    def test_delete_plain_def_gives_guidance(self, t):
        def pred(r):
            return r.b is None

        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.delete(pred)

    def test_delete_rejects_dict_predicate(self, t):
        with pytest.raises(TypeError, match="boolean expression"):
            t.delete(lambda r: {"x": r.a > 1})

    def test_update_predicate(self, t):
        out = rows(t.update(lambda r: r.b is None, a=0))
        assert [r["a"] for r in out] == [0, 2, 0, 4]


class TestFallbacks:
    """A null identity check that could not be rewritten fails closed with the
    guidance ``TypeError``. Run as is it would be a Python bool, and ``and`` /
    ``or`` / conditional expressions would return a partial ``Expr`` that
    filters the wrong rows without any error."""

    def test_lambda_without_source_gives_guidance(self, t):
        fn = eval("lambda r: r.b is None")
        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.filter(fn)

    def test_lambda_without_source_and_closure_never_name_errors(self, t):
        ns = {"th": 2}
        fn = eval("lambda r: (r.b is not None) & (r.a > th)", ns)
        with pytest.raises(TypeError, match=r"is_not_null\(\)"):
            t.filter(fn)

    @pytest.mark.parametrize(
        "source",
        [
            # `True and expr` -> expr: the null check vanished ([2, 3, 4], not [2, 4])
            "lambda r: (r.b is not None) and (r.a > 1)",
            # `False or expr` -> expr: the other branch vanished ([4], not [1, 3, 4])
            "lambda r: (r.b is None) or (r.a == 4)",
            "lambda r: (None is not r.b) and (r.a > 1)",
            # a branch condition compiles to POP_JUMP_IF_NOT_NONE, not IS_OP
            "lambda r: r.a > 1 if r.b is None else r.a > 3",
            "lambda r: (lambda v: v is not None)(r.b) and (r.a > 1)",
            # parenthesized `None` operands
            "lambda r: ((None) is not r.b) and (r.a > 1)",
            "lambda r: (r.b is not (None)) and (r.a > 1)",
            # the compiler emits one IS_OP per branch, after the `None` load
            "lambda r: (None is not (r.b if r.a > 0 else r.c)) and (r.a > 1)",
            # an unrelated identity test next to the real null check
            "lambda r: (r.b is not None) and (r.a > 1) if r is not r.a else r.a",
        ],
    )
    def test_short_circuit_without_source_fails_closed(self, t, source):
        with pytest.raises(TypeError, match=r"could not rewrite"):
            t.filter(eval(source))

    def test_short_circuit_in_plain_def_fails_closed(self, t):
        def pred(r):
            return (r.b is not None) and (r.a > 1)

        def pred_branch(r):
            if r.b is None:
                return r.a > 1
            return r.a > 3

        for fn in (pred, pred_branch):
            with pytest.raises(TypeError, match=r"'.*pred.*' compares a value with None"):
                t.filter(fn)

    def test_short_circuit_in_callable_instance_fails_closed(self, t):
        class Pred:
            def __call__(self, r):
                return (r.b is None) or (r.a == 4)

        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.filter(Pred())

    def test_every_public_boundary_fails_closed(self, t):
        fn = eval("lambda r: (r.b is not None) and (r.a > 1)")
        for call in (
            lambda: t.filter(fn),
            lambda: t.select(fn),
            lambda: t.delete(fn),
            lambda: t.update(fn, a=0),
            lambda: t.derive(eval("lambda r: {'x': (r.b is None) or r.a}")),
        ):
            with pytest.raises(TypeError, match=r"could not rewrite"):
                call()

    def test_python_value_check_without_source_fails_closed(self, t):
        # Without source the operand's type is unknown, so a check on a
        # Python value is rejected too; the guidance says to test it outside.
        fn = eval("lambda r: r.a > th if th is not None else r.a > 3", {"th": 1})
        with pytest.raises(TypeError, match=r"before building the function"):
            t.filter(fn)

    def test_unrelated_identity_check_next_to_none_literal_runs(self, t):
        # `flag is marker` compares two Python values and the `None` is an
        # if_else branch value: neither is a null identity check, with or
        # without source.
        marker = object()
        flag = marker
        ns = {"if_else": if_else, "flag": flag, "marker": marker}
        source = "lambda r: {'x': if_else(r.a > 2, None if flag is marker else 1, 0)}"
        expected = [0, 0, None, None]
        for fn in (
            lambda r: {"x": if_else(r.a > 2, None if flag is marker else 1, 0)},  # pyright: ignore[reportArgumentType]
            eval(source, ns),
        ):
            assert t.derive(fn).to_arrow().column("x").to_pylist() == expected

    def test_rewritten_lambda_with_unrelated_identity_check_runs(self, t):
        # After the rewrite `r.b is None` is a helper call; the remaining
        # `flag is marker` and `None` literal must not trip the check.
        marker = object()
        flag = marker
        out = t.derive(
            lambda r: {
                "x": if_else(r.b is None, None if flag is marker else 1, r.a)  # pyright: ignore[reportArgumentType]
            }
        )
        assert out.to_arrow().column("x").to_pylist() == [None, 2, None, 4]

    def test_plain_def_with_unrelated_identity_check_runs(self, t):
        # CPython 3.14 gives the implicit `return None` the position of the
        # last `if` test, here an identity check against a sentinel.
        marker = object()

        def pred(r, flag=marker):
            if flag is marker:
                return r.a > 2

        assert a_values(t.filter(pred)) == [3, 4]

    def test_check_without_column_offsets_fails_closed(self):
        # `-X no_debug_ranges` drops column offsets, so an IS_OP cannot be
        # paired with its operands; any IS_OP next to a `None` load counts.
        script = textwrap.dedent(
            """
            import pandas as pd
            from ltseq import LTSeq
            t = LTSeq.from_pandas(pd.DataFrame({"a": [1, 2], "b": [None, "x"]}))
            try:
                t.filter(eval("lambda r: (None is not r.b) and (r.a > 1)"))
            except TypeError as e:
                assert "could not rewrite" in str(e), e
            else:
                raise AssertionError("ran an unrewritten null check")
            """
        )
        subprocess.run([sys.executable, "-X", "no_debug_ranges", "-c", script], check=True)

    def test_stale_source_fails_closed(self, t, tmp_path):
        # The source on disk no longer matches the loaded lambda, so it cannot
        # be located; it must not run unrewritten.
        mod = _import_module_from_source(
            tmp_path,
            "ltseq_none_check_stale_mod",
            """
            PRED = lambda r: (r.b is not None) and (r.a > 1)  # noqa: E731
            """,
        )
        try:
            path = tmp_path / "ltseq_none_check_stale_mod.py"
            # Same line, different lambda: the candidate compiles, but not to
            # the loaded code object.
            path.write_text("\nPRED = lambda r: (r.b is None) or (r.a > 1)  # noqa: E731\n")
            stat = path.stat()
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))
            with pytest.raises(TypeError, match=r"could not rewrite"):
                t.filter(mod.PRED)
        finally:
            del sys.modules[mod.__name__]

    def test_methods_and_eq_none_without_source_still_work(self, t):
        assert a_values(t.filter(eval("lambda r: r.b.is_not_null() & (r.a > 1)"))) == [2, 4]
        assert a_values(t.filter(eval("lambda r: r.a > 1"))) == [2, 3, 4]
        # `== None` needs no rewrite: the operator itself builds `.is_null()` (#154)
        assert a_values(t.filter(eval("lambda r: r.b == None"))) == [1, 3]

    def test_plain_def_gives_guidance(self, t):
        def pred(r):
            return r.b is None

        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.filter(pred)

    def test_plain_def_without_none_check_runs(self, t):
        def pred(r):
            return r.a > 1

        assert a_values(t.filter(pred)) == [2, 3, 4]

    def test_lambda_without_none_check_is_returned_unchanged(self):
        fn = lambda r: r.a > 1  # noqa: E731
        assert _transform_lambda_for_none_checks(fn) is fn

    def test_non_function_callable_is_returned_unchanged(self):
        class Pred:
            def __call__(self, r):
                return r.a > 1

        p = Pred()
        assert _transform_lambda_for_none_checks(p) is p


def _unflattened_partial(fn, *args, **kwargs):
    """A partial of a partial. CPython flattens ``partial(partial(f))`` into
    one partial unless the inner one carries attributes."""
    inner = functools.partial(fn, *args, **kwargs)
    setattr(inner, "tag", "inner")
    outer = functools.partial(inner)
    assert outer.func is inner
    return outer


class _ForwardingProxy:
    """The shape of wrapt's ``FunctionWrapper``: it reaches the function only
    through attribute hooks and its own ``__call__``. The callable LTSeq is
    handed is read as Python reads it, so this proxy is followed there; one
    that the code names is read statically and is not. With wrapt 1.17.2, a
    ``FunctionWrapper`` and this class behave alike on both reads."""

    def __init__(self, wrapped):
        object.__setattr__(self, "_wrapped", wrapped)

    @property
    def __class__(self):
        return type(object.__getattribute__(self, "_wrapped"))

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_wrapped"), name)

    def __call__(self, *args):
        return object.__getattribute__(self, "_wrapped")(*args)


class TestWrappedCallables:
    """A wrapper implemented in C (``functools.partial``, ``lru_cache``,
    ``staticmethod``) has no code of its own. The fail-closed check must look
    through it at the function it calls, or an unrewritten ``is None`` runs
    and ``and`` / ``or`` silently drop part of the predicate."""

    def test_partial_of_lambda_is_rewritten(self, t):
        pred = lambda r, th: (r.b is not None) & (r.a > th)  # noqa: E731
        assert a_values(t.filter(functools.partial(pred, th=1))) == [2, 4]
        assert a_values(t.filter(functools.partial(pred, th=2))) == [4]

    def test_nested_partial_of_lambda_is_rewritten(self, t):
        pred = lambda r, th: (r.b is None) | (r.a > th)  # noqa: E731
        assert a_values(t.filter(_unflattened_partial(pred, th=3))) == [1, 3, 4]

    def test_partial_of_lambda_with_short_circuit_never_drops_the_check(self, t):
        # Rewritten, `and` reaches Expr.__bool__, which refuses instead of
        # returning `r.a > 1` alone (which would admit row 3, where b is null).
        pred = lambda r: (r.b is not None) and (r.a > 1)  # noqa: E731
        with pytest.raises(TypeError, match=r"&"):
            t.filter(functools.partial(pred))

    @pytest.mark.parametrize(
        "wrap",
        [
            functools.partial,
            _unflattened_partial,
            functools.lru_cache,
            staticmethod,
        ],
        ids=["partial", "nested-partial", "lru_cache", "staticmethod"],
    )
    def test_wrapped_function_without_source_fails_closed(self, t, wrap):
        # Unwrapped, this admits rows 2 and 3 instead of only 2.
        fn = eval("lambda r: (r.b is not None) and (r.a > 1)")
        with pytest.raises(TypeError, match=r"could not rewrite"):
            t.filter(wrap(fn))

    def test_wrapped_plain_def_fails_closed(self, t):
        def pred(r):
            return (r.b is None) or (r.a == 4)

        for wrapped in (functools.partial(pred), functools.lru_cache(pred)):
            with pytest.raises(TypeError, match=r"'.*pred' compares a value with None"):
                t.filter(wrapped)

    def test_decorated_lambda_fails_closed(self, t):
        # The decorator's own code has no null check; the lambda it calls
        # (reachable through __wrapped__) does, and was not rewritten.
        def logged(fn):
            @functools.wraps(fn)
            def wrapper(r):
                return fn(r)

            return wrapper

        with pytest.raises(TypeError, match=r"could not rewrite"):
            t.filter(logged(lambda r: (r.b is not None) and (r.a > 1)))

    def test_partial_subclass_fails_closed(self, t):
        # Only an exact functools.partial is rebuilt around the rewritten
        # lambda; a subclass may change how it calls, so it is checked instead.
        class Bound(functools.partial):
            pass

        pred = lambda r: (r.b is not None) and (r.a > 1)  # noqa: E731
        with pytest.raises(TypeError, match=r"could not rewrite"):
            t.filter(Bound(pred))

    def test_wrapped_function_without_null_check_runs(self, t):
        pred = eval("lambda r, th: r.a > th")
        assert a_values(t.filter(functools.partial(pred, th=2))) == [3, 4]
        assert a_values(t.filter(_unflattened_partial(pred, th=2))) == [3, 4]
        assert a_values(t.filter(functools.lru_cache(lambda r: r.a > 2))) == [3, 4]

    def test_callable_that_fabricates_attributes_runs(self, t):
        # `__getattr__` returns a fresh object for `__wrapped__` / `__func__`;
        # following those would never end.
        class Anything:
            def __getattr__(self, name):
                return Anything()

            def __call__(self, r):
                return r.a > 2

        assert a_values(t.filter(Anything())) == [3, 4]

    @pytest.mark.parametrize("decorated", [False, True], ids=["proxy", "decorated-proxy"])
    def test_proxy_of_function_fails_closed(self, t, decorated):
        # A proxy that the code names is not followed
        # (TestCalledHelpers.test_helper_reached_other_than_by_name_is_not_checked).
        # A decorator's closure names the proxy it wraps, which must not make
        # it one: run unchecked, the decorated proxy returned [2, 3, 4].
        def pred(r):
            return (r.b is None) or (r.a > 1)

        fn = _ForwardingProxy(pred)
        if decorated:
            fn = _passthrough(fn)
        with pytest.raises(TypeError, match=r"^'[^']*pred' compares a value with None"):
            t.filter(fn)

    def test_partial_without_null_check_is_returned_unchanged(self):
        p = functools.partial(lambda r, th: r.a > th, th=1)
        assert _transform_lambda_for_none_checks(p) is p


def _passthrough(fn):
    @functools.wraps(fn)
    def wrapper(*args):
        return fn(*args)

    return wrapper


def _bound_method(fn):
    class Holder:
        def check(self, r):
            return fn(r)

    return Holder().check


def _callable_instance(fn):
    class Check:
        def __call__(self, r):
            return fn(r)

    return Check()


def _classmethod_call(fn):
    class Check:
        __call__ = classmethod(lambda cls, r: fn(r))

    return Check()


def _metaclass_call(fn):
    class Calls(type):
        def __call__(cls, r):
            return fn(r)

    return Calls("Check", (), {})


class TestCalledHelpers:
    """A helper the lambda calls is not rewritten, so its ``r.b is None`` is
    the Python value False, and ``or`` / ``and`` / ``|`` used to drop it from
    the captured expression without an error (#185). The functions a lambda
    names, through globals and closure variables, are checked and refused
    instead, as far as they are user code."""

    def test_module_helper_raises_naming_it(self, t):
        # Run unchecked, this returned [2, 3, 4] instead of [1, 2, 3, 4].
        with pytest.raises(
            TypeError,
            match=r"^'missing' \(reached by name from '.*<lambda>'\) compares a value with None",
        ):
            t.filter(lambda r: missing(r) or (r.a > 1))

    def test_bitor_in_either_order(self, t):
        # Run unchecked, the first returned [4] instead of [1, 3, 4], and the
        # second failed on `bool | BinOpExpr` without naming the cause.
        for pred in (
            lambda r: (r.a > 3) | missing(r),
            lambda r: missing(r) | (r.a > 3),
        ):
            with pytest.raises(TypeError, match=r"could not rewrite"):
                t.filter(pred)

    def test_helper_through_helper(self, t):
        with pytest.raises(TypeError) as info:
            t.filter(lambda r: missing_via_helper(r) or (r.a > 1))
        assert str(info.value).startswith(
            "'missing' (reached by name from 'TestCalledHelpers.test_helper_through_helper."
            "<locals>.<lambda>' via 'missing_via_helper') compares a value with None"
        )
        with pytest.raises(TypeError) as info:
            t.filter(lambda r: missing_via_two_helpers(r) or (r.a > 1))
        assert str(info.value).startswith(
            "'missing' (reached by name from 'TestCalledHelpers.test_helper_through_helper."
            "<locals>.<lambda>' via 'missing_via_two_helpers' -> 'missing_via_helper') "
            "compares a value with None"
        )

    def test_route_has_the_fewest_names(self, t):
        # Each wrapper's closure names the function, which the wrapper also
        # reaches through `__wrapped__` without a name; that route is the one
        # reported. The first wrapper goes through a partial on the way, so
        # its named route takes fewer steps. The second has the helper's name
        # from functools.wraps, so its named route read "via 'missing'".
        def keep_partial(fn):
            @functools.wraps(functools.partial(fn))
            def wrapper(*args):
                return fn(*args)

            return wrapper

        def pred(r):
            return (r.b is None) or (r.a > 1)

        with pytest.raises(TypeError, match=r"^'[^']*pred' compares a value with None"):
            t.filter(keep_partial(pred))
        check = _passthrough(missing)
        with pytest.raises(
            TypeError,
            match=r"^'missing' \(reached by name from '[^']*<lambda>'\) compares a value with None",
        ):
            t.filter(lambda r: check(r) or (r.a > 1))

    def test_helper_in_closure(self, t):
        def absent(r):
            return r.b is None

        with pytest.raises(TypeError, match=r"'.*absent' \(reached by name from"):
            t.filter(lambda r: absent(r) or (r.a > 1))

    def test_helper_in_closure_of_helper(self, t):
        def make_check():
            def absent(r):
                return r.b is None

            def check(r):
                return absent(r)

            return check

        check = make_check()
        with pytest.raises(
            TypeError, match=r"'.*absent' \(reached by name from .* via '.*check'\)"
        ):
            t.filter(lambda r: check(r) or (r.a > 1))

    @pytest.mark.parametrize(
        "wrap",
        [
            functools.partial,
            functools.lru_cache,
            functools.cache,
            staticmethod,
            _passthrough,
            _bound_method,
            _callable_instance,
            _classmethod_call,
            _metaclass_call,
        ],
        ids=[
            "partial",
            "lru_cache",
            "cache",
            "staticmethod",
            "decorator",
            "bound-method",
            "callable-instance",
            "classmethod-call",
            "metaclass-call",
        ],
    )
    def test_helper_behind_wrapper(self, t, wrap):
        def absent(r):
            return r.b is None

        check = wrap(absent)
        with pytest.raises(TypeError, match=r"'.*absent' \(reached by name from"):
            t.filter(lambda r: check(r) or (r.a > 1))

    def test_every_public_boundary(self, t):
        for call in (
            lambda: t.filter(lambda r: missing(r) or (r.a > 1)),
            lambda: t.select(lambda r: [r.a, missing(r) or r.b]),
            lambda: t.delete(lambda r: missing(r) or (r.a > 1)),
            lambda: t.update(lambda r: missing(r) or (r.a > 1), a=0),
            lambda: t.derive(lambda r: {"x": missing(r) or r.a}),
        ):
            with pytest.raises(TypeError, match=r"'missing' \(reached by name from"):
                call()

    def test_helper_passed_by_name_is_checked(self, t):
        # Run unchecked, `apply(missing, r) or ...` returned [2, 3, 4]
        # instead of [1, 2, 3, 4]. The code does not say whether the function
        # it passes `missing` to calls it, so a function that ignores it is
        # refused too (contract section 3.4).
        for pred in (
            lambda r: apply(missing, r) or (r.a > 1),
            lambda r: ignores_check(r, missing),
        ):
            with pytest.raises(
                TypeError, match=r"^'missing' \(reached by name from '.*<lambda>'\)"
            ):
                t.filter(pred)

    @pytest.mark.parametrize(
        "form",
        ["attribute", "item", "default-value", "partial-argument", "class-method", "proxy"],
    )
    def test_helper_reached_other_than_by_name_is_not_checked(self, t, form):
        # Each form that contract section 3.4 lists as not reached. The walk
        # follows names, not attributes, items or the values of parameters;
        # `type.__call__`, written in C, runs a class's methods; and a proxy
        # that the code names reaches its function only by running code. So
        # `or` still folds the helper's bool out, and the result is the rows
        # where a > 1.
        helpers = types.SimpleNamespace(missing=missing)
        checks = [missing]

        def with_default(r, check=missing):
            return check(r)

        bound = functools.partial(apply, missing)

        class Missing:
            def __new__(cls, r):
                return r.b is None

        proxied = _ForwardingProxy(missing)
        pred = {
            "attribute": lambda r: helpers.missing(r) or (r.a > 1),
            "item": lambda r: checks[0](r) or (r.a > 1),
            "default-value": lambda r: with_default(r) or (r.a > 1),
            "partial-argument": lambda r: bound(r) or (r.a > 1),
            "class-method": lambda r: Missing(r) or (r.a > 1),
            "proxy": lambda r: proxied(r) or (r.a > 1),
        }[form]
        assert a_values(t.filter(pred)) == [2, 3, 4]

    def test_python_value_test_in_helper_is_rejected_too(self, t):
        # Helpers are checked, not rewritten, so a test on a Python value
        # cannot be told apart from one on a column.
        def threshold(value=None):
            return 2 if value is None else value

        with pytest.raises(TypeError, match=r"against a sentinel object rather than None"):
            t.filter(lambda r: r.a > threshold())

    def test_sentinel_in_helper_runs(self, t):
        unset = object()

        def threshold(value=unset):
            return 2 if value is unset else value

        assert a_values(t.filter(lambda r: r.a > threshold())) == [3, 4]

    def test_helper_without_null_test_runs(self, t):
        def above(r, n):
            return r.a > n

        assert a_values(t.filter(lambda r: above_one(r))) == [2, 3, 4]
        assert a_values(t.filter(lambda r: above(r, 2) & above_one(r))) == [3, 4]
        # The lambda's own test is rewritten; the helper next to it is clean.
        assert a_values(t.filter(lambda r: (r.b is None) & above_one(r))) == [3]

    def test_recursive_helpers_terminate(self, t):
        assert a_values(t.filter(lambda r: ping(r))) == [3, 4]

    def test_ltseq_functions_are_not_checked(self, t):
        # `log(x, base=None)` tests its Python argument with `is None`.
        assert _tests_identity_with_none(log.__code__)
        out = t.derive(lambda r: {"x": log(r.a), "y": if_else(r.b.is_null(), 1, 0)})
        assert out.to_arrow().column("y").to_pylist() == [1, 0, 1, 0]

    def test_standard_library_functions_are_not_checked(self, t):
        assert _tests_identity_with_none(loads.__code__)
        assert a_values(t.filter(lambda r: r.a.is_in(loads("[2, 4]")))) == [2, 4]

    def test_installed_packages_are_not_checked(self, t, tmp_path):
        # The same helper is library code under site-packages and user code
        # anywhere else.
        source = """
            def threshold(value=None):
                return 2 if value is None else value
            """
        site_packages = tmp_path / "site-packages"
        site_packages.mkdir()
        installed = _import_module_from_source(
            site_packages, "ltseq_none_check_installed_mod", source
        )
        local = _import_module_from_source(tmp_path, "ltseq_none_check_local_mod", source)
        try:
            installed_threshold = installed.threshold
            local_threshold = local.threshold
            assert a_values(t.filter(lambda r: r.a > installed_threshold())) == [3, 4]
            with pytest.raises(TypeError, match=r"'threshold' \(reached by name from"):
                t.filter(lambda r: r.a > local_threshold())
        finally:
            del sys.modules[installed.__name__], sys.modules[local.__name__]

    @pytest.mark.parametrize("decorated", [False, True], ids=["function", "decorated"])
    def test_installed_function_handed_in_is_checked(self, t, tmp_path, decorated):
        # Library code is skipped only when reached by name, and a
        # decorator's closure naming the function it wraps does not make it
        # so: run unchecked, the decorated function returned [2, 3, 4].
        source = """
            def pred(r):
                return (r.b is None) or (r.a > 1)
            """
        site_packages = tmp_path / "site-packages"
        site_packages.mkdir()
        installed = _import_module_from_source(
            site_packages, "ltseq_none_check_installed_pred", source
        )
        try:
            fn = _passthrough(installed.pred) if decorated else installed.pred
            with pytest.raises(TypeError, match=r"^'pred' compares a value with None"):
                t.filter(fn)
        finally:
            del sys.modules[installed.__name__]

    def test_wrapper_reached_both_ways_is_read_both_ways(self, t):
        # The decorator's closure names the partial it wraps, so the partial
        # is reached directly and by name. Read as called, its `func` is the
        # property's; read statically, it is the function the partial calls.
        def harmless(r):
            return r.a > 1

        class Shadowed(functools.partial):
            @property
            def func(self):
                return harmless

        def pred(r):
            return (r.b is None) or (r.a > 1)

        with pytest.raises(TypeError, match=r"^'[^']*pred' \(reached by name from"):
            t.filter(_passthrough(Shadowed(pred)))

    def test_closure_variable_bound_after_the_call_is_skipped(self, t):
        use_later = False
        pred = lambda r: later(r) if use_later else r.a > 2  # noqa: E731
        assert a_values(t.filter(pred)) == [3, 4]

        def later(r):
            return r.b is None

    @pytest.mark.parametrize("named", ["instance", "class", "instance-of-class"])
    def test_objects_found_by_name_run_no_code(self, t, named):
        # Like a lazy proxy, or werkzeug's LocalProxy outside a request. The
        # helper names it in a branch that does not run, so the capture must
        # neither run its attribute hooks (`__getattribute__` also serves
        # `__getattr__`, properties and the `__class__` that isinstance
        # reads) nor fail on them. A class's hooks are its metaclass's, and
        # they run on reading `__dict__`, which inspect.getattr_static does.
        touched = []

        class Unbound:
            def __getattribute__(self, name):
                touched.append(name)
                raise RuntimeError("working outside of a context")

            def __call__(self):
                raise RuntimeError("working outside of a context")

        class Bound(type):
            def __getattribute__(cls, name):
                touched.append(name)
                raise RuntimeError("working outside of a context")

        class Lazy(metaclass=Bound):
            def __call__(self):
                raise RuntimeError("working outside of a context")

        obj = {"instance": Unbound, "class": Lazy, "instance-of-class": Lazy}[named]
        if named != "class":
            obj = obj()

        def threshold(from_context=False):
            return obj() if from_context else 2

        assert a_values(t.filter(lambda r: r.a > threshold())) == [3, 4]
        assert touched == []

    def test_wrapper_subclass_is_followed_without_its_hooks(self, t):
        touched = []

        class Recorded(functools.partial):
            def __getattribute__(self, name):
                touched.append(name)
                return super().__getattribute__(name)

        def absent(r):
            return r.b is None

        check = Recorded(absent)
        with pytest.raises(TypeError, match=r"'.*absent' \(reached by name from"):
            t.filter(lambda r: check(r) or (r.a > 1))
        assert touched == []
