"""
Tests for the `is None` / `is not None` lambda rewrite (issue #144).

The rewrite must:
- produce the same rows as `.is_null()` / `.is_not_null()` (not `== None`);
- keep the lambda's module globals and closure cells;
- be independent of source layout (several lambdas on one line, f-strings,
  triple-quoted strings, multi-line lambdas, nested lambdas);
- keep Python semantics for operands that are not expressions;
- fall back to a guidance TypeError (never a NameError, never a silent
  wrong result) when the lambda source cannot be inspected.
"""

import pandas as pd
import pytest

from ltseq import LTSeq
from ltseq.expr import _transform_lambda_for_none_checks
from ltseq.expr.base import if_else

MODULE_THRESHOLD = 2


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
        out = rows(t.derive(lambda r: {"label": if_else(r.b is None, "gap", "ok")}))
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
        out = rows(t.derive(lambda r: {"lbl": if_else(r.b is None, label, """ok)""")}))
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


class TestFallbacks:
    def test_lambda_without_source_gives_guidance(self, t):
        fn = eval("lambda r: r.b is None")
        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.filter(fn)

    def test_lambda_without_source_and_closure_never_name_errors(self, t):
        ns = {"th": 2}
        fn = eval("lambda r: (r.b is not None) & (r.a > th)", ns)
        with pytest.raises(TypeError):
            t.filter(fn)

    def test_plain_def_gives_guidance(self, t):
        def pred(r):
            return r.b is None

        with pytest.raises(TypeError, match=r"is_null\(\)"):
            t.filter(pred)

    def test_lambda_without_none_check_is_returned_unchanged(self):
        fn = lambda r: r.a > 1  # noqa: E731
        assert _transform_lambda_for_none_checks(fn) is fn

    def test_non_function_callable_is_returned_unchanged(self):
        class Pred:
            def __call__(self, r):
                return r.a > 1

        p = Pred()
        assert _transform_lambda_for_none_checks(p) is p
