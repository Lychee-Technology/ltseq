"""Dynamic CASE expressions in IN lists (#297, apache/datafusion#26082/#26083).

DataFusion 55 evaluated IN-list items on an empty batch to detect constant
lists (`try_evaluate_constant_list`), so a row-dependent `CASE` could return a
scalar on zero rows and be frozen into a static `IN (SET)` filter. DataFusion
56.0.0 requires every leaf of every item to be a `Literal` before building a
static filter (apache/datafusion#26083). PR #385 also closed the original
public `is_in()` entry path by refusing non-literal items at capture.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else


def _keyed(table: LTSeq, key: str, value: str) -> list:
    arrow = table.to_arrow()
    pairs = sorted(zip(arrow.column(key).to_pylist(), arrow.column(value).to_pylist()))
    return [v for _, v in pairs]


def _plan_lines(table: LTSeq, prefix: str) -> list[str]:
    _, physical = table.explain_plan()
    return [line for line in physical.splitlines() if prefix in line]


def _t1_table() -> LTSeq:
    return LTSeq.from_arrow(
        pa.table(
            {
                "n": pa.array([2, 10, 5, 7], pa.int32()),
                "d": pa.array([0, 2, 2, 1], pa.int32()),
            }
        )
    )


def _t2_table() -> LTSeq:
    return LTSeq.from_arrow(
        pa.table(
            {
                "k": pa.array([0, 1, 2, 3], pa.int64()),
                "x": pa.array([1, 5, 9, None], pa.int64()),
                "y": pa.array([0, 5, None, 0], pa.int64()),
            }
        )
    )


def test_t1_dynamic_case_in_list_via_filter_where():
    """T1: upstream #26082 repro through LTSeq's internal filter_where path."""
    t = _t1_table()
    kept = LTSeq._from_inner(
        t._inner.filter_where("n IN (2, 100, 101, CASE WHEN d > 5 THEN 1 ELSE d END * 5)")
    )
    assert sorted(kept.to_arrow().column("n").to_pylist()) == [2, 10]
    filters = _plan_lines(kept, "FilterExec:")
    assert filters
    assert any("IN ([" in line and "CASE WHEN" in line for line in filters), filters
    assert all("IN (SET)" not in line for line in filters), filters


def test_t2_null_producing_dynamic_case_via_filter_where():
    """T2: NULL-producing dynamic CASE in IN list preserves SQL three-valued logic."""
    t = _t2_table()
    clause = "x IN (CASE WHEN y = 0 THEN 1 ELSE y END, 20, 21, 22)"
    kept = LTSeq._from_inner(t._inner.filter_where(clause))
    assert sorted(kept.to_arrow().column("k").to_pylist()) == [0, 1]
    filters = _plan_lines(kept, "FilterExec:")
    assert filters
    assert any("IN ([" in line and "CASE WHEN" in line for line in filters), filters
    assert all("IN (SET)" not in line for line in filters), filters

    null_rows = LTSeq._from_inner(t._inner.filter_where(f"({clause}) IS NULL"))
    assert sorted(null_rows.to_arrow().column("k").to_pylist()) == [2, 3]


@pytest.mark.parametrize("sorted_input", [False, True], ids=["unsorted", "sorted"])
def test_t3_legal_dsl_disjunction_with_dynamic_case(sorted_input):
    """T3: legal LTSeq DSL equality disjunctions with dynamic if_else()."""
    t2 = _t2_table().sort("k") if sorted_input else _t2_table()
    t2_pred = (
        lambda r: (r.x == if_else(r.y == 0, 1, r.y))
        | (r.x == 20)
        | (r.x == 21)
        | (r.x == 22)
    )
    t2_proj = t2.derive(m=t2_pred)
    if sorted_input:
        assert t2_proj.to_arrow().column("m").to_pylist() == [True, True, None, None]
        assert t2.filter(t2_pred).to_arrow().column("k").to_pylist() == [0, 1]
    else:
        assert _keyed(t2_proj, "k", "m") == [True, True, None, None]
        assert sorted(t2.filter(t2_pred).to_arrow().column("k").to_pylist()) == [0, 1]
        for op_prefix, table in (
            ("ProjectionExec:", t2_proj),
            ("FilterExec:", t2.filter(t2_pred)),
        ):
            lines = _plan_lines(table, op_prefix)
            assert lines
            assert all("IN (SET)" not in line for line in lines), lines

    t1 = _t1_table().sort("n") if sorted_input else _t1_table()
    t1_pred = (
        lambda r: (r.n == 2)
        | (r.n == 100)
        | (r.n == 101)
        | (r.n == if_else(r.d > 5, 1, r.d) * 5)
    )
    t1_proj = t1.derive(m=t1_pred)
    if sorted_input:
        assert t1_proj.to_arrow().column("n").to_pylist() == [2, 5, 7, 10]
        assert t1_proj.to_arrow().column("m").to_pylist() == [True, False, False, True]
        assert t1.filter(t1_pred).to_arrow().column("n").to_pylist() == [2, 10]
    else:
        assert _keyed(t1_proj, "n", "m") == [True, False, False, True]
        assert sorted(t1.filter(t1_pred).to_arrow().column("n").to_pylist()) == [2, 10]
        for op_prefix, table in (
            ("ProjectionExec:", t1_proj),
            ("FilterExec:", t1.filter(t1_pred)),
        ):
            lines = _plan_lines(table, op_prefix)
            assert lines
            assert any("IN (SET) ([2, 100, 101])" in line and "CASE " in line for line in lines), lines


def test_t3_literal_first_disjunction_keeps_dynamic_case_outside_static_set():
    """Literal equalities merge into IN (SET), while dynamic CASE stays outside."""
    t2 = _t2_table()
    pred = (
        lambda r: ((r.x == 20) | (r.x == 21) | (r.x == 22) | (r.x == 23))
        | (r.x == if_else(r.y == 0, 1, r.y))
    )
    assert _keyed(t2.derive(m=pred), "k", "m") == [True, True, None, None]
    kept = t2.filter(pred)
    assert sorted(kept.to_arrow().column("k").to_pylist()) == [0, 1]
    filters = _plan_lines(kept, "FilterExec:")
    assert filters
    assert any("IN (SET) ([20, 21, 22, 23])" in line and "CASE " in line for line in filters), filters


def test_t4_literal_only_static_filter_and_expression_refusal():
    """T4: literal-only is_in() still uses IN (SET); expression items raise TypeError."""
    t = _t2_table()
    proj = t.derive(m=lambda r: r.x.is_in([1, 2, 3, 4]))
    assert _keyed(proj, "k", "m") == [True, False, False, None]
    kept = t.filter(lambda r: r.x.is_in([1, 2, 3, 4]))
    assert kept.to_arrow().column("k").to_pylist() == [0]
    filters = _plan_lines(kept, "FilterExec:")
    assert any("IN (SET)" in line for line in filters), filters

    with pytest.raises(TypeError, match="is_in\\(\\) takes literal values only"):
        t.derive(m=lambda r: r.x.is_in([r.y, 1, 2, 3]))
