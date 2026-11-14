"""The guards of the rewrites the rule inventory rates Restricted (#273).

Row-wise demand (contract §20.2): an execution error is raised if and only
if a demanded value fails, and a rewrite that would drop or skip a demanded
value that can fail is not applied. The rule inventory in
`docs/proposals/v0.5/review/trade-offs.md` rates each DataFusion rule, and
`src/rewrite_guard.rs` holds the Restricted ones to their condition. Rust's
`plan_guard_*` tests assert each guarded rule's optimized plan; these check
the behavior the guards keep, at every engine layout (#274).
"""

import pyarrow as pa
import pytest

from ltseq import LTSeq
from ltseq.expr import LiteralExpr


def failing():
    """A constant cast that fails on every row that evaluates it."""
    return LiteralExpr("abc").cast("int32")


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(lambda r: failing().is_null(), id="is_null"),
        pytest.param(lambda r: failing() * 0, id="times_zero"),
        pytest.param(lambda r: failing() == failing(), id="equal_to_itself"),
        pytest.param(lambda r: r.a | (r.a & (failing() > 0)), id="absorption"),
    ],
)
def test_dropping_identities_keep_a_failing_operand(engine_layout, value):
    """Assessment hazard X2. The simplifier's identities gated on
    non-nullability (`A IS NULL` to FALSE, `A * 0` to 0, `A = A` to TRUE)
    and absorption (`A OR (A AND B)` to `A`, with no gate) dropped the
    cast, and the value came back without the error the cast raises on its
    own. In the absorption case `a` is NULL on the second row, so the right
    operand of `|` is demanded there (D10). `simplify_expressions` now
    refuses a rewrite that drops an expression that can fail, so the plan
    keeps the cast and each raises."""
    t = LTSeq.from_arrow(pa.table({"a": pa.array([True, None], pa.bool_())}))
    derived = t.derive(v=value)
    logical, _ = derived.explain_plan()
    assert 'opaque_cast_Int32(Utf8("abc"))' in logical, logical
    with pytest.raises(ValueError, match="Cast error"):
        derived.to_arrow()


def test_a_dropping_identity_still_applies_where_nothing_can_fail(engine_layout):
    """The guard refuses only rewrites that drop what can fail."""
    t = LTSeq.from_arrow(pa.table({"a": pa.array([True, None], pa.bool_())}))
    derived = t.derive(v=lambda r: r.a | (r.a & (LiteralExpr("12").cast("int32") > 0)))
    assert derived.to_arrow().column("v").to_pylist() == [True, None]


def x_and_s():
    return LTSeq.from_arrow(
        pa.table(
            {
                "x": pa.array([0, 2, 5], pa.int64()),
                "s": pa.array(["abc", "2", "5"], pa.string()),
            }
        )
    )


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda t: t.sort("k"), id="sort"),
        pytest.param(lambda t: t.distinct(), id="distinct"),
    ],
)
def test_a_filter_after_an_operator_does_not_skip_its_keys(engine_layout, operation):
    """Assessment hazard X4 under D10a (a). A sort or `distinct` demands its
    keys on every row of its input, including rows a later filter removes
    (§20.2). The filter `x != 0` moved below the operator, so the key was
    never computed for the row with `s = "abc"`, and two rows came back.

    A `distinct` is a DataFusion `Distinct`, and `ltseq_keep_demanded_keys`
    stops the filter at a barrier above the projection that computes the
    key. A `sort` reads its input in table order through LTSeq's own
    provider (`TableOrder`), which took the filter into its input plan; it
    takes none where a key can fail. Either way the cast raises."""
    keyed = x_and_s().derive(k=lambda r: r.s.cast("int32"))
    filtered = operation(keyed).filter(lambda r: r.x != 0)
    logical, _ = filtered.explain_plan()
    assert "full_filters" not in logical, logical
    with pytest.raises(ValueError, match="Cast error"):
        filtered.to_arrow()
    with pytest.raises(ValueError, match="Cast error"):
        filtered.count()


def test_a_filter_after_an_operator_still_moves_where_no_key_can_fail(engine_layout):
    """Where no key can fail, the filter still moves into the scan."""
    keyed = x_and_s().derive(k=lambda r: r.x + 1)
    for filtered in (
        keyed.sort("k").filter(lambda r: r.x != 0),
        keyed.distinct().filter(lambda r: r.x != 0),
    ):
        logical, _ = filtered.explain_plan()
        assert "DemandBarrier" not in logical, logical
        lines = logical.splitlines()
        operator = next(i for i, l in enumerate(lines) if "Sort:" in l or "Aggregate:" in l)
        moved = next(i for i, l in enumerate(lines) if "x != Int64(0)" in l)
        assert moved > operator, logical
        assert sorted(filtered.to_arrow().column("x").to_pylist()) == [2, 5]
