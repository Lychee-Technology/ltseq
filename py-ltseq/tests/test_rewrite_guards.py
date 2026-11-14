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


def test_each_expression_keeps_its_own_failing_operand(engine_layout):
    """R2-F1. With `a` NULL, `v`'s absorption operand is demanded (D10), and
    `w` evaluates the same cast only where `a` is TRUE. The guard counted
    the cast once for the whole projection, so it let absorption reduce `v`
    to `a` while `w` kept a copy, and the row came back as `[None, 0]`. Each
    expression now keeps its own occurrences, so `v` keeps the cast and
    raises."""
    t = LTSeq.from_arrow(pa.table({"a": pa.array([None], pa.bool_())}))
    from ltseq.expr import if_else

    derived = t.derive(
        v=lambda r: r.a | (r.a & (failing() > 0)),
        w=lambda r: if_else(r.a, failing(), 0),
    )
    logical, _ = derived.explain_plan()
    projection = next(line for line in logical.splitlines() if " AS v" in line)
    assert projection.count('opaque_cast_Int32(Utf8("abc"))') == 2, logical
    with pytest.raises(ValueError, match="Cast error"):
        derived.to_arrow()


@pytest.mark.parametrize(
    "lower, upper, expected",
    [
        pytest.param(lambda r: 10 // r.x > 1, lambda r: r.x != 0, None, id="lower_fails"),
        pytest.param(lambda r: r.x != 0, lambda r: 10 // r.x > 1, [2], id="upper_fails"),
    ],
)
def test_chained_filters_are_not_merged_where_one_can_fail(
    engine_layout, lower, upper, expected
):
    """R2-F2. The first filter demands its predicate on every row, the second
    only on the rows the first keeps. `push_down_filter` merged them into
    one conjunction and put `x != 0` first, which suppressed the division by
    zero the first filter demands on `x = 0`, and returned `[2]`. Where
    either predicate can fail, a barrier now keeps them apart: the first
    case raises, and the second still evaluates the division only where
    `x != 0`."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([2, 0, 0, 0, 0], pa.int64())}))
    filtered = t.filter(lower).filter(upper)
    logical, _ = filtered.explain_plan()
    assert logical.count("Filter:") == 2 and "DemandBarrier" in logical, logical
    if expected is None:
        with pytest.raises(ValueError, match="(?i)divi"):
            filtered.to_arrow()
    else:
        assert filtered.to_arrow().column("x").to_pylist() == expected


def test_chained_filters_that_cannot_fail_still_merge(engine_layout):
    t = LTSeq.from_arrow(pa.table({"x": pa.array([2, 0, 5], pa.int64())}))
    filtered = t.filter(lambda r: r.x > 0).filter(lambda r: r.x != 5)
    logical, _ = filtered.explain_plan()
    assert logical.count("Filter:") <= 1 and "DemandBarrier" not in logical, logical
    assert filtered.to_arrow().column("x").to_pylist() == [2]


def test_a_sql_cast_that_can_fail_raises_only_where_demanded(engine_layout):
    """R2-F3. `filter_where` takes SQL, whose casts the program writes. A
    constant cast that fails folded into a planning error even where no row
    reaches it (X1), and `unwrap_cast` removed a narrowing cast next to a
    literal, so `CAST(x AS INTEGER) > 5` kept `x = 2**40` instead of
    raising (X3). They are now lowered as `cast()` is."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([1, 2], pa.int64())}))
    clause = "CASE WHEN x = 0 THEN CAST('abc' AS INTEGER) ELSE x END > 0"
    kept = LTSeq._from_inner(t._inner.filter_where(clause))
    # The table declares no order, so its rows come in any order (#148).
    assert sorted(kept.to_arrow().column("x").to_pylist()) == [1, 2]
    zero = LTSeq.from_arrow(pa.table({"x": pa.array([0, 2], pa.int64())}))
    with pytest.raises(ValueError, match="Cast error"):
        LTSeq._from_inner(zero._inner.filter_where(clause)).to_arrow()

    big = LTSeq.from_arrow(pa.table({"x": pa.array([2**40], pa.int64())}))
    narrowed = LTSeq._from_inner(big._inner.filter_where("CAST(x AS INTEGER) > 5"))
    logical, _ = narrowed.explain_plan()
    assert "opaque_cast_Int32" in logical, logical
    with pytest.raises(ValueError, match="(?i)cast|overflow|out of range"):
        narrowed.to_arrow()


@pytest.mark.parametrize(
    "after",
    [
        pytest.param(lambda t: t.derive(j=lambda r: r.i + 1), id="derive"),
        pytest.param(
            lambda t: t.derive(j=lambda r: r.i * 2).filter(lambda r: r.j % 3 != 1),
            id="derive_filter",
        ),
        pytest.param(
            lambda t: t.filter(lambda r: r.i % 5 != 0).derive(j=lambda r: r.i - 1).head(40),
            id="filter_derive_head",
        ),
    ],
)
def test_rows_equal_on_the_sort_keys_keep_their_order_after_row_wise_operations(
    engine_layout, after
):
    """R2-F4, `EnsureRequirements` (X5). A stable sort orders ties by each
    row's position, which it drops. Reading the table re-sorted it by its
    keys alone above a projection that computes a column; DataFusion
    repartitioned the projection, removed the stable sort as redundant
    under the new one, and returned rows equal on `k` in any order at more
    than one partition. The stable sort is now planned at the root, above
    the row-wise operations, so its ties keep their order."""
    n = 200
    t = LTSeq.from_arrow(pa.table({"k": [i % 3 for i in range(n)], "i": list(range(n))}))
    result = after(t.sort("k")).to_arrow()
    pairs = list(zip(result.column("k").to_pylist(), result.column("i").to_pylist()))
    assert pairs == sorted(pairs), pairs[:20]
    assert result.num_rows > 0
