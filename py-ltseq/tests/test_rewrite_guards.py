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


@pytest.mark.parametrize(
    "after",
    [
        pytest.param(lambda t: t.filter(lambda r: r.x != 0), id="filter"),
        pytest.param(
            lambda t: t.derive(y=lambda r: r.x + 1).filter(lambda r: r.x != 0),
            id="derive_filter",
        ),
        pytest.param(lambda t: t.filter(lambda r: r.x != 0).head(5), id="filter_head"),
    ],
)
def test_a_filter_after_a_sort_does_not_skip_a_key_the_sort_computes(engine_layout, after):
    """R4-F1, X4 under D10a (a). Reading a sorted table moves the stable
    sort above the filters and projections after it (R2-F4), so the filter
    `x != 0` ran below the sort, and the sort computed its second key, a
    cast, only on the row the filter kept: one row came back, while the
    sort alone raises on `'bad'`. `TableOrder` cannot keep a key the sort
    computes itself. Where a key can fail, the sort's computed keys are
    now computed on every row of its input, under a barrier the filter
    does not cross, and the sort reads them as columns."""
    t = LTSeq.from_arrow(
        pa.table({"x": pa.array([0, 1], pa.int64()), "s": pa.array(["bad", "1"])})
    )
    sorted_ = t.sort("x", lambda r: r.s.cast("int32"))
    with pytest.raises(ValueError, match="Cast error"):
        sorted_.to_arrow()
    filtered = after(sorted_)
    logical, _ = filtered.explain_plan()
    lines = logical.splitlines()
    barrier = next(i for i, l in enumerate(lines) if "DemandBarrier" in l)
    moved = next(i for i, l in enumerate(lines) if "x != Int64(0)" in l)
    cast = next(i for i, l in enumerate(lines) if "opaque_cast_Int32" in l)
    assert moved < barrier < cast, logical
    with pytest.raises(ValueError, match="Cast error"):
        filtered.to_arrow()


def test_a_filter_after_a_sort_by_computed_keys_that_cannot_fail_still_moves(engine_layout):
    """R4-F1. Where no key can fail, the sort is moved above the filter as
    before, with no barrier, and ties on `x` keep the order of the second
    key and then of the input."""
    n = 60
    t = LTSeq.from_arrow(
        pa.table({"x": [i % 3 for i in range(n)], "i": list(range(n))})
    )
    filtered = t.sort("x", lambda r: 3 - r.i % 4).filter(lambda r: r.i % 5 != 0)
    logical, _ = filtered.explain_plan()
    assert "DemandBarrier" not in logical, logical
    result = filtered.to_arrow()
    rows = list(zip(result.column("x").to_pylist(), result.column("i").to_pylist()))
    expected = sorted(
        ((x, i) for x, i in zip([i % 3 for i in range(n)], range(n)) if i % 5 != 0),
        key=lambda row: (row[0], 3 - row[1] % 4, row[1]),
    )
    assert rows == expected


def test_a_projection_after_a_sort_keeps_its_computed_keys(engine_layout):
    """R5-F1. Reading a sorted table moves the stable sort above a `derive`
    after it (R2-F4), and DataFusion's plan builder rewrote the sort's keys
    in terms of the projection below it by name. A cast is named as its
    argument is, so `cast(s)` became the column passing `s` through: the
    rows came back ordered by `s` as strings, and a cast that fails was
    never evaluated."""
    t = LTSeq.from_arrow(
        pa.table({"x": pa.array([0, 0, 1], pa.int64()), "s": ["10", "9", "1"]})
    )
    derived = t.sort("x", lambda r: r.s.cast("int32")).derive(y=lambda r: r.x + 1)
    assert derived.to_arrow().column("s").to_pylist() == ["9", "10", "1"]
    t = LTSeq.from_arrow(pa.table({"x": pa.array([0, 1], pa.int64()), "s": ["bad", "2"]}))
    with pytest.raises(ValueError, match="Cast error"):
        t.sort("x", lambda r: r.s.cast("int32")).derive(y=lambda r: r.x + 1).to_arrow()


@pytest.mark.parametrize(
    "limited",
    [
        pytest.param(lambda t: t.head(0), id="head_0"),
        pytest.param(lambda t: t.filter(lambda r: r.x != 0).head(0), id="filter_head_0"),
        pytest.param(
            lambda t: t.derive(y=lambda r: r.x + 1).head(0), id="derive_head_0"
        ),
    ],
)
@pytest.mark.parametrize("terminal", ["to_arrow", "count"])
@pytest.mark.parametrize("s, fails", [(["1", "2"], False), (["bad", "2"], True)])
def test_head_0_of_a_sort_evaluates_its_keys(engine_layout, limited, terminal, s, fails):
    """R5-F1. `eliminate_limit` keeps a sort whose key can fail under
    `head(0)`, since the sort demands its keys on every row of its input.
    DataFusion then gave the sort the limit's fetch of 0 and planned a TopK
    of no rows, which panicked (`k > 0`) whether or not a key failed.
    `ltseq_no_zero_fetch_sorts` plans it as a whole sort under a limit of
    0 rows: no rows come back where every key converts, and the cast's
    error is raised where one does not. `count()` is not answered from
    the limit's exact row count either (`aggregate_statistics`)."""
    t = LTSeq.from_arrow(pa.table({"x": pa.array([0, 1], pa.int64()), "s": s}))
    result = limited(t.sort("x", lambda r: r.s.cast("int32")))
    run = (lambda: result.to_arrow().num_rows) if terminal == "to_arrow" else result.count
    if fails:
        with pytest.raises(ValueError, match="Cast error"):
            run()
    else:
        assert run() == 0


def test_a_window_over_ties_reads_rows_in_the_stable_order(engine_layout):
    """R2-F4, `EnsureRequirements` (X5). A window ordered by the declared
    key `k` asks its input only for `k`, and DataFusion met that with an
    unstable sort by `k` in place of the stable one, so `shift(1)` read
    whichever tied row that sort put first, even at one partition.
    `ltseq_keep_stable_sorts` keeps the sort by `k` and position, so each
    row reads the row before it in the input among those equal on `k`."""
    n = 120
    t = LTSeq.from_arrow(pa.table({"k": [i % 3 for i in range(n)], "i": list(range(n))}))
    result = t.sort("k").derive(p=lambda r: r.i.shift(1)).to_arrow()
    k = result.column("k").to_pylist()
    i = result.column("i").to_pylist()
    p = result.column("p").to_pylist()
    assert list(zip(k, i)) == sorted(zip(k, i))
    assert p == [None] + i[:-1]


@pytest.mark.parametrize(
    "clause, expected",
    [
        pytest.param("(-x & x) = 0", [], id="neg_and"),
        pytest.param("(-x & x) = 2", [2], id="neg_and_kept"),
        pytest.param("(-x | x) = -1", [3], id="neg_or"),
        pytest.param("(-x ^ x) = -1", [], id="neg_xor"),
        pytest.param("-(x & 3) = -2", [2], id="negated_and"),
    ],
)
def test_sql_bitwise_operations_keep_their_value(engine_layout, clause, expected):
    """R3-F1. `filter_where`'s SQL writes bitwise operators, and the
    simplifier's identities for them are wrong in value: `-A & A` became 0,
    `-A | A` and `-A ^ A` became -1, and `-(A & B)` became `-A | -B`, each
    on a column that is not nullable. `(-x & x) = 0` became TRUE, the filter
    was removed and every row came back. The guarded simplifier now keeps
    every bitwise operation."""
    schema = pa.schema([pa.field("x", pa.int64(), nullable=False)])
    t = LTSeq.from_arrow(pa.table({"x": [2, 3, 4]}, schema=schema))
    kept = LTSeq._from_inner(t._inner.filter_where(clause))
    logical, _ = kept.explain_plan()
    assert "Filter:" in logical and "x" in logical.split("Filter:")[1], logical
    # The table declares no order, so its rows come in any order (#148).
    assert sorted(kept.to_arrow().column("x").to_pylist()) == expected


@pytest.mark.parametrize("clause", ["(x ^ x) IS NULL", "(x ^ y ^ x) IS NULL"])
def test_sql_xor_of_a_null_is_null(engine_layout, clause):
    """R3-F1. XOR cancellation (`A ^ A` to 0, `A ^ B ^ A` to `B`) has no
    nullability gate, so a NULL `x` gave 0 or `y` instead of NULL."""
    t = LTSeq.from_arrow(
        pa.table({"x": pa.array([None, 2], pa.int64()), "y": pa.array([1, 1], pa.int64())})
    )
    kept = LTSeq._from_inner(t._inner.filter_where(clause))
    assert kept.to_arrow().column("x").to_pylist() == [None]


def test_a_case_branch_that_can_fail_stays_conditional(engine_layout):
    """R3-F2. A `CASE` evaluates a branch only on the rows that reach it.
    The simplifier turned `CASE WHEN x = 0 THEN false ELSE CAST(s AS
    INTEGER) > 0 END` into `x != 0 AND CAST(s AS INTEGER) > 0`, which keeps
    the one cast but evaluates it on every row, and raised on `'abc'`,
    which no row demands. The guard tells an occurrence in a branch from
    one evaluated on every row, so it keeps the `CASE` here, whether SQL or
    `if_else` writes it."""
    from ltseq.expr import if_else

    t = LTSeq.from_arrow(
        pa.table({"x": pa.array([0, 1], pa.int64()), "s": pa.array(["abc", "1"])})
    )
    clause = "CASE WHEN x = 0 THEN false ELSE CAST(s AS INTEGER) > 0 END"
    for kept in (
        LTSeq._from_inner(t._inner.filter_where(clause)),
        t.filter(lambda r: if_else(r.x == 0, False, r.s.cast("int32") > 0)),
    ):
        logical, _ = kept.explain_plan()
        assert "CASE" in logical and "opaque_cast_Int32" in logical, logical
        assert kept.to_arrow().column("x").to_pylist() == [1]


def test_a_case_nested_in_a_branch_stays_conditional(engine_layout):
    """R6-F1. The cast is in a branch of the outer `CASE` both before and
    after the innermost `CASE` becomes `x != 0 AND CAST(s AS INTEGER) > 0`,
    so a guard that only told a branch from every row accepted the rewrite,
    and the cast was evaluated on every row with `z`, raising on `'abc'`,
    which no branch reaches. The guard counts how deep the branch is, so the
    innermost `CASE` is kept and the row with `x = 1` comes back."""
    t = LTSeq.from_arrow(
        pa.table(
            {
                "x": pa.array([0, 1], pa.int64()),
                "s": pa.array(["abc", "1"]),
                "z": pa.array([True, True]),
            }
        )
    )
    clause = (
        "CASE WHEN z THEN CASE WHEN (CASE WHEN x = 0 THEN false"
        " ELSE CAST(s AS INTEGER) > 0 END) THEN 1 ELSE 0 END ELSE 0 END > 0"
    )
    kept = LTSeq._from_inner(t._inner.filter_where(clause))
    logical, _ = kept.explain_plan()
    assert "THEN Boolean(false) ELSE opaque_cast_Int32" in logical, logical
    assert kept.to_arrow().column("x").to_pylist() == [1]


LAST, FIRST = 2**63 - 1, -(2**63)


def timestamps(values):
    """A table of one non-nullable `timestamp[us]` column `ts`."""
    field = pa.field("ts", pa.timestamp("us"), nullable=False)
    return LTSeq.from_arrow(
        pa.table({"ts": pa.array(values, pa.timestamp("us"))}, schema=pa.schema([field]))
    )


@pytest.mark.parametrize(
    "clause, edge, rows",
    [
        ("(ts + INTERVAL '1 day') = (ts + INTERVAL '1 day')", LAST, 2),
        ("(ts + INTERVAL '1 day') IS NULL", LAST, 0),
        ("(ts - INTERVAL '1 day') IS NOT NULL", FIRST, 2),
    ],
)
def test_temporal_arithmetic_that_fails_keeps_its_error(engine_layout, clause, edge, rows):
    """R6-F2. Arrow checks timestamp arithmetic even where integer
    arithmetic wraps: a day past the last timestamp, or before the first,
    raises. Read as infallible, `v = v` became TRUE and `v IS NULL` FALSE
    on a non-nullable `ts`, so the filter was removed, or the relation
    emptied, without evaluating `v`. Both now raise where a row is at the
    edge, and keep their value where every row has a day on either side."""
    failing = LTSeq._from_inner(timestamps([edge, 0])._inner.filter_where(clause))
    with pytest.raises(Exception, match="out of range"):
        failing.to_arrow()
    fine = LTSeq._from_inner(timestamps([0, 1])._inner.filter_where(clause))
    assert fine.to_arrow().num_rows == rows


def decimals(values):
    """A table of one non-nullable `decimal128(38, 0)` column `x`."""
    field = pa.field("x", pa.decimal128(38, 0), nullable=False)
    return LTSeq.from_arrow(
        pa.table({"x": pa.array(values, pa.decimal128(38, 0))}, schema=pa.schema([field]))
    )


@pytest.mark.parametrize(
    "clause, rows",
    [
        ("x / 2 > 0", 2),
        ("(x / 2) = (x / 2)", 2),
        ("(x / 2) IS NULL", 0),
        ("(x / 2) IS NOT NULL", 2),
        ("(x % 2) = (x % 2)", 2),
    ],
)
def test_decimal_division_by_a_constant_keeps_its_error(engine_layout, clause, rows):
    """R7-F1. A decimal division scales its numerator before it divides,
    which overflows at `10**38 - 1` even when the divisor is 2. Read as
    infallible because its divisor is a safe constant, `v = v` became TRUE
    and `v IS NULL` FALSE on a non-nullable `x`, so the filter was removed,
    or the relation emptied, without evaluating `v`. Each now raises the
    overflow `x / 2 > 0` raises, and keeps its value where none overflows."""
    from decimal import Decimal

    if "/" in clause:
        failing = LTSeq._from_inner(
            decimals([Decimal(10**38 - 1), Decimal(1)])._inner.filter_where(clause)
        )
        logical, _ = failing.explain_plan()
        assert "Filter:" in logical and " / " in logical, logical
        with pytest.raises(Exception, match="(?i)overflow"):
            failing.to_arrow()
    fine = LTSeq._from_inner(decimals([Decimal(2), Decimal(1)])._inner.filter_where(clause))
    logical, _ = fine.explain_plan()
    assert "Filter:" in logical, logical
    assert fine.to_arrow().num_rows == rows
