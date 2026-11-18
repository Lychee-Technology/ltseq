"""Each `rand()` is one value per row, however often a lowering writes it (#298).

DataFusion evaluates each copy of a volatile expression separately, and
`random()` draws again for each. Several lowerings write one operand into
the expression they build more than once: `between` is `x >= lo & x <= hi`,
`s.islower()` is `x = lower(x)`, `dt.diff(unit="month")` reads each operand
for its year and its month, `dt.age()` reads its operand's year and day of
year, `diff()` is `x - lag(x)`, `pct_change()` reads `x` three times,
`update` writes its predicate into one CASE per column, and `group_ordered`
compares its key with the previous row's key. With `rand()` in the operand,
each copy tested another draw.

The operations stage each `rand()` as a column beneath them, so a volatile
operand gives the answer of its staged form, the column a `derive` computes
once. Each probe draws from a small domain, and either has one answer over
it or checks that every row's answer is one the domain gives. A test that
read two draws gives other answers on a share of the rows, so with a few
hundred rows it fails every time.
"""

import collections
import datetime
import math

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else, rand
from ltseq.expr.types import ColumnExpr

# Odd, for the group_ordered and diff probes (see `pairs_table`).
ROWS = 401


def table(**columns) -> LTSeq:
    return LTSeq.from_arrow(pa.table({"k": list(range(ROWS)), **columns}))


def column(t: LTSeq, name: str = "out") -> list:
    return t.to_arrow().column(name).to_pylist()


def answers(f, values: list) -> set:
    """`f` on a column holding each value of the domain: the answers a
    staged operand gives."""
    domain = LTSeq.from_arrow(pa.table({"v": values}))
    return set(column(domain.derive(out=lambda r: f(r.v))))


def assert_answers_in_domain(t: LTSeq, draw, f, values: list) -> None:
    """Every row of `f` on the inline volatile operand gives an answer `f`
    gives for a value of the domain, as its staged form does."""
    allowed = answers(f, values)
    inline = set(column(t.derive(out=lambda r: f(draw(r)))))
    staged = set(column(t.derive(v=draw).derive(out=lambda r: f(r.v))))
    assert staged <= allowed
    assert inline <= allowed, f"inline gave {inline - allowed}, outside {allowed}"


HALF = (lambda r: rand().round(0) - 0.5, [-0.5, 0.5])


def test_between_reads_one_draw(engine_layout):
    """`between` writes its receiver twice; -0.5 or 0.5 is never between
    -0.1 and 0.1. Two draws gave 0.5 >= -0.1 and -0.5 <= 0.1 on a quarter of
    the rows."""
    draw, values = HALF
    t = table()
    assert t.filter(lambda r: draw(r).between(-0.1, 0.1)).count() == 0
    assert_answers_in_domain(t, draw, lambda x: x.between(-0.1, 0.1), values)


def test_between_on_two_draws_stays_two_draws():
    """Pinning the receiver keeps two `rand()` calls in it two values: the
    difference of two draws from {0, 1} is 0 on about half of the rows, not
    on all."""
    t = table()
    kept = t.filter(lambda r: (rand().round(0) - rand().round(0)).between(-0.1, 0.1)).count()
    assert 0 < kept < ROWS


def strings(lower: list[str]) -> LTSeq:
    a, b = lower
    return table(a=[a] * ROWS, b=[b] * ROWS)


@pytest.mark.parametrize(
    "method, values",
    [("islower", ["abc", "def"]), ("isupper", ["ABC", "DEF"])],
)
def test_case_tests_read_one_draw(method, values):
    """`s.islower()` is `x = lower(x)` and `s.isupper()` is `x = upper(x)`;
    each value of the domain passes. Two draws compared "abc" with "def"."""
    t = strings(values)

    def draw(r):
        return if_else(rand() > 0.5, r.a, r.b)

    def f(x):
        return getattr(x.s, method)()

    assert t.filter(lambda r: f(draw(r))).count() == ROWS
    assert_answers_in_domain(t, draw, f, values)


def dates(d1: datetime.date, d2: datetime.date, **more) -> LTSeq:
    return table(d1=[d1] * ROWS, d2=[d2] * ROWS, **more)


def date_draw(r):
    return if_else(rand() > 0.5, r.d1, r.d2)


def test_month_diff_reads_one_draw():
    """`dt.diff(unit="month")` reads each operand for its year and for its
    month. 2020-03-15 and 2021-01-15 are 2 and 12 months after 2020-01-01;
    the year of one and the month of the other gave 0 or 14."""
    d1, d2 = datetime.date(2020, 3, 15), datetime.date(2021, 1, 15)
    base = datetime.date(2020, 1, 1)
    t = dates(d1, d2, base=[base] * ROWS)
    allowed = {2, 12}
    inline = set(column(t.derive(out=lambda r: date_draw(r).dt.diff(r.base, unit="month"))))
    assert inline <= allowed
    behind = set(column(t.derive(out=lambda r: r.base.dt.diff(date_draw(r), unit="month"))))
    assert behind <= {-2, -12}


def test_age_reads_one_draw():
    """`dt.age()` reads its operand's year and day of year. On January 1st
    ten years ago and December 31st twenty years ago, the year of one and the
    day of the other give ages neither date has (on most days of the year)."""
    year = datetime.date.today().year
    d1, d2 = datetime.date(year - 10, 1, 1), datetime.date(year - 20, 12, 31)
    t = dates(d1, d2)
    assert_answers_in_domain(t, date_draw, lambda x: x.dt.age(), [d1, d2])


def update_table() -> LTSeq:
    return table(u=[0] * ROWS, w=[0] * ROWS)


def test_update_tests_its_predicate_once(engine_layout):
    """`update` writes its predicate into one CASE per updated column; each
    row is updated in every column or in none."""
    got = update_table().update(lambda r: rand() > 0.5, u=1, w=1).to_arrow()
    u, w = got.column("u").to_pylist(), got.column("w").to_pylist()
    assert u == w
    assert 0 < sum(u) < ROWS
    assert got.column_names == ["k", "u", "w"]


def pairs_table() -> LTSeq:
    return table().sort("k")


def pairs_draw(r):
    """`(k + u) // 2` for a draw `u` of 0 or 1: row 2m is m, row 2m+1 is m or
    m+1, row 2m+2 is m+1. One of rows 2m+1 and 2m+2 starts a new value, and
    which depends on the draw of row 2m+1 alone. A probe that read row
    2m+1's value twice, with two draws, gets that wrong on half of the
    pairs, so it checks each pair rather than a total, which two draws
    reach by chance."""
    return ((r.k + rand().round(0)) // 2).cast("float64")


def test_window_diff_reads_one_draw(engine_layout):
    """`diff()` is `x - lag(x)`: with one draw per row the differences of
    rows 2m+1 and 2m+2 add up to 1, the step from row 2m to row 2m+2."""
    got = column(pairs_table().derive(out=lambda r: pairs_draw(r).diff()))
    assert got[0] is None
    assert [got[i] + got[i + 1] for i in range(1, ROWS, 2)] == [1] * (ROWS // 2)


def test_pct_change_reads_one_draw(engine_layout):
    """`pct_change()` reads its receiver three times; with one draw per row
    the ratios `1 + change` of rows 2m+1 and 2m+2 multiply up to the ratio
    of row 2m+2's value to row 2m's, `(m + 2) / (m + 1)`."""
    got = column(pairs_table().derive(out=lambda r: (pairs_draw(r) + 1).pct_change()))
    assert got[0] is None
    for m, i in enumerate(range(1, ROWS, 2)):
        ratio = (1 + got[i]) * (1 + got[i + 1])
        assert math.isclose(ratio, (m + 2) / (m + 1), rel_tol=1e-12), (i, got[i], got[i + 1])


def test_group_ordered_compares_each_draw_with_the_previous_one(engine_layout):
    """`group_ordered` on a key without a window function compares the key
    with the previous row's: each row's draw with the previous row's draw,
    as on a derived column. Each pair of rows 2m+1 and 2m+2 starts exactly
    one group, so there are `ROWS // 2 + 1` of them."""
    grouped = pairs_table().group_ordered(pairs_draw)
    groups = ROWS // 2 + 1
    flat = grouped.flatten()
    ids = column(flat, "__group_id__")
    assert [ids[i + 2] - ids[i] for i in range(0, ROWS - 1, 2)] == [1] * (ROWS // 2)
    assert grouped.first().count() == groups
    assert not any(name.startswith("__ltseq_draw_") for name in flat._schema)
    staged = pairs_table().derive(v=pairs_draw).group_ordered(lambda r: r.v)
    assert staged.first().count() == groups


def test_two_calls_are_two_draws():
    """Staging keeps each `rand()` written its own value."""
    t = table()
    assert t.filter(lambda r: rand() == rand()).count() == 0
    got = column(t.derive(out=lambda r: rand().round(0) - rand().round(0)))
    assert set(got) == {-1.0, 0.0, 1.0}


def test_staged_draws_leave_no_column_and_keep_the_order():
    """The staged columns are dropped, and each operation keeps the sort
    keys and row order it keeps without `rand()`."""
    t = table(x=[float(i) for i in range(ROWS)]).sort("k")
    for volatile, plain in (
        (t.filter(lambda r: rand() < 2.0), t.filter(lambda r: r.k >= 0)),
        (t.derive(y=lambda r: rand()), t.derive(y=lambda r: r.x)),
        (t.update(lambda r: rand() < 2.0, x=1.0), t.update(lambda r: r.k >= 0, x=1.0)),
        (t.delete(lambda r: rand() > 2.0), t.delete(lambda r: r.k < 0)),
    ):
        assert volatile._schema == plain._schema
        assert volatile.sort_keys == plain.sort_keys
        assert column(volatile, "k") == list(range(ROWS))
    assert t.select(lambda r: [r.k, rand().round(0)]).to_arrow().num_columns == 2


def test_a_draw_takes_a_name_no_column_has():
    """A column or derived column named like a staged draw is kept."""
    t = LTSeq.from_arrow(pa.table({"__ltseq_draw_0": [1, 2, 3]}))
    got = t.derive(__ltseq_draw_1=lambda r: rand() < 2.0).to_arrow()
    assert got.column_names == ["__ltseq_draw_0", "__ltseq_draw_1"]
    assert got.column("__ltseq_draw_0").to_pylist() == [1, 2, 3]
    assert got.column("__ltseq_draw_1").to_pylist() == [True] * 3
    kept = t.filter(lambda r: rand() < 2.0).to_arrow()
    assert kept.column_names == ["__ltseq_draw_0"]


# Review round 1 of #413.

NEVER = (lambda r: (rand().round(0) - 0.5).between(-0.1, 0.1))


def test_between_on_an_explicit_window_reads_one_draw(engine_layout):
    """R1-F1: `between` pins a `rand()` inside `.over(...)` too. Neither
    -0.5 nor 0.5 is between -0.1 and 0.1, inline or staged."""
    t = pairs_table()

    def draw(r):
        return rand().shift(0).over(order_by=r.k).round(0) - 0.5

    inline = column(t.derive(out=lambda r: draw(r).between(-0.1, 0.1)))
    staged = column(t.derive(v=draw).derive(out=lambda r: r.v.between(-0.1, 0.1)))
    assert not any(staged)
    assert not any(inline)


def test_window_partition_and_order_keys_are_pinned():
    """R1-F1: pinning reaches a window's keys, and keeps two calls two."""
    from ltseq.expr.base import _pinned
    from ltseq.expr.types import SharedDrawExpr

    t = table()
    captured = []
    t.derive(out=lambda r: captured.append(
        rand().shift(1).over(partition_by=rand().round(0), order_by=rand())
    ) or r.k)
    pinned = _pinned(captured[0])
    draws = [pinned.expr.on, pinned.partition_by.on, pinned.order_by]
    assert all(isinstance(d, SharedDrawExpr) for d in draws)
    assert len({d.draw for d in draws}) == 3


def test_search_first_reads_one_draw():
    """R1-F2: `search_first` finds no row where `filter` keeps none."""
    t = pairs_table()
    assert t.filter(NEVER).count() == 0
    assert t.search_first(NEVER).count() == 0
    assert t.search_first(lambda r: rand() < 2.0).to_arrow().column_names == ["k"]


def test_aggregate_keys_read_one_draw():
    """R1-F2: a computed `group_by` / `agg` key is false on every row, and
    keeps the name it has without staging."""
    t = pairs_table()
    plain = t.group_by(lambda r: (r.k * 0.0).between(-0.1, 0.1)).agg(n=lambda g: g.count())
    for got in (
        t.group_by(NEVER).agg(n=lambda g: g.count()),
        t.agg(by=NEVER, n=lambda g: g.count()),
    ):
        table_ = got.to_arrow()
        key = table_.column_names[0]
        assert table_.column(key).to_pylist() == [False]
        assert table_.column("n").to_pylist() == [ROWS]
        assert "random()" in key and "__ltseq_draw_" not in key
        assert list(got._schema) == [key, "n"]
    assert len(plain._schema) == 2


def test_sort_key_reads_one_draw(engine_layout):
    """R1-F2: a computed sort key that is false on every row leaves the order
    of a stable sort by `k` alone."""
    t = pairs_table()
    got = t.sort(NEVER, "k")
    assert column(got, "k") == list(range(ROWS))
    assert list(got._schema) == ["k"]
    assert got.sort_keys == t.sort(lambda r: r.k > ROWS, "k").sort_keys


def test_cum_sum_and_search_pattern_read_one_draw():
    """R1-F2: the other consumers of a row expression stage too."""
    t = pairs_table()
    sums = t.cum_sum(lambda r: if_else(NEVER(r), 1, 0))
    assert set(column(sums, sums.to_arrow().column_names[-1])) == {0}
    assert list(sums._schema)[0] == "k" and len(sums._schema) == 2
    # search_pattern's kernel takes no `round`; it reads a staged draw.
    always = (lambda r: (rand() - 0.5).between(-1.0, 1.0))
    assert t.search_pattern(always, lambda r: r.k >= 0).count() == ROWS - 1
    assert t.search_pattern_count(lambda r: r.k >= 0, always) == ROWS - 1
    assert t.search_pattern(always).to_arrow().column_names == ["k"]


def test_select_keeps_the_names_of_its_expressions():
    """R1-F3: a selected expression holding `rand()` is named as without
    staging, bare or computed."""
    t = table()
    assert list(t.select(lambda r: [r.k, rand()])._schema) == ["k", "random()"]
    computed = t.select(lambda r: rand().round(0))
    assert list(computed._schema) == ["round(random(),Int64(0))"]
    assert set(column(computed, "round(random(),Int64(0))")) <= {0.0, 1.0}
    assert t.select(lambda r: [r.k, rand()]).select("random()").count() == ROWS


def test_group_filter_keeps_the_draws_of_the_key(engine_layout):
    """R1-F4: rows 2m and 2m+1 have key 2m, or 2m and 2m+1 by the draw of
    row 2m+1. Keeping the groups of two keeps the pairs that drew 0, and
    grouping them again keeps those pairs whole, as for a derived key."""

    def key(r):
        return 2 * (r.k // 2) + (r.k % 2) * rand().round(0)

    t = pairs_table()

    def sizes(grouped):
        ids = column(grouped.filter(lambda g: g.count() == 2).flatten(), "__group_id__")
        return set(collections.Counter(ids).values())

    assert sizes(t.derive(v=key).group_ordered(lambda r: r.v)) == {2}
    kept = t.group_ordered(key).filter(lambda g: g.count() == 2)
    assert sizes(t.group_ordered(key)) == {2}
    flat = kept.flatten()
    assert set(collections.Counter(column(flat, "__group_id__")).values()) == {2}
    assert list(flat._schema) == ["k", "__group_id__", "__group_count__", "__rn__"]
    assert list(kept.to_pandas().columns) == ["k"]
    assert list(kept.first()._schema) == ["k"]
    # Each execution draws anew, so each check reads one execution.
    again = kept.filter(lambda g: g.count() == 2).flatten()
    assert set(collections.Counter(column(again, "__group_id__")).values()) == {2}
    assert list(again._schema) == list(flat._schema)


def test_a_draw_never_takes_a_name_the_expression_reads():
    """R1-F5: a missing column named like a staged draw stays missing."""
    t = table()
    missing = ColumnExpr("__ltseq_draw_0")
    with pytest.raises(Exception) as unstaged:
        t.derive(out=lambda r: missing + r.k).to_arrow()
    with pytest.raises(type(unstaged.value)):
        t.derive(out=lambda r: missing + rand()).to_arrow()
    with pytest.raises(Exception):
        t.filter(lambda r: missing > rand()).to_arrow()


@pytest.mark.parametrize(
    "predicate",
    [
        lambda g: g.sum("__ltseq_draw_0") >= 0,
        lambda g: g.first().__ltseq_draw_0 >= 0,
    ],
    ids=["aggregate", "first-row"],
)
def test_a_group_filter_never_reads_a_draw_of_the_key(predicate):
    """R3-F1: a group filter naming a missing column like a staged draw of
    the key reports the column missing, as it does for a key without
    `rand()`, in a first filter and in a filter of its result, whose rows
    keep the key's draws."""
    t = LTSeq.from_arrow(pa.table({"k": list(range(ROWS))})).sort("k")

    def volatile(r):
        return rand().round(0)

    def stable(r):
        return r.k // 2

    with pytest.raises(Exception) as unstaged:
        t.group_ordered(stable).filter(predicate).flatten().to_arrow()
    assert "__ltseq_draw_0" in str(unstaged.value)
    with pytest.raises(type(unstaged.value), match="__ltseq_draw_0"):
        t.group_ordered(volatile).filter(predicate).flatten().to_arrow()
    kept = t.group_ordered(volatile).filter(lambda g: g.count() >= 1)
    assert list(kept.to_pandas().columns) == ["k"]
    with pytest.raises(type(unstaged.value), match="__ltseq_draw_0"):
        kept.filter(predicate).flatten().to_arrow()
    # The renamed draws still key the groups that passed.
    again = kept.filter(lambda g: g.count() >= 1).flatten()
    assert list(again._schema) == ["k", "__group_id__", "__group_count__", "__rn__"]
    assert again.count() == ROWS
