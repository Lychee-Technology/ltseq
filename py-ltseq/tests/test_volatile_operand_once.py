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

import datetime
import math

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else, rand

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
