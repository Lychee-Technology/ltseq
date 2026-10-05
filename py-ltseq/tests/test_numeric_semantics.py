"""Integer overflow policy and Decimal128 coverage (#154).

Integer `+ - *` and the SUM family wrap on overflow (two's complement, as
in NumPy and Polars) and raise nothing; `docs/api.md` documents this. These
tests pin that policy per execution path and the documented exceptions:
integer `/` and `abs()` raise on `i64::MIN`, `avg` is computed in floating
point, and `search_pattern` predicates raise because they run on a separate
evaluator (#221 tracks aligning it and the checked-arithmetic question).
"""

from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq, sum_if

BIG = 2**62
I64_MAX = 2**63 - 1
I64_MIN = -(2**63)


def _int64_table(**columns):
    return LTSeq.from_arrow(
        pa.table({name: pa.array(values, pa.int64()) for name, values in columns.items()})
    )


@pytest.fixture
def big():
    # Two rows of 2**62: their sum, or either one times 4, leaves int64.
    return _int64_table(i=[0, 1], x=[BIG, BIG]).sort("i")


def _col(table, name):
    return table.to_arrow().column(name).to_pylist()


class TestIntegerOverflowWraps:
    def test_arithmetic(self, big):
        out = big.derive(
            add=lambda r: r.x + r.x,
            mul=lambda r: r.x * 4,
            rmul=lambda r: 4 * r.x,
            sub=lambda r: (0 - r.x) - r.x - 1,
        )
        assert _col(out, "add") == [I64_MIN, I64_MIN]
        assert _col(out, "mul") == [0, 0]
        assert _col(out, "rmul") == [0, 0]
        assert _col(out, "sub") == [I64_MAX, I64_MAX]

    def test_filter_and_search_first_see_the_wrapped_value(self, big):
        assert _col(big.filter(lambda r: r.x * 4 == 0), "i") == [0, 1]
        assert _col(big.search_first(lambda r: r.x * 4 == 0), "i") == [0]

    def test_sum_aggregates(self, big):
        assert _col(big.agg(s=lambda g: g.x.sum()), "s") == [I64_MIN]
        grouped = big.derive(k=lambda r: r.i * 0).group_by("k").agg(s=lambda g: g.x.sum())
        assert _col(grouped, "s") == [I64_MIN]
        assert _col(big.agg(s=lambda g: sum_if(g.x > 0, g.x)), "s") == [I64_MIN]

    def test_cumulative_and_rolling_sums(self, big):
        assert _col(big.cum_sum("x"), "x_cumsum") == [BIG, I64_MIN]
        assert _col(big.derive(c=lambda r: r.x.cum_sum()), "c") == [BIG, I64_MIN]
        assert _col(big.derive(c=lambda r: r.x.rolling(2).sum()), "c") == [BIG, I64_MIN]

    def test_diff(self):
        t = _int64_table(i=[0, 1], x=[I64_MIN, I64_MAX]).sort("i")
        assert _col(t.derive(d=lambda r: r.x.diff()), "d") == [None, -1]

    def test_group_window_sum(self, big):
        nested = big.derive(k=lambda r: r.i * 0).sort("i").group_ordered(lambda r: r.k)
        assert _col(nested.derive(lambda g: {"s": g.sum("x")}), "s") == [I64_MIN, I64_MIN]

    def test_avg_does_not_wrap(self, big):
        assert _col(big.agg(a=lambda g: g.x.avg()), "a") == [float(BIG)]

    def test_cast_to_float_avoids_wrapping(self, big):
        assert _col(big.derive(y=lambda r: r.x.cast("float64") * 4), "y") == [4.0 * BIG] * 2
        floats = big.derive(xf=lambda r: r.x.cast("float64"))
        assert _col(floats.agg(s=lambda g: g.xf.sum()), "s") == [2.0 * BIG]


class TestIntegerOverflowRaises:
    """The documented exceptions to wrapping."""

    def test_division_of_min_by_minus_one(self):
        t = _int64_table(x=[I64_MIN])
        with pytest.raises(ValueError, match="[Oo]verflow"):
            t.derive(y=lambda r: r.x / -1).to_arrow()
        with pytest.raises(ValueError, match="[Oo]verflow"):
            t.derive(y=lambda r: r.x // -1).to_arrow()

    def test_abs_of_min(self):
        t = _int64_table(x=[I64_MIN])
        with pytest.raises(ValueError, match="overflow"):
            t.derive(y=lambda r: r.x.abs()).to_arrow()

    def test_search_pattern_predicate(self, big):
        # search_pattern evaluates predicates with checked kernels (#221).
        with pytest.raises(RuntimeError, match="[Oo]verflow"):
            big.search_pattern(lambda r: r.x * 4 == 0)


class TestDecimal:
    @pytest.fixture
    def dec(self):
        return LTSeq.from_arrow(
            pa.table(
                {
                    "g": ["a", "a", "b"],
                    "d": pa.array([Decimal("1.10"), Decimal("2.25"), None], pa.decimal128(10, 2)),
                }
            )
        )

    def test_schema_and_values(self, dec):
        assert dec.schema["d"] == "decimal"
        assert dec.to_dicts() == [
            {"g": "a", "d": Decimal("1.10")},
            {"g": "a", "d": Decimal("2.25")},
            {"g": "b", "d": None},
        ]

    def test_aggregates_are_exact(self, dec):
        out = dec.agg(s=lambda g: g.d.sum(), a=lambda g: g.d.avg(), c=lambda g: g.d.count())
        assert out.to_dicts() == [{"s": Decimal("3.35"), "a": Decimal("1.675"), "c": 2}]
        grouped = dec.group_by("g").agg(s=lambda g: g.d.sum())
        assert {r["g"]: r["s"] for r in grouped.to_dicts()} == {"a": Decimal("3.35"), "b": None}

    def test_arithmetic_is_exact(self, dec):
        out = dec.derive(x=lambda r: r.d * 2, y=lambda r: r.d + 1)
        assert [(r["x"], r["y"]) for r in out.to_dicts()] == [
            (Decimal("2.20"), Decimal("2.10")),
            (Decimal("4.50"), Decimal("3.25")),
            (None, None),
        ]

    def test_filter_against_int_and_decimal_literals(self, dec):
        assert [r["d"] for r in dec.filter(lambda r: r.d > 2).to_dicts()] == [Decimal("2.25")]
        assert [r["d"] for r in dec.filter(lambda r: r.d > Decimal("1.10")).to_dicts()] == [
            Decimal("2.25")
        ]

    def test_cum_sum(self, dec):
        out = dec.assume_sorted("g").cum_sum("d")
        assert _col(out, "d_cumsum") == [Decimal("1.10"), Decimal("3.35"), Decimal("3.35")]
