"""Tests for newly added functions: string (pos/left/right/lstrip/rstrip/asc/str_char/concat_ws),
datetime (dt_add with hours/minutes/seconds, dt_diff with unit, dt_age),
and math (gcd, lcm, factorial).
"""

import pytest
from ltseq import LTSeq
from ltseq.expr import concat_ws, factorial, gcd, lcm, str_char


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_table(**kwargs):
    """Create a single-row LTSeq from keyword column values."""
    rows = [dict(zip(kwargs.keys(), vals)) for vals in zip(*kwargs.values())]
    import pyarrow as pa
    arrays = {k: pa.array(v) for k, v in kwargs.items()}
    return LTSeq.from_arrow(pa.table(arrays))


# ===========================================================================
# String: pos
# ===========================================================================

class TestStrPos:
    def test_pos_found(self):
        t = make_table(email=["user@example.com", "admin@test.org"])
        result = t.derive(at_pos=lambda r: r.email.s.pos("@")).collect()
        assert result[0]["at_pos"] == 5   # "user@..." → position 5 (1-based)
        assert result[1]["at_pos"] == 6   # "admin@..." → position 6

    def test_pos_not_found(self):
        t = make_table(name=["hello", "world"])
        result = t.derive(p=lambda r: r.name.s.pos("@")).collect()
        assert result[0]["p"] == 0
        assert result[1]["p"] == 0

    def test_pos_first_char(self):
        t = make_table(s=["abc"])
        result = t.derive(p=lambda r: r.s.s.pos("a")).collect()
        assert result[0]["p"] == 1  # 1-based


# ===========================================================================
# String: left / right
# ===========================================================================

class TestStrLeft:
    def test_left_basic(self):
        t = make_table(code=["ABC123", "XY9"])
        result = t.derive(prefix=lambda r: r.code.s.left(3)).collect()
        assert result[0]["prefix"] == "ABC"
        assert result[1]["prefix"] == "XY9"

    def test_left_zero(self):
        t = make_table(s=["hello"])
        result = t.derive(p=lambda r: r.s.s.left(0)).collect()
        assert result[0]["p"] == ""

    def test_left_exceeds_length(self):
        t = make_table(s=["hi"])
        result = t.derive(p=lambda r: r.s.s.left(10)).collect()
        assert result[0]["p"] == "hi"


class TestStrRight:
    def test_right_basic(self):
        t = make_table(code=["ABC123", "XY9"])
        result = t.derive(suffix=lambda r: r.code.s.right(3)).collect()
        assert result[0]["suffix"] == "123"
        assert result[1]["suffix"] == "XY9"

    def test_right_zero(self):
        t = make_table(s=["hello"])
        result = t.derive(p=lambda r: r.s.s.right(0)).collect()
        assert result[0]["p"] == ""


# ===========================================================================
# String: lstrip / rstrip
# ===========================================================================

class TestStrLstrip:
    def test_lstrip_spaces(self):
        t = make_table(s=["  hello", "  world  "])
        result = t.derive(clean=lambda r: r.s.s.lstrip()).collect()
        assert result[0]["clean"] == "hello"
        assert result[1]["clean"] == "world  "

    def test_lstrip_no_leading(self):
        t = make_table(s=["hello  "])
        result = t.derive(clean=lambda r: r.s.s.lstrip()).collect()
        assert result[0]["clean"] == "hello  "


class TestStrRstrip:
    def test_rstrip_spaces(self):
        t = make_table(s=["hello  ", "  world  "])
        result = t.derive(clean=lambda r: r.s.s.rstrip()).collect()
        assert result[0]["clean"] == "hello"
        assert result[1]["clean"] == "  world"

    def test_rstrip_no_trailing(self):
        t = make_table(s=["  hello"])
        result = t.derive(clean=lambda r: r.s.s.rstrip()).collect()
        assert result[0]["clean"] == "  hello"


# ===========================================================================
# String: asc (ASCII code)
# ===========================================================================

class TestStrAsc:
    def test_asc_uppercase(self):
        t = make_table(ch=["A", "B", "Z"])
        result = t.derive(code=lambda r: r.ch.s.asc()).collect()
        assert result[0]["code"] == 65  # ord('A')
        assert result[1]["code"] == 66  # ord('B')
        assert result[2]["code"] == 90  # ord('Z')

    def test_asc_lowercase(self):
        t = make_table(ch=["a"])
        result = t.derive(code=lambda r: r.ch.s.asc()).collect()
        assert result[0]["code"] == 97   # ord('a')

    def test_asc_uses_first_char(self):
        t = make_table(s=["Hello"])
        result = t.derive(code=lambda r: r.s.s.asc()).collect()
        assert result[0]["code"] == 72   # ord('H')


# ===========================================================================
# String: str_char (global function)
# ===========================================================================

class TestStrChar:
    def test_str_char_basic(self):
        t = make_table(n=[65, 66, 97])
        result = t.derive(ch=lambda r: str_char(r.n)).collect()
        assert result[0]["ch"] == "A"
        assert result[1]["ch"] == "B"
        assert result[2]["ch"] == "a"

    def test_asc_and_char_roundtrip(self):
        t = make_table(s=["X"])
        result = t.derive(
            code=lambda r: r.s.s.asc(),
            back=lambda r: str_char(r.s.s.asc()),
        ).collect()
        assert result[0]["code"] == 88
        assert result[0]["back"] == "X"


# ===========================================================================
# String: concat_ws (global function)
# ===========================================================================

class TestConcatWs:
    def test_concat_ws_basic(self):
        t = make_table(first=["John", "Jane"], last=["Doe", "Smith"])
        result = t.derive(full=lambda r: concat_ws(" ", r.first, r.last)).collect()
        assert result[0]["full"] == "John Doe"
        assert result[1]["full"] == "Jane Smith"

    def test_concat_ws_comma(self):
        t = make_table(a=["x"], b=["y"], c=["z"])
        result = t.derive(s=lambda r: concat_ws(",", r.a, r.b, r.c)).collect()
        assert result[0]["s"] == "x,y,z"

    def test_concat_ws_empty_delimiter(self):
        t = make_table(a=["he"], b=["llo"])
        result = t.derive(s=lambda r: concat_ws("", r.a, r.b)).collect()
        assert result[0]["s"] == "hello"


# ===========================================================================
# DateTime: dt_add with hours/minutes/seconds/weeks
# ===========================================================================

class TestDtAddExtended:
    def test_add_hours(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "ts": pa.array([datetime.datetime(2024, 1, 1, 10, 0, 0)], type=pa.timestamp("s")),
        }))
        result = t.derive(ts2=lambda r: r.ts.dt.add(hours=2)).collect()
        assert "12:00:00" in str(result[0]["ts2"])

    def test_add_minutes(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "ts": pa.array([datetime.datetime(2024, 1, 1, 10, 0, 0)], type=pa.timestamp("s")),
        }))
        result = t.derive(ts2=lambda r: r.ts.dt.add(minutes=30)).collect()
        assert "10:30:00" in str(result[0]["ts2"])

    def test_add_weeks(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "d": pa.array([datetime.date(2024, 1, 1)], type=pa.date32()),
        }))
        result = t.derive(d2=lambda r: r.d.dt.add(weeks=1)).collect()
        # 2024-01-01 + 7 days = 2024-01-08
        assert "2024-01-08" in str(result[0]["d2"])

    def test_add_mixed(self):
        """Add days, months, years together (existing behaviour still works)."""
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "d": pa.array([datetime.date(2024, 1, 15)], type=pa.date32()),
        }))
        result = t.derive(d2=lambda r: r.d.dt.add(days=10, months=1)).collect()
        assert "2024-02-25" in str(result[0]["d2"])


# ===========================================================================
# DateTime: dt_diff with unit
# ===========================================================================

class TestDtDiffUnit:
    def test_diff_day_default(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "d1": pa.array([datetime.date(2024, 1, 31)], type=pa.date32()),
            "d2": pa.array([datetime.date(2024, 1, 1)], type=pa.date32()),
        }))
        result = t.derive(diff=lambda r: r.d1.dt.diff(r.d2)).collect()
        assert result[0]["diff"] == 30

    def test_diff_month(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "d1": pa.array([datetime.date(2024, 4, 1)], type=pa.date32()),
            "d2": pa.array([datetime.date(2024, 1, 1)], type=pa.date32()),
        }))
        result = t.derive(diff=lambda r: r.d1.dt.diff(r.d2, unit="month")).collect()
        assert result[0]["diff"] == 3

    def test_diff_year(self):
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "d1": pa.array([datetime.date(2030, 1, 1)], type=pa.date32()),
            "d2": pa.array([datetime.date(2024, 1, 1)], type=pa.date32()),
        }))
        result = t.derive(diff=lambda r: r.d1.dt.diff(r.d2, unit="year")).collect()
        assert result[0]["diff"] == 6


# ===========================================================================
# DateTime: dt_age
# ===========================================================================

class TestDtAge:
    def test_age_past_date(self):
        """Age of a birth date far in the past should be > 0."""
        import datetime
        import pyarrow as pa
        t = LTSeq.from_arrow(pa.table({
            "birth": pa.array([datetime.date(1990, 1, 1)], type=pa.date32()),
        }))
        result = t.derive(age=lambda r: r.birth.dt.age()).collect()
        assert result[0]["age"] >= 34  # At least 34 years since 1990

    def test_age_recent(self):
        """Age of a future date should be 0 or negative."""
        import datetime
        import pyarrow as pa
        future_date = datetime.date.today().replace(year=datetime.date.today().year + 1)
        t = LTSeq.from_arrow(pa.table({
            "d": pa.array([future_date], type=pa.date32()),
        }))
        result = t.derive(age=lambda r: r.d.dt.age()).collect()
        assert result[0]["age"] <= 0


# ===========================================================================
# Math: gcd
# ===========================================================================

class TestGcd:
    def test_gcd_basic(self):
        t = make_table(a=[12, 15, 100], b=[8, 25, 75])
        result = t.derive(g=lambda r: gcd(r.a, r.b)).collect()
        assert result[0]["g"] == 4
        assert result[1]["g"] == 5
        assert result[2]["g"] == 25

    def test_gcd_coprime(self):
        t = make_table(a=[7], b=[13])
        result = t.derive(g=lambda r: gcd(r.a, r.b)).collect()
        assert result[0]["g"] == 1

    def test_gcd_same(self):
        t = make_table(a=[6], b=[6])
        result = t.derive(g=lambda r: gcd(r.a, r.b)).collect()
        assert result[0]["g"] == 6


# ===========================================================================
# Math: lcm
# ===========================================================================

class TestLcm:
    def test_lcm_basic(self):
        t = make_table(a=[4, 3], b=[6, 5])
        result = t.derive(l=lambda r: lcm(r.a, r.b)).collect()
        assert result[0]["l"] == 12
        assert result[1]["l"] == 15

    def test_lcm_coprime(self):
        t = make_table(a=[7], b=[13])
        result = t.derive(l=lambda r: lcm(r.a, r.b)).collect()
        assert result[0]["l"] == 91

    def test_lcm_same(self):
        t = make_table(a=[6], b=[6])
        result = t.derive(l=lambda r: lcm(r.a, r.b)).collect()
        assert result[0]["l"] == 6


# ===========================================================================
# Math: factorial
# ===========================================================================

class TestFactorial:
    def test_factorial_basic(self):
        t = make_table(n=[0, 1, 5, 10])
        result = t.derive(f=lambda r: factorial(r.n)).collect()
        assert result[0]["f"] == 1
        assert result[1]["f"] == 1
        assert result[2]["f"] == 120
        assert result[3]["f"] == 3628800

    def test_factorial_in_filter(self):
        t = make_table(n=[3, 4, 5, 6])
        result = t.filter(lambda r: factorial(r.n) > 100).collect()
        assert len(result) == 2  # 5! = 120, 6! = 720
        assert all(r["n"] >= 5 for r in result)
