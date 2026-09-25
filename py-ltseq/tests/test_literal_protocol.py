"""Typed literal protocol (issue #145).

Python literals cross the FFI boundary as typed payloads, never as strings that
Rust re-parses. Unsupported values fail when the literal is created inside the
user's lambda, naming the offending type; Decimal / date / datetime literals
travel as Decimal128 / Date32 / TimestampMicrosecond.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

import numpy as np
import pyarrow as pa
import pytest

from ltseq import LTSeq
from ltseq.expr import ColumnExpr, LiteralExpr


# ---------------------------------------------------------------------------
# Capture-time validation
# ---------------------------------------------------------------------------


@pytest.fixture
def ints():
    return LTSeq.from_arrow(pa.table({"a": pa.array([1, 2, 3], pa.int64())}))


@pytest.mark.parametrize(
    "value, type_name",
    [
        ([1, 2], "list"),
        ((1, 2), "tuple"),
        ({"k": 1}, "dict"),
        ({1, 2}, "set"),
        (b"raw", "bytes"),
        (1 + 2j, "complex"),
        (Fraction(1, 3), "fractions.Fraction"),
        (timedelta(days=1), "datetime.timedelta"),
        (np.bool_(True), "numpy.bool"),
        (object(), "object"),
    ],
)
def test_unsupported_literal_raises_type_error_naming_type(value, type_name):
    with pytest.raises(TypeError, match=rf"Unsupported literal type {type_name}\b"):
        LiteralExpr(value)


def test_unsupported_literal_fails_inside_lambda_capture(ints):
    """The error surfaces while the lambda is captured, not at collect time."""
    with pytest.raises(TypeError, match=r"Unsupported literal type list"):
        ints.derive(b=lambda r: r.a + [1, 2])
    with pytest.raises(TypeError, match=r"Unsupported literal type list"):
        ints.filter(lambda r: r.a > [1])


def test_error_lists_supported_types():
    with pytest.raises(TypeError, match=r"supported literal types: .*Decimal.*date"):
        LiteralExpr([1])


def test_pandas_nat_is_rejected_with_a_null_hint():
    pd = pytest.importorskip("pandas")
    with pytest.raises(ValueError, match="use None"):
        LiteralExpr(pd.NaT)


def test_pandas_timestamp_is_a_datetime_literal():
    pd = pytest.importorskip("pandas")
    serialized = LiteralExpr(pd.Timestamp("1970-01-01 00:00:01", tz="Asia/Tokyo")).serialize()
    assert serialized == {
        "type": "Literal",
        "value": -9 * 3600 * 1_000_000 + 1_000_000,
        "dtype": "TimestampMicrosecond",
        "tz": "UTC",
    }


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**70])
def test_int_out_of_int64_range_names_the_literal(value):
    with pytest.raises(ValueError, match=str(value)):
        LiteralExpr(value)


def test_int64_bounds_are_accepted():
    assert LiteralExpr(2**63 - 1).serialize()["value"] == 2**63 - 1
    assert LiteralExpr(-(2**63)).serialize()["value"] == -(2**63)


def test_out_of_range_int_fails_inside_lambda_capture(ints):
    with pytest.raises(ValueError, match=str(2**70)):
        ints.filter(lambda r: r.a > 2**70)


@pytest.mark.parametrize("value", ["NaN", "sNaN", "Infinity", "-Infinity"])
def test_non_finite_decimal_rejected(value):
    with pytest.raises(ValueError, match="finite"):
        LiteralExpr(Decimal(value))


def test_decimal_over_precision_38_rejected():
    with pytest.raises(ValueError, match="precision"):
        LiteralExpr(Decimal("1" * 39))


@pytest.mark.parametrize("value", [Decimal("1E+38"), Decimal("1E+5000"), Decimal("5E+100000")])
def test_decimal_over_precision_is_rejected_before_exponent_expansion(value):
    # The precision comes from the coefficient digits and the exponent; the
    # unscaled integer is never built, so a huge exponent fails the same way
    # a 39-digit value does instead of hitting Python's int-to-str limit.
    with pytest.raises(ValueError, match="needs precision"):
        LiteralExpr(value)


@pytest.mark.parametrize("value", [Decimal("0E+5000"), Decimal("-0E+5000")])
def test_zero_decimal_with_positive_exponent_is_plain_zero(value):
    assert LiteralExpr(value).serialize() == {
        "type": "Literal",
        "value": 0,
        "dtype": "Decimal128",
        "precision": 1,
        "scale": 0,
    }


# ---------------------------------------------------------------------------
# Serialization shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        (Decimal("1.5"), {"value": 15, "dtype": "Decimal128", "precision": 2, "scale": 1}),
        (Decimal("-0.05"), {"value": -5, "dtype": "Decimal128", "precision": 2, "scale": 2}),
        (Decimal("2"), {"value": 2, "dtype": "Decimal128", "precision": 1, "scale": 0}),
        (Decimal("1E+3"), {"value": 1000, "dtype": "Decimal128", "precision": 4, "scale": 0}),
        (Decimal("0"), {"value": 0, "dtype": "Decimal128", "precision": 1, "scale": 0}),
        (date(1970, 1, 2), {"value": 1, "dtype": "Date32"}),
        (date(1969, 12, 31), {"value": -1, "dtype": "Date32"}),
        (
            datetime(1970, 1, 1, 0, 0, 1, 5),
            {"value": 1_000_005, "dtype": "TimestampMicrosecond", "tz": None},
        ),
        (
            datetime(1970, 1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            {"value": 0, "dtype": "TimestampMicrosecond", "tz": "UTC"},
        ),
        (np.int32(7), {"value": 7, "dtype": "Int64"}),
        (np.float32(0.5), {"value": 0.5, "dtype": "Float64"}),
    ],
)
def test_typed_serialization(value, expected):
    serialized = LiteralExpr(value).serialize()
    assert serialized == {"type": "Literal", **expected}
    # Payload values are plain Python primitives, not the original objects.
    assert type(serialized["value"]) in (int, float, type(None))


# ---------------------------------------------------------------------------
# End to end: Decimal
# ---------------------------------------------------------------------------


@pytest.fixture
def floats():
    return LTSeq.from_arrow(pa.table({"x": pa.array([1.0, 1.5, 2.0, 2.5], pa.float64())}))


def test_decimal_compares_numerically_against_float_column(floats):
    out = floats.filter(lambda r: r.x > Decimal("1.5")).to_arrow()
    assert out.column("x").to_pylist() == [2.0, 2.5]


def test_decimal_integral_value_is_not_compared_as_string(ints):
    """`Decimal('2')` used to be stringified and cast to int by accident."""
    out = ints.filter(lambda r: r.a >= Decimal("2")).to_arrow()
    assert out.column("a").to_pylist() == [2, 3]


def test_decimal_fraction_against_int_column(ints):
    """Previously a cast error at collect time."""
    out = ints.filter(lambda r: r.a > Decimal("1.5")).to_arrow()
    assert out.column("a").to_pylist() == [2, 3]


def test_decimal_against_decimal_column():
    t = LTSeq.from_arrow(
        pa.table(
            {"p": pa.array([Decimal("1.10"), Decimal("1.25"), Decimal("9.99")], pa.decimal128(5, 2))}
        )
    )
    out = t.filter(lambda r: r.p > Decimal("1.2")).to_arrow()
    assert out.column("p").to_pylist() == [Decimal("1.25"), Decimal("9.99")]


def test_decimal_literal_derives_decimal_column(ints):
    out = ints.derive(d=lambda r: Decimal("1.25") + r.a * 0).to_arrow()
    assert pa.types.is_decimal(out.schema.field("d").type)
    assert out.column("d").to_pylist()[0] == Decimal("1.25")


def test_decimal_literal_alone_keeps_precision_and_scale(ints):
    out = ints.derive(d=lambda r: r.a.fill_null(0) * 0 + Decimal("12.345")).to_arrow()
    assert out.column("d").to_pylist() == [Decimal("12.345")] * 3


# ---------------------------------------------------------------------------
# End to end: date / datetime
# ---------------------------------------------------------------------------


@pytest.fixture
def dates():
    return LTSeq.from_arrow(
        pa.table(
            {
                "d": pa.array(
                    [date(2023, 12, 31), date(2024, 1, 1), date(2024, 6, 1)], pa.date32()
                ),
                "ts": pa.array(
                    [
                        datetime(2024, 1, 1, 0, 0),
                        datetime(2024, 1, 1, 12, 0),
                        datetime(2024, 1, 2, 0, 0),
                    ],
                    pa.timestamp("us"),
                ),
                "ts_ns": pa.array(
                    [
                        datetime(2024, 1, 1, 0, 0),
                        datetime(2024, 1, 1, 12, 0),
                        datetime(2024, 1, 2, 0, 0),
                    ],
                    pa.timestamp("ns"),
                ),
                "ts_utc": pa.array(
                    [
                        datetime(2024, 1, 1, 0, 0),
                        datetime(2024, 1, 1, 12, 0),
                        datetime(2024, 1, 2, 0, 0),
                    ],
                    pa.timestamp("us", tz="UTC"),
                ),
            }
        )
    )


def test_date_comparison_on_date32_column(dates):
    out = dates.filter(lambda r: r.d >= date(2024, 1, 1)).to_arrow()
    assert out.column("d").to_pylist() == [date(2024, 1, 1), date(2024, 6, 1)]


def test_date_literal_derives_date32(dates):
    out = dates.derive(start=lambda r: r.d.fill_null(date(2000, 1, 1))).to_arrow()
    assert out.schema.field("start").type == pa.date32()


def test_naive_datetime_on_microsecond_column(dates):
    out = dates.filter(lambda r: r.ts > datetime(2024, 1, 1, 6)).to_arrow()
    assert out.num_rows == 2


def test_naive_datetime_on_nanosecond_column(dates):
    out = dates.filter(lambda r: r.ts_ns > datetime(2024, 1, 1, 6)).to_arrow()
    assert out.num_rows == 2


def test_aware_datetime_compares_by_instant(dates):
    # 2024-01-01T08:00+02:00 is 06:00 UTC.
    cutoff = datetime(2024, 1, 1, 8, tzinfo=timezone(timedelta(hours=2)))
    out = dates.filter(lambda r: r.ts_utc > cutoff).to_arrow()
    assert out.num_rows == 2


def test_datetime_literal_derives_timestamp(dates):
    out = dates.derive(t=lambda r: r.ts.fill_null(datetime(2000, 1, 1))).to_arrow()
    assert pa.types.is_timestamp(out.schema.field("t").type)


# ---------------------------------------------------------------------------
# End to end: numpy scalars and primitives
# ---------------------------------------------------------------------------


def test_numpy_scalars_are_typed_numbers(ints):
    assert ints.filter(lambda r: r.a > np.int64(1)).to_arrow().num_rows == 2
    assert ints.filter(lambda r: r.a > np.float64(1.5)).to_arrow().num_rows == 2


def test_none_literal_in_fill_null():
    t = LTSeq.from_arrow(pa.table({"a": pa.array([1, None], pa.int64())}))
    out = t.derive(b=lambda r: r.a.fill_null(None)).to_arrow()
    assert out.column("b").to_pylist() == [1, None]


def test_column_expr_coercion_uses_same_validation():
    with pytest.raises(TypeError, match="Unsupported literal type dict"):
        ColumnExpr("a") == {"x": 1}


# ---------------------------------------------------------------------------
# Specialized evaluators consume typed literals too
# ---------------------------------------------------------------------------


@pytest.fixture
def steps():
    return LTSeq.from_arrow(pa.table({"x": pa.array([1, 1, 3, 3, 4, 10, 10], pa.int64())})).sort(
        "x"
    )


def test_linear_scan_float_threshold_is_not_truncated(steps):
    """The linear-scan fast path used to truncate `-0.5` to `0`.

    A Decimal threshold is not scan-eligible and takes the fallback path, so
    it is the reference answer for the float one.
    """
    fast = steps.group_ordered(lambda r: (r.x - r.x.shift(1)) > -0.5).first().count()
    reference = (
        steps.group_ordered(lambda r: (r.x - r.x.shift(1)) > Decimal("-0.5")).first().count()
    )
    assert fast == reference == 7


def test_linear_scan_integer_threshold(steps):
    assert steps.group_ordered(lambda r: (r.x - r.x.shift(1)) > 1).first().count() == 3
    assert (
        steps.group_ordered(lambda r: (r.x - r.x.shift(1)) > Decimal("1")).first().count() == 3
    )


def test_search_pattern_boolean_literal():
    """`Boolean` literals were rejected by the pattern evaluator's dtype names."""
    t = LTSeq.from_arrow(
        pa.table({"a": [True, False, True, True], "v": pa.array([1, 2, 3, 4], pa.int64())})
    ).sort("v")
    out = t.search_pattern(lambda r: r.a == True, lambda r: r.a == False).to_arrow()  # noqa: E712
    assert out.column("v").to_pylist() == [1]
    assert t.search_pattern_count(lambda r: r.a == True, lambda r: r.a == True) == 1  # noqa: E712


# ---------------------------------------------------------------------------
# Rust boundary: payloads are extracted by dtype, never re-parsed
# ---------------------------------------------------------------------------


def _gt_literal(literal: dict) -> dict:
    return {"type": "BinOp", "op": "Gt", "left": {"type": "Column", "name": "a"}, "right": literal}


@pytest.mark.parametrize(
    "literal, message",
    [
        ({"type": "Literal", "value": "1", "dtype": "Int64"}, "Int64 literal 'value' must be an int"),
        ({"type": "Literal", "value": 1.5, "dtype": "Int64"}, "Int64 literal 'value' must be an int"),
        ({"type": "Literal", "value": 1, "dtype": "String"}, "String literal 'value' must be a str"),
        ({"type": "Literal", "value": "True", "dtype": "Boolean"}, "Boolean literal 'value' must be a bool"),
        (
            {"type": "Literal", "value": 15, "dtype": "Decimal128", "precision": 39, "scale": 1},
            "invalid precision",
        ),
        ({"type": "Literal", "value": 1, "dtype": "Decimal128"}, "Missing field: precision"),
        ({"type": "Literal", "value": 1, "dtype": "Int32"}, "Unknown literal dtype: Int32"),
        # PyO3 would coerce these; the boundary checks the Python type first.
        ({"type": "Literal", "value": True, "dtype": "Int64"}, "Int64 literal 'value' must be an int"),
        ({"type": "Literal", "value": 1, "dtype": "Float64"}, "Float64 literal 'value' must be a float"),
        ({"type": "Literal", "value": True, "dtype": "Float64"}, "Float64 literal 'value' must be a float"),
        ({"type": "Literal", "value": 1, "dtype": "Boolean"}, "Boolean literal 'value' must be a bool"),
        (
            {"type": "Literal", "value": True, "dtype": "Decimal128", "precision": 1, "scale": 0},
            "Decimal128 literal 'value' must be an int",
        ),
        (
            {"type": "Literal", "value": 1, "dtype": "Decimal128", "precision": True, "scale": 0},
            "Decimal128 literal 'precision' must be an int",
        ),
        ({"type": "Literal", "value": True, "dtype": "Date32"}, "Date32 literal 'value' must be an int"),
        (
            {"type": "Literal", "value": 1.0, "dtype": "TimestampMicrosecond", "tz": None},
            "TimestampMicrosecond literal 'value' must be an int",
        ),
        (
            {"type": "Literal", "value": 1, "dtype": "TimestampMicrosecond", "tz": 0},
            "TimestampMicrosecond literal 'tz' must be a str or None",
        ),
        ({"type": "Literal", "dtype": "Null"}, "Missing field: value"),
        ({"type": "Literal", "value": 0, "dtype": "Null"}, "Null literal 'value' must be None"),
    ],
)
def test_rust_rejects_mistyped_literal_payloads(ints, literal, message):
    with pytest.raises(ValueError, match=message):
        ints._inner.filter(_gt_literal(literal))
