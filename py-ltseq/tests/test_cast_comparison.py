"""A cast compared with a literal is evaluated, in `filter` as in `derive` (#260).

DataFusion's simplifier rewrites `CAST(a AS t) <op> literal`, and
`CAST(a AS t) IN (literals)`, to compare `a` itself when every literal
converts exactly to `a`'s type (`unwrap_cast_in_comparison`). It does so in
the logical optimizer, and again in the physical simplifier a Parquet scan
applies to the filter pushed into it. The rewrite is sound only for a cast
that keeps every value of its input. For a narrowing cast it deleted the
failure: `r.a.cast("int32") > 5` kept the row `a = 2**40`, compared
unconverted, where `r.a.cast("int32")` alone raises. For a decimal cast to
an integer it undid the truncation: `5.5` cast to `int64` did not equal 5.
For a decimal of negative scale it panicked.

The reference is the cast value itself. The bare cast, derived and
collected, either raises or gives the values the comparison is made on. A
filter or a derived comparison raises exactly when the bare cast does;
otherwise the derived comparison is Python's operator on the cast values
(NULL for a NULL value) and the filter keeps the rows where it is true.
"""

from __future__ import annotations

import datetime
import operator
import random
import re
from decimal import Decimal

import pyarrow as pa
import pytest

from ltseq import LTSeq

from . import layouts

OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}

INT32 = (-(2**31), 2**31 - 1)
INT64 = (-(2**63), 2**63 - 1)
TARGET_RANGES = {"int32": INT32, "int64": INT64}

SOURCE_TYPES = {
    "int8": (pa.int8(), (-(2**7), 2**7 - 1)),
    "int16": (pa.int16(), (-(2**15), 2**15 - 1)),
    "int32": (pa.int32(), INT32),
    "int64": (pa.int64(), INT64),
    "uint8": (pa.uint8(), (0, 2**8 - 1)),
    "uint16": (pa.uint16(), (0, 2**16 - 1)),
    "uint32": (pa.uint32(), (0, 2**32 - 1)),
    "uint64": (pa.uint64(), (0, 2**64 - 1)),
}


def attempt(run):
    """The result of ``run()``, or the exception it raised."""
    try:
        return run()
    except Exception as error:  # noqa: BLE001 - the error is the result
        return error


def cast_message(error: Exception) -> str:
    """Arrow's message for a failed cast, without its numbers: with many
    partitions, another value can be the first to fail. A Parquet scan that
    evaluates the filter itself wraps the error it raises, any error, in
    one of its own and prints Arrow's in its debug form."""
    text = re.sub(r"-?\d+", "N", str(error))
    wrapped = re.search(r'\w+Error\("(.*?)"\)', text)
    return wrapped.group(1) if wrapped else re.sub(r"^.*?Arrow error: [^:]+: ", "", text)


def comparison(op, target, literal):
    if op == "is_in":
        return lambda r: r.a.cast(target).is_in(literal)
    return lambda r: OPS[op](r.a.cast(target), literal)


def compare(op, value, literal):
    """``op`` in three-valued logic, ``literal`` a list for ``is_in``."""
    if value is None:
        return None
    if op == "is_in":
        return value in literal
    return OPS[op](value, literal)


def by_id(table: pa.Table, column: str) -> dict:
    return dict(zip(table.column("id").to_pylist(), table.column(column).to_pylist()))


def check_comparison(t: LTSeq, target: str, op: str, literal, cast: dict | Exception):
    """Check one comparison of ``r.a.cast(target)`` against ``cast``, the
    bare cast's values by row id or the error it raised."""
    predicate = comparison(op, target, literal)
    label = f"r.a.cast({target!r}) {op} {literal!r}"
    derived = attempt(lambda: t.derive(k=predicate).to_arrow())
    kept = attempt(lambda: t.filter(predicate).to_arrow())
    if isinstance(cast, Exception):
        for result in (derived, kept):
            assert isinstance(result, type(cast)), f"{label}: {result!r}"
            assert cast_message(result) == cast_message(cast), f"{label}: {result!r}"
        return
    assert not isinstance(derived, Exception), f"{label}: {derived!r}"
    assert not isinstance(kept, Exception), f"{label}: {kept!r}"
    expected = {k: compare(op, v, literal) for k, v in cast.items()}
    assert by_id(derived, "k") == expected, label
    assert sorted(kept.column("id").to_pylist()) == sorted(
        k for k, v in expected.items() if v is True
    ), label


def bare_cast(t: LTSeq, target: str) -> dict | Exception:
    result = attempt(lambda: t.derive(c=lambda r: r.a.cast(target)).to_arrow())
    return result if isinstance(result, Exception) else by_id(result, "c")


def test_reproduction():
    t = LTSeq.from_arrow(pa.table({"a": pa.array([2**40, 3], pa.int64())}))
    with pytest.raises(ValueError, match="Cast error"):
        t.filter(lambda r: r.a.cast("int32") > 5).to_arrow()
    with pytest.raises(ValueError, match="Cast error"):
        t.derive(c=lambda r: r.a.cast("int32") > 5).to_arrow()
    with pytest.raises(ValueError, match="Cast error"):
        t.derive(c=lambda r: r.a.cast("int32")).to_arrow()


# Int64 values near the edges of each range, and small ones. CSV reads back
# only int64 values as integers.
def interesting_ints(source: tuple[int, int], target: tuple[int, int]) -> list[int]:
    points = {0, 1, -1, 3, 5, -5, 2**40, -(2**40), *source, *target, *INT64}
    points |= {target[0] - 1, target[1] + 1}
    low, high = max(source[0], INT64[0]), min(source[1], INT64[1])
    return sorted(p for p in points if low <= p <= high)


def random_case(rng: random.Random, source_name: str, target: str, held_only: bool):
    """A random column of type ``source_name`` holding int64 values, and a
    literal, for a cast to ``target``.

    A column of ``held_only`` holds only values the target holds, so that
    the comparison is computed rather than raising; any other holds one the
    target does not, if the column's type has one. NULL appears only at
    even rows, so each two-row CSV part file has a value and keeps the
    column's type. Most literals convert exactly to the column's type, the
    case the rewrite applied to.
    """
    source_type, source_range = SOURCE_TYPES[source_name]
    target_range = TARGET_RANGES[target]
    points = interesting_ints(source_range, target_range)
    held = [p for p in points if target_range[0] <= p <= target_range[1]]
    values = [
        None if index % 2 == 0 and rng.random() < 0.2 else rng.choice(held if held_only else points)
        for index in range(6)
    ]
    unheld = [p for p in points if p not in held]
    if unheld and not held_only:
        values[1] = rng.choice(unheld)
    literal = rng.choice(held if rng.random() < 0.8 else interesting_ints(INT64, target_range))
    # Distinct list items: the literal first, then values the target holds.
    items = list(dict.fromkeys([literal, *rng.sample(held, len(held))]))
    return pa.array(values, source_type), literal, items


CASES = [
    (source_name, target, *random_case(random.Random(f"260-{case}"), source_name, target, held_only))
    for case, (source_name, target, held_only) in enumerate(
        (source_name, target, held_only)
        for source_name in SOURCE_TYPES
        for target in TARGET_RANGES
        for held_only in (True, False)
    )
]


def test_filter_agrees_with_derive_on_integer_casts(data_layout):
    """Random columns of every integer type cast to int32 and int64, under
    every comparison operator and `is_in`, in every input layout (the
    Parquet layouts take the physical simplifier's path)."""
    for source_name, target, values, literal, items in CASES:
        rows = pa.table({"id": pa.array(range(len(values)), pa.int64()), "a": values})
        t = data_layout(rows)
        cast = bare_cast(t, target)
        low, high = TARGET_RANGES[target]
        if all(v is None or low <= v <= high for v in values.to_pylist()):
            assert cast == dict(enumerate(values.to_pylist())), (source_name, target, cast)
        else:
            assert isinstance(cast, Exception), (source_name, target, values)
        for op in OPS:
            check_comparison(t, target, op, literal, cast)
        # A list of up to three items DataFusion unfolds into `=`; four stay an IN list.
        check_comparison(t, target, "is_in", items[:2], cast)
        check_comparison(t, target, "is_in", items[:4], cast)


# Columns of the other types the rewrite takes (an integer, decimal, date,
# timestamp, string, dictionary or binary column next to a literal it
# holds), each with the target, literals, and whether the bare cast raises.
OTHER_PAIRS = {
    "uint64 past int64": (
        pa.array([2**63 + 1, 3, None], pa.uint64()),
        "int64", [5, 3], True,
    ),
    "decimal(10,2) truncates to int64": (
        pa.array([Decimal("5.5"), Decimal("3"), None, Decimal("-2.75"), Decimal("7")], pa.decimal128(10, 2)),
        "int64", [5, -2, 3], False,
    ),
    "decimal(10,2) truncates to int32": (
        pa.array([Decimal("5.5"), Decimal("3"), None, Decimal("-2.75")], pa.decimal128(10, 2)),
        "int32", [5, -2], False,
    ),
    "decimal(38,0) past int64": (
        pa.array([Decimal(10**20), Decimal(3)], pa.decimal128(38, 0)),
        "int64", [5, 3], True,
    ),
    "decimal(20,0) past int32": (
        pa.array([Decimal(2**40), Decimal(3)], pa.decimal128(20, 0)),
        "int32", [5, 3], True,
    ),
    "decimal(5,-2) to int32": (
        pa.array([Decimal(12300), Decimal(-200), None], pa.decimal128(5, -2)),
        "int32", [5, 12300, -200], False,
    ),
    "timestamp past int32": (
        pa.array([2**40, 3], pa.timestamp("s")),
        "int32", [5, 3], True,
    ),
    "timestamp within int32": (
        pa.array([5, 3, None], pa.timestamp("s")),
        "int32", [5, 3], False,
    ),
    "date64 past int32": (
        pa.array([86_400_000 * 30, 0], pa.date64()),
        "int32", [0, 5], True,
    ),
    "date32 to int32": (
        pa.array([3, 10, None], pa.date32()),
        "int32", [3, 5], False,
    ),
    "int64 past date32": (
        pa.array([2**40, 3], pa.int64()),
        "date32", [datetime.date(1970, 1, 4), datetime.date(1970, 1, 2)], True,
    ),
    "int64 within date32": (
        pa.array([3, 10, None], pa.int64()),
        "date32", [datetime.date(1970, 1, 4), datetime.date(1970, 1, 2)], False,
    ),
    "dictionary of int64 past int32": (
        pa.array([2**40, 3], pa.int64()).dictionary_encode(),
        "int32", [5, 3], True,
    ),
    "int64 to utf8": (
        pa.array([3, 10, None], pa.int64()),
        "utf8", ["3", "03", "10"], False,
    ),
    "large_string to utf8": (
        pa.array(["x", "y", None], pa.large_string()),
        "utf8", ["x", "z"], False,
    ),
    "string_view to utf8": (
        pa.array(["x", "y", None], pa.string_view()),
        "utf8", ["x", "z"], False,
    ),
    "binary not utf-8": (
        pa.array([b"\xff", b"x"], pa.binary()),
        "utf8", ["x"], True,
    ),
}


@pytest.mark.parametrize(
    "pair, layout",
    [
        (pair, layout)
        for pair in sorted(OTHER_PAIRS)
        for layout in ("one_batch", "parquet_row_groups")
        # Parquet has no decimal of negative scale.
        if layout == "one_batch" or "-" not in str(OTHER_PAIRS[pair][0].type)
    ],
)
def test_filter_agrees_with_derive_on_other_casts(pair, layout, tmp_path):
    values, target, literals, raises = OTHER_PAIRS[pair]
    rows = pa.table({"id": pa.array(range(len(values)), pa.int64()), "a": values})
    t = layouts.build(layout, rows, tmp_path)
    cast = bare_cast(t, target)
    if t.to_arrow().schema.field("a").type == values.type:
        # Parquet reads some types back as others (date64 as date32).
        assert isinstance(cast, Exception) == raises, cast
    for literal in literals:
        for op in OPS:
            check_comparison(t, target, op, literal, cast)
    check_comparison(t, target, "is_in", literals, cast)


def test_lossless_cast_is_unwrapped():
    """A cast that keeps every value stays transparent to DataFusion, so
    the comparison is on the column and Parquet statistics can prune."""
    t = LTSeq.from_arrow(pa.table({"a": pa.array([2**30, 3], pa.int32())}))
    logical, physical = t.filter(lambda r: r.a.cast("int64") > 5).explain_plan()
    assert "CAST" not in logical and "opaque_cast" not in logical, logical
    assert "a > Int32(5)" in logical, logical
    assert "opaque_cast" not in physical, physical


def test_lossless_cast_prunes_parquet(tmp_path):
    rows = pa.table({"a": pa.array([2**30, 3], pa.int32())})
    t = LTSeq.read_parquet(str(layouts.write_parquet(rows, tmp_path, files=1)))
    _, physical = t.filter(lambda r: r.a.cast("int64") > 5).explain_plan()
    assert "pruning_predicate" in physical and "a_max" in physical, physical


@pytest.mark.parametrize(
    "dtype, target",
    [(pa.int64(), "int32"), (pa.uint64(), "int64"), (pa.decimal128(10, 2), "int64")],
)
def test_cast_that_changes_a_value_is_kept(dtype, target, tmp_path):
    """Plan guard: a cast that can fail or round stays in the optimized
    plans, logical and physical, also when the scan takes the filter. A
    DataFusion upgrade that sees through it fails here first."""
    rows = pa.table({"a": pa.array([3, 4], dtype)})
    for t in (
        LTSeq.from_arrow(rows),
        LTSeq.read_parquet(str(layouts.write_parquet(rows, tmp_path, files=1))),
    ):
        logical, physical = t.filter(lambda r: r.a.cast(target) > 5).explain_plan()
        name = f"opaque_cast_{'Int32' if target == 'int32' else 'Int64'}"
        assert name in logical, logical
        assert name in physical, physical


def test_cast_keeps_its_output_name():
    t = LTSeq.from_arrow(pa.table({"a": pa.array([3, 4], pa.int64())}))
    narrowing = t.select(lambda r: r.a.cast("int32") > 5).to_arrow()
    widening = t.select(lambda r: r.a.cast("int64") > 5).to_arrow()
    assert narrowing.schema.names == ["?table?.a > Int32(5)"]
    assert widening.schema.names == ["?table?.a > Int64(5)"]
