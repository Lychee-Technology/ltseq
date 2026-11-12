"""Comparisons on a dictionary- or run-end-encoded float follow the decoded
column (#292).

DataFusion compares floats through `apply_cmp`, which turns `-0.0` into
`0.0` before Arrow's IEEE 754 totalOrder kernels, so that `-0.0 == 0.0`
holds and neither zero orders below the other (contract §18). It does so
only for a plain Float16, Float32 or Float64 array: an encoded float reached
the kernels as stored, so `==`, `!=`, `<`, `<=`, `>` and `>=` decided a zero
by its sign there. Next to a literal an encoded operand now keeps its
encoding (an order with a zero compares with the zero that keeps both zeros
together); next to a value that varies by row it is decoded.

Each comparison is checked against the same comparison on the decoded
column, run by this build, and against Python's floats, which follow IEEE
754, under SQL's three-valued logic. The operands are a column and a
literal in both orders, two columns in both orders (encoded with plain,
encoded with the other encoding, encoded with itself) and computed values,
in `derive` and in `filter`; the inputs are split into chunks that each
carry their own dictionary or runs, and run at every engine layout. NaN is
#290's and stays out of the data, except where only parity with the decoded
column is asserted.
"""

from __future__ import annotations

import math
import operator

import pyarrow as pa
import pytest

from ltseq import LTSeq, if_else

NAN = math.nan

X = [0.0, -0.0, 1.0, None, -1.0, -0.0, 0.0, 2.5, -0.0, 0.0]
Y = [-0.0, 0.0, 1.0, 0.0, None, -0.0, -0.0, -2.5, 0.0, 1.0]

OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
LITERALS = {"-0.0": -0.0, "0.0": 0.0, "0": 0, "1.0": 1.0, "-1.0": -1.0}

VALUE_TYPES = {"float64": pa.float64(), "float32": pa.float32(), "float16": pa.float16()}
ENCODINGS = ["dictionary", "run_end_encoded"]


def encode(values: list, value_type: pa.DataType, encoding: str, chunks: int) -> pa.ChunkedArray:
    """`values` as `chunks` arrays, each with a dictionary or runs of its own."""
    size = -(-len(values) // chunks)
    parts = []
    for start in range(0, len(values), size):
        part = pa.array(values[start : start + size], value_type)
        if encoding == "dictionary":
            # pyarrow hashes no float16 for `dictionary_encode`.
            indices = pa.array(range(len(part)), pa.int32())
            part = pa.DictionaryArray.from_arrays(indices, part)
        elif encoding == "run_end_encoded":
            part = pa.RunEndEncodedArray.from_arrays(pa.array(range(1, len(part) + 1), pa.int32()), part)
        parts.append(part)
    return pa.chunked_array(parts)


def tables(value_type: pa.DataType, encoding: str, chunks: int = 1, x: list = X, y: list = Y) -> tuple[LTSeq, LTSeq]:
    """The table with `x` encoded, and the same table decoded. `y` is plain;
    `ye` is `y` in the other encoding."""
    other = next(e for e in ENCODINGS if e != encoding)

    def build(x_column, ye_column):
        return LTSeq.from_arrow(
            pa.table(
                {
                    "k": pa.array(range(len(x)), pa.int64()),
                    "x": x_column,
                    "y": pa.array(y, value_type),
                    "ye": ye_column,
                }
            )
        ).sort("k")

    encoded = build(encode(x, value_type, encoding, chunks), encode(y, value_type, other, chunks))
    decoded = build(pa.array(x, value_type), pa.array(y, value_type))
    return encoded, decoded


def ieee(op, a, b):
    """`a op b` on Python floats, NULL when either is NULL."""
    if a is None or b is None:
        return None
    return op(a, b)


# Operand pairs: how to write them, and their values on row `k`.
def pairs() -> dict:
    out = {}
    for name, value in LITERALS.items():
        out[f"x ? {name}"] = (lambda r, v=value: (r.x, v), lambda k, v=value: (X[k], v))
        out[f"{name} ? x"] = (lambda r, v=value: (v, r.x), lambda k, v=value: (v, X[k]))
    out["x ? y"] = (lambda r: (r.x, r.y), lambda k: (X[k], Y[k]))
    out["y ? x"] = (lambda r: (r.y, r.x), lambda k: (Y[k], X[k]))
    out["x ? ye"] = (lambda r: (r.x, r.ye), lambda k: (X[k], Y[k]))
    out["ye ? x"] = (lambda r: (r.ye, r.x), lambda k: (Y[k], X[k]))
    out["x ? x"] = (lambda r: (r.x, r.x), lambda k: (X[k], X[k]))
    out["if_else(x) ? y"] = (
        lambda r: (if_else(r.k >= 0, r.x, r.x), r.y),
        lambda k: (X[k], Y[k]),
    )
    out["x.fill_null(-0.0) ? 0.0"] = (
        lambda r: (r.x.fill_null(-0.0), 0.0),
        lambda k: (-0.0 if X[k] is None else X[k], 0.0),
    )
    out["x * 1.0 ? ye"] = (lambda r: (r.x * 1.0, r.ye), lambda k: (X[k], Y[k]))
    return out


PAIRS = pairs()


def compared(op_name: str, pair: str):
    write = PAIRS[pair][0]

    def predicate(r):
        left, right = write(r)
        return OPS[op_name](left, right)

    return predicate


def reference(op_name: str, pair: str) -> list:
    value = PAIRS[pair][1]
    return [ieee(OPS[op_name], *value(k)) for k in range(len(X))]


def flags(t: LTSeq) -> dict[str, list]:
    columns = {f"{pair} {op}": compared(op, pair) for op in OPS for pair in PAIRS}
    names = list(columns)
    got = t.derive(**{f"c{i}": columns[name] for i, name in enumerate(names)}).to_arrow()
    return {name: got.column(f"c{i}").to_pylist() for i, name in enumerate(names)}


@pytest.mark.parametrize("chunks", [1, 3])
@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize("value_type", VALUE_TYPES)
def test_derived_comparisons_follow_the_decoded_column(value_type, encoding, chunks):
    encoded, decoded = tables(VALUE_TYPES[value_type], encoding, chunks)
    got, plain = flags(encoded), flags(decoded)
    wanted = {f"{pair} {op}": reference(op, pair) for op in OPS for pair in PAIRS}
    assert {name: got[name] for name in got if got[name] != plain[name]} == {}
    assert {name: got[name] for name in got if got[name] != wanted[name]} == {}


@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("encoding", ENCODINGS)
def test_filters_follow_the_decoded_column(engine_layout, encoding, op):
    """Every pair as a filter, at each engine layout, so that batches split
    the rows and the filter may be pushed into the scan."""
    wrong = {}
    for value_type in VALUE_TYPES.values():
        encoded, decoded = tables(value_type, encoding, chunks=3)
        for pair in PAIRS:
            predicate = compared(op, pair)
            got = encoded.filter(predicate).to_arrow().column("k").to_pylist()
            plain = decoded.filter(predicate).to_arrow().column("k").to_pylist()
            want = [k for k, v in enumerate(reference(op, pair)) if v]
            if not got == plain == want:
                wrong[f"{value_type} {pair} {op}"] = (got, plain, want)
    assert wrong == {}


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_negated_and_combined_comparisons(encoding):
    """`~`, `&` and `|` over comparisons with zeros, which DataFusion's
    simplifier rewrites (negating `<` into `>=`, merging equalities into
    lists) after lowering."""
    encoded, decoded = tables(pa.float64(), encoding, chunks=2)
    predicates = {
        "~(x < 0.0)": lambda r: ~(r.x < 0.0),
        "~(x >= -0.0)": lambda r: ~(r.x >= -0.0),
        "~(x != y)": lambda r: ~(r.x != r.y),
        "x == y | x == 1.0 | x == 2.5 | x == 7.0": lambda r: (r.x == r.y)
        | (r.x == 1.0)
        | (r.x == 2.5)
        | (r.x == 7.0),
        "x != y & x != 1.0 & x != 2.5 & x != 7.0": lambda r: (r.x != r.y)
        & (r.x != 1.0)
        & (r.x != 2.5)
        & (r.x != 7.0),
        "x.between(-0.0, 0.0)": lambda r: r.x.between(-0.0, 0.0),
        "x.between(0.0, 1.0)": lambda r: r.x.between(0.0, 1.0),
    }
    wrong = {}
    for name, predicate in predicates.items():
        got = encoded.filter(predicate).to_arrow().column("k").to_pylist()
        plain = decoded.filter(predicate).to_arrow().column("k").to_pylist()
        if got != plain:
            wrong[name] = (got, plain)
    assert wrong == {}
    # The decoded column's own answers, for one of them: both zeros match.
    kept = encoded.filter(predicates["x.between(-0.0, 0.0)"]).to_arrow().column("k").to_pylist()
    assert kept == [k for k, v in enumerate(X) if v == 0.0]


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_window_operands_follow_the_decoded_column(encoding):
    """A comparison in a window expression: this row's encoded value
    against the previous row's, of the plain column and, for a dictionary,
    of the encoded column itself. `shift` of a run-end encoded column fails
    in DataFusion whatever its value type ("run_ends array should be
    strictly positive"), so that pair is left out for it."""
    encoded, decoded = tables(pa.float64(), encoding, chunks=2)
    columns = {"x == y.shift(1)": lambda r: r.x == r.y.shift(1), "y.shift(1) < x": lambda r: r.y.shift(1) < r.x}
    previous_y = [None, *Y[:-1]]
    wanted = {
        "x == y.shift(1)": [ieee(operator.eq, x, p) for x, p in zip(X, previous_y)],
        "y.shift(1) < x": [ieee(operator.lt, p, x) for x, p in zip(X, previous_y)],
    }
    if encoding == "dictionary":
        columns["x.shift(1) < x"] = lambda r: r.x.shift(1) < r.x
        wanted["x.shift(1) < x"] = [ieee(operator.lt, p, x) for x, p in zip(X, [None, *X[:-1]])]

    def derived(t):
        got = t.derive(**{f"c{i}": f for i, f in enumerate(columns.values())}).to_arrow()
        return {name: got.column(f"c{i}").to_pylist() for i, name in enumerate(columns)}

    got = derived(encoded)
    assert got == derived(decoded)
    assert got == wanted


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_group_ordered_follows_the_decoded_column(encoding):
    """`group_ordered` keys and boundary predicates compare as on the
    decoded column. An encoded column is not one the counting kernel takes
    (`can_linear_scan`), so the boundary count runs on DataFusion."""
    encoded, decoded = tables(pa.float64(), encoding, chunks=2)
    keys = {
        "x < 0.0": lambda r: r.x < 0.0,
        "x == y": lambda r: r.x == r.y,
        "x >= -0.0": lambda r: r.x >= -0.0,
    }
    if encoding == "dictionary":
        keys["x != x.shift(1)"] = lambda r: r.x != r.x.shift(1)
    got = {name: encoded.group_ordered(key).first().count() for name, key in keys.items()}
    assert got == {name: decoded.group_ordered(key).first().count() for name, key in keys.items()}


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_search_pattern_follows_the_decoded_column(encoding):
    """`search_pattern` evaluates its steps itself; a step comparing two
    columns, and one comparing with a zero, match as on the decoded column."""
    encoded, decoded = tables(pa.float64(), encoding, chunks=2)
    steps = [
        (lambda r: r.x == r.y, lambda r: r.x >= 0.0),
        (lambda r: r.x <= r.ye, lambda r: r.x > -0.0),
        (lambda r: r.x != r.y,),
        (lambda r: r.x < 0.0,),
    ]
    for pattern in steps:
        got = encoded.search_pattern(*pattern).to_arrow().column("k").to_pylist()
        plain = decoded.search_pattern(*pattern).to_arrow().column("k").to_pylist()
        assert got == plain
        assert encoded.search_pattern_count(*pattern) == len(plain)
    # Rows 0, 1, 5 and 6 compare two zeros: x == y holds there.
    got = encoded.search_pattern(lambda r: r.x == r.y).to_arrow().column("k").to_pylist()
    assert got == [k for k in range(len(X)) if ieee(operator.eq, X[k], Y[k])]


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_nan_compares_as_on_the_decoded_column(encoding):
    """A NaN in an encoded column compares as it does decoded. Which NaN
    equals which, and where NaN orders, is #290's; here only parity."""
    x = [NAN, -0.0, NAN, 0.0, None, 1.0]
    y = [NAN, NAN, 0.0, -0.0, NAN, None]
    encoded, decoded = tables(pa.float64(), encoding, chunks=2, x=x, y=y)

    def derived(t):
        exprs = {}
        for i, op in enumerate(OPS):
            exprs[f"y{i}"] = lambda r, op=op: OPS[op](r.x, r.y)
            exprs[f"z{i}"] = lambda r, op=op: OPS[op](r.x, -0.0)
            exprs[f"n{i}"] = lambda r, op=op: OPS[op](r.x, NAN)
        got = t.derive(**exprs).to_arrow()
        return {name: got.column(name).to_pylist() for name in exprs}

    assert derived(encoded) == derived(decoded)
