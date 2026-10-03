"""Every DSL entry point that serializes must also execute (#147).

`r.x // 2` serialized for a long time while every execution path rejected
it: the serialization tests passed, and nothing ran the expression. This
module ties the DSL surface to execution:

1. The surface is discovered from the classes themselves: the operator
   dunders defined on `Expr`, the public methods of the `.s` and `.dt`
   accessors, and the public scalar methods of `Expr`. New surface cannot
   be added without a case here.
2. Each discovered entry point needs a case in `CASES`, or a reason in
   `UNSUPPORTED_OPERATORS` or `NOT_ROW_SCALAR`.
3. Each case runs through `derive`, `filter` and `search_first`, and the
   three paths must select the same rows. Operator cases are also checked
   value by value against a reference.

Window functions (`shift`, `rolling`, ...) are reached through
`ColumnExpr.__getattr__`, which accepts any name, so they cannot be
discovered this way; their own test modules cover them.
"""

import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import pyarrow as pa
import pytest

from ltseq import LTSeq
from ltseq.expr.accessors import StringAccessor, TemporalAccessor
from ltseq.expr.base import Expr
from ltseq.expr.types import ColumnExpr

DATA = {
    "i": [0, 1, 2, 3, 4, 5],
    "x": [7, -7, 7, -7, 6, 0],
    "y": [2, 2, -2, -2, 3, 5],
    "f": [7.5, -7.5, 1.0, -0.5, 2.25, 0.0],
    "b": [True, False, True, False, True, False],
    "n": [1, None, 3, None, 5, 6],
    "s": ["Abc", " de ", "XYZ", "123", "a-b", "q"],
    "ts": [datetime(2024, 1, 2, 3, 4, 5, 6000) + timedelta(days=d, hours=d) for d in range(6)],
}
ROWS = [dict(zip(DATA, values)) for values in zip(*DATA.values())]


def _trunc_div(a: int, b: int) -> int:
    """`/` on integers: SQL truncating division, not Python's (#218)."""
    q = abs(a) // abs(b)
    return q if (a < 0) == (b < 0) else -q


def _trunc_mod(a: int, b: int) -> int:
    """`%`: SQL remainder with the dividend's sign, not Python's (#218)."""
    return a - b * _trunc_div(a, b)


@dataclass(frozen=True)
class Case:
    build: Callable[[Any], Any]
    # Expected value per row; set for operators, whose semantics this module
    # pins. Method and accessor semantics are tested in their own modules.
    reference: Callable[[dict], Any] | None = None
    # False for values that change between calls (dt.age reads the clock),
    # so filter/search_first select on non-null instead of equality.
    stable: bool = True


CASES: dict[str, Case] = {
    # Arithmetic
    "__add__": Case(lambda r: r.x + r.y, lambda row: row["x"] + row["y"]),
    "__radd__": Case(lambda r: 10 + r.x, lambda row: 10 + row["x"]),
    "__sub__": Case(lambda r: r.x - r.y, lambda row: row["x"] - row["y"]),
    "__rsub__": Case(lambda r: 10 - r.x, lambda row: 10 - row["x"]),
    "__mul__": Case(lambda r: r.x * r.y, lambda row: row["x"] * row["y"]),
    "__rmul__": Case(lambda r: 3 * r.x, lambda row: 3 * row["x"]),
    "__truediv__": Case(lambda r: r.x / r.y, lambda row: _trunc_div(row["x"], row["y"])),
    "__rtruediv__": Case(lambda r: 100 / r.y, lambda row: _trunc_div(100, row["y"])),
    "__floordiv__": Case(lambda r: r.x // r.y, lambda row: row["x"] // row["y"]),
    "__rfloordiv__": Case(lambda r: 100 // r.y, lambda row: 100 // row["y"]),
    "__mod__": Case(lambda r: r.x % r.y, lambda row: _trunc_mod(row["x"], row["y"])),
    "__rmod__": Case(lambda r: 100 % r.y, lambda row: _trunc_mod(100, row["y"])),
    # Comparison
    "__eq__": Case(lambda r: r.x == 7, lambda row: row["x"] == 7),
    "__ne__": Case(lambda r: r.x != 7, lambda row: row["x"] != 7),
    "__lt__": Case(lambda r: r.x < r.y, lambda row: row["x"] < row["y"]),
    "__le__": Case(lambda r: r.x <= 6, lambda row: row["x"] <= 6),
    "__gt__": Case(lambda r: r.x > r.y, lambda row: row["x"] > row["y"]),
    "__ge__": Case(lambda r: r.x >= 6, lambda row: row["x"] >= 6),
    # Logical and unary
    "__and__": Case(lambda r: r.b & (r.x > 0), lambda row: row["b"] and row["x"] > 0),
    "__or__": Case(lambda r: r.b | (r.x > 0), lambda row: row["b"] or row["x"] > 0),
    "__invert__": Case(lambda r: ~r.b, lambda row: not row["b"]),
    "__abs__": Case(lambda r: abs(r.x), lambda row: abs(row["x"])),
    # Scalar Expr methods
    "fill_null": Case(lambda r: r.n.fill_null(0)),
    "is_null": Case(lambda r: r.n.is_null()),
    "is_not_null": Case(lambda r: r.n.is_not_null()),
    "is_in": Case(lambda r: r.x.is_in([7, 6])),
    "between": Case(lambda r: r.x.between(0, 6)),
    "cast": Case(lambda r: r.x.cast("float64")),
    "abs": Case(lambda r: r.x.abs()),
    "round": Case(lambda r: r.f.round()),
    "floor": Case(lambda r: r.f.floor()),
    "ceil": Case(lambda r: r.f.ceil()),
    # String accessor
    "s.asc": Case(lambda r: r.s.s.asc()),
    "s.concat": Case(lambda r: r.s.s.concat("!")),
    "s.contains": Case(lambda r: r.s.s.contains("b")),
    "s.ends_with": Case(lambda r: r.s.s.ends_with("c")),
    "s.find": Case(lambda r: r.s.s.find("b")),
    "s.isalpha": Case(lambda r: r.s.s.isalpha()),
    "s.isdigit": Case(lambda r: r.s.s.isdigit()),
    "s.islower": Case(lambda r: r.s.s.islower()),
    "s.isupper": Case(lambda r: r.s.s.isupper()),
    "s.left": Case(lambda r: r.s.s.left(2)),
    "s.len": Case(lambda r: r.s.s.len()),
    "s.like": Case(lambda r: r.s.s.like("A%")),
    "s.lower": Case(lambda r: r.s.s.lower()),
    "s.lstrip": Case(lambda r: r.s.s.lstrip()),
    "s.ord": Case(lambda r: r.s.s.ord()),
    "s.pad_left": Case(lambda r: r.s.s.pad_left(5, "*")),
    "s.pad_right": Case(lambda r: r.s.s.pad_right(5, "*")),
    "s.pos": Case(lambda r: r.s.s.pos("b")),
    "s.regex_match": Case(lambda r: r.s.s.regex_match("^[A-Z]")),
    "s.replace": Case(lambda r: r.s.s.replace("b", "B")),
    "s.right": Case(lambda r: r.s.s.right(2)),
    "s.rstrip": Case(lambda r: r.s.s.rstrip()),
    "s.slice": Case(lambda r: r.s.s.slice(0, 2)),
    "s.split": Case(lambda r: r.s.s.split("-", 1)),
    "s.split_part": Case(lambda r: r.s.s.split_part("-", 1)),
    "s.starts_with": Case(lambda r: r.s.s.starts_with("A")),
    "s.strip": Case(lambda r: r.s.s.strip()),
    "s.upper": Case(lambda r: r.s.s.upper()),
    # Temporal accessor
    "dt.add": Case(lambda r: r.ts.dt.add(days=1)),
    "dt.age": Case(lambda r: r.ts.dt.age(), stable=False),
    "dt.day": Case(lambda r: r.ts.dt.day()),
    "dt.diff": Case(lambda r: r.ts.dt.diff(r.ts, "day")),
    "dt.hour": Case(lambda r: r.ts.dt.hour()),
    "dt.millisecond": Case(lambda r: r.ts.dt.millisecond()),
    "dt.minute": Case(lambda r: r.ts.dt.minute()),
    "dt.month": Case(lambda r: r.ts.dt.month()),
    "dt.second": Case(lambda r: r.ts.dt.second()),
    "dt.weekday": Case(lambda r: r.ts.dt.weekday()),
    "dt.year": Case(lambda r: r.ts.dt.year()),
}

# Operators Expr defines only to refuse with a pointer to the alternative,
# mapped to (an expression using it, the expected error message).
UNSUPPORTED_OPERATORS: dict[str, tuple[Callable[[Any], Any], str]] = {
    "__neg__": (lambda r: -r.x, "Unary minus is not supported"),
    "__pow__": (lambda r: r.x**2, r"use power\(base, exponent\)"),
    "__rpow__": (lambda r: 2**r.x, r"use power\(base, exponent\)"),
}

# Public Expr methods that are not row-scalar expressions.
NOT_ROW_SCALAR = {
    "serialize": "the serializer itself",
    "pct_change": "window function over table order; test_sequence_ops_advanced.py",
    "lookup": "needs a second table; test_lookup.py",
}

# Python's operator protocol: binary operators with their reflected forms,
# rich comparisons, and the unary operators.
_BINARY = ["add", "sub", "mul", "truediv", "floordiv", "mod", "pow", "matmul"]
_BINARY += ["lshift", "rshift", "and", "or", "xor", "divmod"]
PYTHON_OPERATOR_DUNDERS = (
    {f"__{name}__" for name in _BINARY}
    | {f"__r{name}__" for name in _BINARY}
    | {"__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"}
    | {"__neg__", "__pos__", "__invert__", "__abs__"}
)


def _defines(cls: type, name: str) -> bool:
    return any(name in vars(klass) for klass in cls.__mro__ if klass is not object)


def _public_methods(cls: type) -> set[str]:
    return {name for name, _ in inspect.getmembers(cls, inspect.isfunction) if not name.startswith("_")}


def discovered_surface() -> set[str]:
    operators = {name for name in PYTHON_OPERATOR_DUNDERS if _defines(ColumnExpr, name)}
    strings = {f"s.{name}" for name in _public_methods(StringAccessor)}
    temporal = {f"dt.{name}" for name in _public_methods(TemporalAccessor)}
    return operators | strings | temporal | _public_methods(Expr)


def test_every_dsl_entry_point_has_a_case():
    covered = set(CASES) | set(UNSUPPORTED_OPERATORS) | set(NOT_ROW_SCALAR)
    surface = discovered_surface()
    assert surface - covered == set(), "DSL entry points with no execution case"
    assert covered - surface == set(), "cases for entry points the DSL no longer has"


@pytest.fixture(scope="module")
def table():
    return LTSeq.from_arrow(pa.table(DATA)).sort("i")


def _ids(t: LTSeq) -> list[int]:
    return t.to_arrow().column("i").to_pylist()


@pytest.mark.parametrize("name", sorted(CASES))
def test_case_executes_on_every_path(table, name):
    case = CASES[name]
    values = table.derive(v=case.build).to_arrow().column("v").to_pylist()
    if case.reference is not None:
        assert values == [case.reference(row) for row in ROWS]

    # A predicate built from the same expression must select the same rows
    # in filter and search_first as the derived values imply.
    probe = next(v for v in values if v is not None)
    if case.stable and isinstance(probe, bool):
        pred, selected = case.build, [v is True for v in values]
    elif case.stable and isinstance(probe, (int, float, str)):
        pred, selected = (lambda r: case.build(r) == probe), [v == probe for v in values]
    else:  # no DSL literal for the type, or a value that drifts between calls
        pred, selected = (lambda r: case.build(r).is_not_null()), [v is not None for v in values]
    expected = [row["i"] for row, keep in zip(ROWS, selected) if keep]
    assert expected, "the probe must select at least one row"
    assert _ids(table.filter(pred)) == expected
    assert _ids(table.search_first(pred)) == expected[:1]


@pytest.mark.parametrize("name", sorted(UNSUPPORTED_OPERATORS))
def test_unsupported_operator_raises_clear_error(table, name):
    build, message = UNSUPPORTED_OPERATORS[name]
    with pytest.raises(NotImplementedError, match=message):
        table.derive(v=build)
