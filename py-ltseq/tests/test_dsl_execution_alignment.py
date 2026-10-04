"""Every DSL entry point that serializes must also execute (#147).

`r.x // 2` serialized for a long time while every execution path rejected
it: the serialization tests passed, and nothing ran the expression. This
module ties the DSL surface to execution:

1. The surface is discovered from the package itself: the operator
   dunders defined on `Expr`, the public methods of the `.s` and `.dt`
   accessors, the public scalar methods of `Expr`, and the functions
   exported from `ltseq.expr` (`if_else`, `power`, ...). New surface
   cannot be added to any of these without a case here.
2. Each discovered entry point needs a case in `CASES`, or a reason in
   `UNSUPPORTED_OPERATORS` or `NOT_ROW_SCALAR`.
3. Each case runs through `derive`, `filter` and `search_first`. For every
   value the derived column takes, a predicate comparing the expression to
   that value must select exactly the rows holding it, so the filter paths
   are pinned to the derived values. Operator cases also check those values
   against a reference.

The three paths share the DataFusion transpiler. The hand-written
evaluators are guarded elsewhere: the `group_ordered(...).first().count()`
kernel by `test_group_count_fast_path.py`, which compares it with the
DataFusion path, and `search_pattern`, whose evaluator supports a narrow
subset, by #188.

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

import ltseq.expr as dsl
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
    "__rfloordiv__": Case(lambda r: -7 // r.y, lambda row: -7 // row["y"]),
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
    # Functions exported from ltseq.expr. The math arguments stay inside
    # each function's domain: a NaN result has no row equal to it.
    "fn.if_else": Case(lambda r: dsl.if_else(r.x > 0, r.x, r.y)),
    "fn.when": Case(lambda r: dsl.when(r.x > 6, 2).when(r.x > 0, 1).otherwise(0)),
    "fn.coalesce": Case(lambda r: dsl.coalesce(r.n, r.x)),
    "fn.nvl": Case(lambda r: dsl.nvl(r.n, 0)),
    "fn.ifa": Case(lambda r: dsl.ifa(r.x > 0, r.x)),
    "fn.sqrt": Case(lambda r: dsl.sqrt(r.i)),
    "fn.power": Case(lambda r: dsl.power(r.x, 2)),
    "fn.sign": Case(lambda r: dsl.sign(r.x)),
    "fn.log": Case(lambda r: dsl.log(r.i + 1, 10)),
    "fn.ln": Case(lambda r: dsl.ln(r.i + 1)),
    "fn.exp": Case(lambda r: dsl.exp(r.i)),
    "fn.sin": Case(lambda r: dsl.sin(r.f)),
    "fn.cos": Case(lambda r: dsl.cos(r.f)),
    "fn.tan": Case(lambda r: dsl.tan(r.f)),
    "fn.asin": Case(lambda r: dsl.asin(r.f / 8)),
    "fn.acos": Case(lambda r: dsl.acos(r.f / 8)),
    "fn.atan": Case(lambda r: dsl.atan(r.f)),
    "fn.atan2": Case(lambda r: dsl.atan2(r.f, r.y)),
    "fn.rand": Case(lambda r: dsl.rand(), stable=False),
    "fn.gcd": Case(lambda r: dsl.gcd(r.x, r.y)),
    "fn.lcm": Case(lambda r: dsl.lcm(r.x, r.y)),
    "fn.factorial": Case(lambda r: dsl.factorial(r.i)),
    "fn.str_char": Case(lambda r: dsl.str_char(r.i + 65)),
    "fn.char": Case(lambda r: dsl.char(r.i + 65)),
    "fn.concat_ws": Case(lambda r: dsl.concat_ws("-", r.s, r.s)),
    "fn.now": Case(lambda r: dsl.now(), stable=False),
    "fn.today": Case(lambda r: dsl.today(), stable=False),
}

# Operators Expr defines only to refuse with a pointer to the alternative,
# mapped to (an expression using it, the expected error message).
UNSUPPORTED_OPERATORS: dict[str, tuple[Callable[[Any], Any], str]] = {
    "__neg__": (lambda r: -r.x, "Unary minus is not supported"),
    "__pow__": (lambda r: r.x**2, r"use power\(base, exponent\)"),
    "__rpow__": (lambda r: 2**r.x, r"use power\(base, exponent\)"),
}

# Public Expr methods and exported functions that are not row-scalar
# expressions, so derive/filter/search_first are not where they run.
NOT_ROW_SCALAR = {
    "serialize": "the serializer itself",
    "pct_change": "window function over table order; test_sequence_ops_advanced.py",
    "lookup": "needs a second table; test_lookup.py",
    "fn.count_if": "aggregate, runs in agg(); test_conditional_aggs.py",
    "fn.sum_if": "aggregate, runs in agg(); test_conditional_aggs.py",
    "fn.avg_if": "aggregate, runs in agg(); test_conditional_aggs.py",
    "fn.min_if": "aggregate, runs in agg(); test_conditional_aggs.py",
    "fn.max_if": "aggregate, runs in agg(); test_conditional_aggs.py",
    "fn.skew": "aggregate, runs in agg(); test_statistical_aggs.py",
    "fn.corr": "aggregate, runs in agg(); test_standalone_calls.py",
    "fn.covar": "aggregate, runs in agg()",
    "fn.concat_agg": "aggregate, runs in agg(); test_standalone_calls.py",
    "fn.row_number": "ranking window, needs .over(); test_ranking.py",
    "fn.rank": "ranking window, needs .over(); test_ranking.py",
    "fn.dense_rank": "ranking window, needs .over(); test_ranking.py",
    "fn.ntile": "ranking window, needs .over(); test_ranking.py",
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


def _exported_functions(module) -> set[str]:
    return {
        name for name in module.__all__ if not name.startswith("_") and inspect.isfunction(getattr(module, name))
    }


def discovered_surface() -> set[str]:
    operators = {name for name in PYTHON_OPERATOR_DUNDERS if _defines(ColumnExpr, name)}
    strings = {f"s.{name}" for name in _public_methods(StringAccessor)}
    temporal = {f"dt.{name}" for name in _public_methods(TemporalAccessor)}
    functions = {f"fn.{name}" for name in _exported_functions(dsl)}
    return operators | strings | temporal | functions | _public_methods(Expr)


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


def _predicates(case: Case, values: list) -> list[tuple[Callable[[Any], Any], list[bool]]]:
    """Predicates over the case's expression, each with the rows it must select."""
    present = [v for v in values if v is not None]
    if case.stable and all(isinstance(v, bool) for v in present):
        return [(case.build, [v is True for v in values])]
    if case.stable and all(isinstance(v, (int, float, str)) for v in present):
        return [
            ((lambda r, p=probe: case.build(r) == p), [v == probe for v in values])
            for probe in dict.fromkeys(present)
        ]
    # No DSL literal for the type (timestamps), or a value that drifts
    # between calls (dt.age): the expression must still run and keep non-nulls.
    return [((lambda r: case.build(r).is_not_null()), [v is not None for v in values])]


@pytest.mark.parametrize("name", sorted(CASES))
def test_case_executes_on_every_path(table, name):
    case = CASES[name]
    values = table.derive(v=case.build).to_arrow().column("v").to_pylist()
    if case.reference is not None:
        assert values == [case.reference(row) for row in ROWS]

    for pred, selected in _predicates(case, values):
        expected = [row["i"] for row, keep in zip(ROWS, selected) if keep]
        assert expected, "each probe must select at least one row"
        assert _ids(table.filter(pred)) == expected
        assert _ids(table.search_first(pred)) == expected[:1]


@pytest.mark.parametrize("name", sorted(UNSUPPORTED_OPERATORS))
def test_unsupported_operator_raises_clear_error(table, name):
    build, message = UNSUPPORTED_OPERATORS[name]
    with pytest.raises(NotImplementedError, match=message):
        table.derive(v=build)
