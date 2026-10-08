"""Errors from deserializing a malformed expression dict in Rust (types.rs).

The Python serializer never produces these dicts; the tests pin which field a
deserialization error names, and whether it reports the field as missing or
as the wrong type.
"""

import pyarrow as pa
import pytest

from ltseq import LTSeq

COL = {"type": "Column", "name": "x"}
LIT = {"type": "Literal", "value": 1, "dtype": "Int64"}


def _drop(d: dict, key: str) -> dict:
    return {k: v for k, v in d.items() if k != key}


def _with(d: dict, key: str, value) -> dict:
    return {**d, key: value}


BINOP = {"type": "BinOp", "op": "Gt", "left": COL, "right": LIT}
UNARY = {"type": "UnaryOp", "op": "Not", "operand": COL}
CALL = {"type": "Call", "func": "abs", "args": [], "kwargs": {}, "on": COL}
WINDOW = {"type": "Window", "expr": COL, "partition_by": None, "order_by": None}
ALIAS = {"type": "Alias", "expr": COL, "alias": "y"}

CASES = [
    ({"name": "x"}, "Missing field: type"),
    ({"type": 1}, "Invalid type: type must be string"),
    ({"type": "Nope"}, "Unknown expression type: Nope"),
    ({"type": "Column"}, "Missing field: name"),
    ({"type": "Column", "name": 1}, "Invalid type: name must be string"),
    (_drop(LIT, "value"), "Malformed literal payload: Int64 literal is missing field 'value'"),
    (_drop(LIT, "dtype"), "Malformed literal payload: literal is missing field 'dtype'"),
    (_with(LIT, "dtype", 1), "Malformed literal payload: literal field 'dtype' must be a str, got int"),
    (_drop(BINOP, "op"), "Missing field: op"),
    (_with(BINOP, "op", 1), "Invalid type: op must be string"),
    (_drop(BINOP, "left"), "Missing field: left"),
    (_with(BINOP, "left", 1), "Invalid type: left must be a dict"),
    (_drop(BINOP, "right"), "Missing field: right"),
    (_with(BINOP, "right", [COL]), "Invalid type: right must be a dict"),
    (_with(BINOP, "left", {"type": "Column"}), "Missing field: name"),
    # `left` deserializes before `right` is looked up.
    (_drop(_with(BINOP, "left", {"type": "Column"}), "right"), "Missing field: name"),
    (_drop(UNARY, "operand"), "Missing field: operand"),
    (_with(UNARY, "operand", "x"), "Invalid type: operand must be a dict"),
    (_drop(CALL, "func"), "Missing field: func"),
    (_with(CALL, "func", 1), "Invalid type: func must be string"),
    (_drop(CALL, "args"), "Missing field: args"),
    (_with(CALL, "args", COL), "Invalid type: args must be a list"),
    (_with(CALL, "args", [1]), "Invalid type: args items must be dicts"),
    (_drop(CALL, "kwargs"), "Missing field: kwargs"),
    (_with(CALL, "kwargs", []), "Invalid type: kwargs must be a dict"),
    (_with(CALL, "kwargs", {1: COL}), "Invalid type: kwargs keys must be strings"),
    (_with(CALL, "kwargs", {"k": 1}), "Invalid type: kwargs values must be dicts"),
    (_with(CALL, "on", "x"), "Invalid type: on must be a dict or None"),
    (_drop(WINDOW, "expr"), "Missing field: expr"),
    (_with(WINDOW, "expr", 1), "Invalid type: expr must be a dict"),
    (_with(WINDOW, "partition_by", "x"), "Invalid type: partition_by must be a dict or None"),
    (_with(WINDOW, "order_by", "x"), "Invalid type: order_by must be a dict or None"),
    (_drop(ALIAS, "alias"), "Missing field: alias"),
    (_with(ALIAS, "alias", 1), "Invalid type: alias must be string"),
    (_drop(ALIAS, "expr"), "Missing field: expr"),
    (_with(ALIAS, "expr", 1), "Invalid type: expr must be a dict"),
]


@pytest.fixture(scope="module")
def table() -> LTSeq:
    return LTSeq.from_arrow(pa.table({"x": [1, 2, 3]}))


@pytest.mark.parametrize("expr, message", CASES, ids=[m for _, m in CASES])
def test_malformed_expression_dict_names_the_field(table, expr, message):
    with pytest.raises(ValueError) as exc_info:
        table._inner.filter(expr)
    assert str(exc_info.value) == message


def _deserialization_error(table: LTSeq, expr: dict) -> str | None:
    """The deserialization error `expr` raises, ignoring later failures
    (a Window or non-boolean filter is rejected after it deserializes)."""
    try:
        table._inner.filter(expr)
    except Exception as e:  # noqa: BLE001 - classified by message below
        if str(e).startswith(
            ("Missing field", "Invalid type", "Unknown expression type", "Malformed literal payload")
        ):
            return str(e)
    return None


@pytest.mark.parametrize("field", ["on", "partition_by", "order_by"])
@pytest.mark.parametrize("form", ["none", "absent"])
def test_optional_expression_fields_accept_none_or_absence(table, field, form):
    base = CALL if field == "on" else WINDOW
    expr = _with(base, field, None) if form == "none" else _drop(base, field)
    assert _deserialization_error(table, expr) is None
