"""``cast()`` over a window expression in one ``derive`` (#312).

The window planner lowers a cast's input, so a window's result composes
with ``cast`` as it does with arithmetic, ``abs()`` and ``fill_null()``.
Functions without such an arm raise an error that names them, instead of
the row transpiler's message about a window over a window.
"""

import pyarrow as pa
import pytest

from ltseq import LTSeq, ntile, rank


def _table():
    return LTSeq.from_arrow(pa.table({"g": ["a", "b", "a", "b"], "v": [3, 1, 2, 4]}))


def test_cast_over_ranking_window(engine_layout):
    out = _table().sort("v").derive(x=lambda r: ntile(4).over(order_by=r.v).cast("int64"))
    table = out.to_arrow()
    assert table.schema.field("x").type == pa.int64()
    assert table.column("x").to_pylist() == [1, 2, 3, 4]


def test_cast_over_arithmetic_on_a_ranking_window(engine_layout):
    out = _table().sort("v").derive(
        x=lambda r: (5 - ntile(4).over(order_by=r.v)).cast("int64"),
        y=lambda r: (ntile(4).over(order_by=r.v) * 2).cast("int64"),
    )
    table = out.to_arrow()
    assert table.schema.field("x").type == pa.int64()
    assert table.column("x").to_pylist() == [4, 3, 2, 1]
    assert table.column("y").to_pylist() == [2, 4, 6, 8]


def test_cast_over_a_partitioned_rank_subtracts_below_zero(engine_layout):
    # A difference of two ranks cast to int64 goes negative instead of
    # wrapping as an unsigned subtraction does (tutorial case 17).
    out = (
        _table()
        .sort("v")
        .derive(
            a=lambda r: rank().over(partition_by=r.g, order_by=r.v).cast("int64"),
            b=lambda r: rank().over(partition_by=r.g, order_by=r.v, descending=True).cast("int64"),
        )
        .derive(d=lambda r: r.a - r.b)
    )
    assert out.to_dicts() == [
        {"g": "b", "v": 1, "a": 1, "b": 2, "d": -1},
        {"g": "a", "v": 2, "a": 1, "b": 2, "d": -1},
        {"g": "a", "v": 3, "a": 2, "b": 1, "d": 1},
        {"g": "b", "v": 4, "a": 2, "b": 1, "d": 1},
    ]


def test_cast_over_shift(engine_layout):
    out = _table().sort("v").derive(
        x=lambda r: r.v.shift(1).cast("float64"),
        y=lambda r: (r.v.shift(1) + 1).cast("int32"),
    )
    table = out.to_arrow()
    assert table.schema.field("x").type == pa.float64()
    assert table.schema.field("y").type == pa.int32()
    assert table.column("x").to_pylist() == [None, 1.0, 2.0, 3.0]
    assert table.column("y").to_pylist() == [None, 2, 3, 4]


def test_cast_over_a_window_inside_another_window(engine_layout):
    # The staged planner hoists the outer shift's input, a cast of a
    # window, into a stage of its own.
    out = _table().sort("v").derive(x=lambda r: r.v.shift(1).cast("float64").shift(1))
    assert out.to_arrow().column("x").to_pylist() == [None, None, 1.0, 2.0]


def test_cast_over_a_window_keeps_the_declared_order():
    out = _table().sort("v").derive(x=lambda r: r.v.shift(1).cast("float64"))
    assert out.sort_keys == [("v", False)]


def test_cast_over_a_window_rejects_an_unknown_target():
    with pytest.raises(ValueError, match="Unsupported cast target type: timestamp"):
        _table().sort("v").derive(x=lambda r: r.v.shift(1).cast("timestamp"))


@pytest.mark.parametrize(
    ("build", "name"),
    [
        (lambda r: r.v.shift(1).cast("utf8").s.len(), r"\.s\.len\(\)"),
        (lambda r: r.v.shift(1).cast("float64").sqrt(), r"sqrt\(\)"),
        (lambda r: rank().over(order_by=r.v).is_in([1, 2]), r"is_in\(\)"),
    ],
)
def test_a_function_without_a_window_arm_names_itself(build, name):
    with pytest.raises(ValueError, match=name + " cannot take a window expression's result") as info:
        _table().sort("v").derive(x=build)
    assert "another window function" not in str(info.value)
