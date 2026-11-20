"""Operations keep a user column named like a working column of theirs (#346).

Contract §6.4 reserves no column name: LTSeq must not fail or change results
because a user column has a name the implementation uses internally
(acceptance row P18). Each operation below adds working columns to its plan
and drops them afterwards. They used to have fixed names, so an input column
with one of those names made the plan's schema ambiguous and the call
failed. They now take names the input does not have.

Each test is P18's shape: the operation over a table whose column ``x`` is
renamed to an internal name, with the result renamed back, equals the
operation over the table itself.
"""

from typing import Any, Callable

import pyarrow as pa
import pytest

from ltseq import LTSeq

# The working columns of each operation. `__count__` is the call
# NestedTable.count() builds, not a column; it is probed all the same.
DISTINCT = ["__distinct_pos__", "__distinct_rn__"]
RVS = ["__rvs_pos__"]
STEP = ["__step_pos__"]
GROUPING = ["__boundary__", "__group_id__", "__group_count__", "__rn__"]
GROUP_FILTER = [*GROUPING, "__group_filter__"]
ALIGN = ["__ref_key__", "__pos__"]
EVERY = sorted({*DISTINCT, *RVS, *STEP, *GROUP_FILTER, *ALIGN, "__count__"})

# The declared order is by `t`; `k` runs 1, 1, 2, 2, 2, 1, 3 in it, so the
# runs of `k` are not its groups.
ROWS = {
    "t": [0, 1, 2, 3, 4, 5, 6],
    "k": [1, 1, 2, 2, 2, 1, 3],
    "v": [10, 20, 30, 40, 50, 60, 70],
}
# The column renamed: strings, so a name it takes keeps neither the
# internal column's values nor its type.
X = ["a", "b", "c", "d", "e", "f", "g"]


def table(columns: list[str]) -> LTSeq:
    """The rows, with a copy of ``x`` under each of ``columns``."""
    data: dict[str, Any] = dict(ROWS)
    for i, name in enumerate(columns):
        data[name] = [f"{value}{i}" for value in X]
    return LTSeq.from_arrow(pa.table(data)).sort("t")


def rows(result: Any) -> list[dict[str, Any]]:
    """The rows of ``result``, a missing value as None so that rows compare
    equal (NaN does not)."""
    d = result.to_pandas()
    return d.astype(object).where(d.notna(), None).to_dict("records")


def assert_keeps(op: Callable[[LTSeq], Any], names: list[str]) -> None:
    """``op`` over a table with columns ``names`` equals ``op`` over the
    same table with these columns named ``x0``, ``x1``, ... instead."""
    neutral = [f"x{i}" for i in range(len(names))]
    expected = rows(op(table(neutral)))
    got = rows(op(table(names)))
    back = dict(zip(names, neutral))
    # Wherever the result has the user's columns, they keep their names and
    # values, and every other column is as it was.
    assert [{back.get(c, c): v for c, v in row.items()} for row in got] == expected


def with_second(names: list[str]) -> list[str]:
    """Each name and the name its operation takes when that one is taken."""
    seconds = [f"{n.rstrip('_')}_1{n[len(n.rstrip('_')):]}" for n in names]
    return [*names, *seconds]


def params(names: list[str]) -> list[Any]:
    """One case per internal name, one with all of them, and one with all of
    them and the names they fall back to."""
    return [
        *[pytest.param([n], id=n) for n in names],
        pytest.param(names, id="all"),
        pytest.param(with_second(names), id="all-and-fallbacks"),
    ]


OPERATIONS: list[tuple[str, Callable[[LTSeq], Any], list[str]]] = [
    ("distinct", lambda t: t.distinct("k"), DISTINCT),
    ("rvs", lambda t: t.rvs(), RVS),
    ("step", lambda t: t.step(2), STEP),
    ("first", lambda t: t.group_ordered(lambda r: r.k).first(), GROUPING),
    ("last", lambda t: t.group_ordered(lambda r: r.k).last(), GROUPING),
    (
        "group_sorted-first",
        lambda t: t.sort("k", "t").group_sorted(lambda r: r.k).first(),
        GROUPING,
    ),
    (
        "filter",
        lambda t: t.group_ordered(lambda r: r.k).filter(lambda g: g.count() > 1),
        GROUP_FILTER,
    ),
    (
        "filter-first",
        lambda t: t.group_ordered(lambda r: r.k)
        .filter(lambda g: (g.count() > 1) & (g.first().v < 50))
        .first(),
        GROUP_FILTER,
    ),
    (
        "derive",
        lambda t: t.group_ordered(lambda r: r.k).derive(
            lambda g: {"n": g.count(), "f": g.first().v, "l": g.last().v, "s": g.sum("v")}
        ),
        GROUPING,
    ),
    (
        "agg",
        lambda t: t.group_ordered(lambda r: r.k).agg(
            lambda g: {"n": g.count(), "f": g.first().v, "l": g.last().v, "s": g.sum("v")}
        ),
        [],
    ),
    ("align", lambda t: t.align([3, 0, 9, 5], key=lambda r: r.t), ALIGN),
]


@pytest.mark.parametrize(
    "op, names",
    [
        pytest.param(op, names, id=f"{label}-{p.id}")
        for label, op, used in OPERATIONS
        for p in params(used or GROUPING)
        for names in [p.values[0]]
    ]
    + [pytest.param(op, EVERY, id=f"{label}-every") for label, op, _ in OPERATIONS],
)
def test_an_operation_keeps_a_column_named_like_its_working_columns(op, names):
    assert_keeps(op, names)


@pytest.mark.parametrize("names", params(GROUPING))
def test_group_count_keeps_a_column_named_like_a_grouping_column(names):
    """`first().count()` counts the groups of a run predicate on its own
    path (a linear scan), and of any other key through `first()`."""
    for key in (lambda r: r.k, lambda r: r.k != r.k.shift(1)):
        assert table(names).group_ordered(key).first().count() == table([]).group_ordered(
            key
        ).first().count()


@pytest.mark.parametrize("names", params(GROUPING))
def test_flatten_names_its_grouping_columns_around_the_users(names):
    """flatten() shows its grouping columns. A user column with the name of
    one keeps that name and its values, and the grouping column takes the
    first free name of its kind."""
    plain = table([]).group_ordered(lambda r: r.k).flatten().to_pandas()
    flat = table(names).group_ordered(lambda r: r.k).flatten().to_pandas()
    for name in names:
        assert flat[name].tolist() == table(names).to_pandas()[name].tolist()
    for name in ["__group_id__", "__group_count__", "__rn__"]:
        free = next(
            n
            for n in [name, *(f"{name.rstrip('_')}_{i}__" for i in range(1, 9))]
            if n not in names
        )
        assert flat[free].tolist() == plain[name].tolist()
    assert len(flat.columns) == len(plain.columns) + len(names)


@pytest.mark.parametrize(
    "outputs",
    [["__group_id__"], ["__rn__"], ["__group_id__", "__group_id_1__", "__rn__"]],
    ids=["group id", "row number", "and their fallbacks"],
)
@pytest.mark.parametrize("inputs", [[], ["__group_id__", "__rn__"]], ids=["", "named inputs"])
def test_agg_outputs_may_have_any_name(inputs, outputs):
    """No output name of agg() is reserved either: the grouping columns take
    names no output has. It used to reject `__group_id__` and `__rn__`."""
    t = table(inputs)
    exprs = [lambda g: g.count(), lambda g: g.first().v, lambda g: g.sum("v")]
    result = t.group_ordered(lambda r: r.k).agg(
        lambda g: {name: exprs[i % 3](g) for i, name in enumerate(outputs)}
    )
    expected = (
        table([])
        .group_ordered(lambda r: r.k)
        .agg(lambda g: {f"o{i}": exprs[i % 3](g) for i in range(len(outputs))})
    )
    assert list(result.to_pandas().columns) == outputs
    assert result.to_pandas().values.tolist() == expected.to_pandas().values.tolist()
