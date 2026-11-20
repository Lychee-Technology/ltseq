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


# The names DataFusion gives a window function's column when nothing aliases
# it first: its expression text. The windows are now computed under names no
# input column has, so a user column with such a name is one like any other
# (R1-F3 of #418's review).
ROW_NUMBER = "row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"


def group_count_window(group_id: str) -> str:
    return (
        f"count(Int64(1)) PARTITION BY [{group_id}] "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    )


WINDOW_NAMES = [
    ROW_NUMBER,
    group_count_window("__group_id__"),
    group_count_window("__group_id_1__"),
    "__ltseq_window__",
]


@pytest.mark.parametrize(
    "names",
    [
        pytest.param([ROW_NUMBER], id="row_number"),
        pytest.param([group_count_window("__group_id__")], id="group count"),
        pytest.param(
            ["__group_id__", group_count_window("__group_id_1__")],
            id="group count after a fallback",
        ),
        pytest.param(["__ltseq_window__"], id="staged window"),
        pytest.param([*WINDOW_NAMES, *GROUP_FILTER], id="all"),
    ],
)
@pytest.mark.parametrize("label, op", [(label, op) for label, op, _ in OPERATIONS])
def test_an_operation_keeps_a_column_named_like_a_window_function(label, op, names):
    assert_keeps(op, names)


def test_filter_then_flatten_keeps_a_column_named_like_a_working_column():
    """The call the issue names: `filter(...).flatten()` returns every row of
    the groups that pass, with the user's columns as they were and the
    grouping columns under the names flatten() chose (R1-F5 of #418's
    review)."""

    def op(t: LTSeq) -> Any:
        return t.group_ordered(lambda r: r.k).filter(lambda g: g.count() > 1).flatten()

    for names in [[n] for n in GROUP_FILTER] + [GROUP_FILTER, with_second(GROUP_FILTER)]:
        expected = rows(op(table([f"x{i}" for i in range(len(names))])))
        got = rows(op(table(names)))
        back = dict(zip(names, [f"x{i}" for i in range(len(names))]))
        # The grouping columns flatten() shows take the first name of their
        # kind that no user column has.
        moved = {}
        for base in ["__group_id__", "__group_count__", "__rn__"]:
            stem = base.rstrip("_")
            moved[next(n for n in [base, f"{stem}_1__", f"{stem}_2__"] if n not in names)] = base
        renamed = [
            {back.get(c, moved.get(c, c)): v for c, v in row.items()} for row in got
        ]
        assert renamed == expected, names
        # Groups 1 and 3 (k = 1, 1 and 2, 2, 2) pass: five rows.
        assert [row["t"] for row in got] == [0, 1, 2, 3, 4]
        for name in names:
            assert [row[name] for row in got] == [
                row[name] for row in rows(table(names)) if row["t"] <= 4
            ]


@pytest.mark.parametrize("names", [[], ["__rn__"], ["__group_id__", "__group_count__", "__rn__"]])
def test_flatten_reports_the_types_it_exports(names):
    """flatten()'s schema agrees with the Arrow table it exports, for the
    user's columns and the grouping columns (R1-F4 of #418's review)."""
    flat = table(names).group_ordered(lambda r: r.k).flatten()
    exported = flat.to_arrow().schema
    for name in names:
        assert str(exported.field(name).type) == "string"
        assert flat.schema[name] == "string"
    grouping = [c for c in exported.names if c not in ROWS and c not in names]
    assert len(grouping) == 3
    for name in grouping:
        assert str(exported.field(name).type) == "int64", name
        assert flat.schema[name] == "int64", name


def numeric(columns: list[str]) -> LTSeq:
    """The rows with a numeric column under each of ``columns``: ``v``
    times one more than its index."""
    data: dict[str, Any] = dict(ROWS)
    for i, name in enumerate(columns):
        data[name] = [value * (i + 2) for value in ROWS["v"]]
    return LTSeq.from_arrow(pa.table(data)).sort("t")


def read_by_agg(t: LTSeq, name: str) -> Any:
    return rows(t.group_ordered(lambda r: r.k).agg(lambda g: {"z": g.sum(name)}))


def read_by_derive(t: LTSeq, name: str) -> Any:
    return rows(t.group_ordered(lambda r: r.k).derive(lambda g: {"z": g.sum(name)}))


def read_by_filter(t: LTSeq, name: str) -> Any:
    return rows(t.group_ordered(lambda r: r.k).filter(lambda g: g.sum(name) > 100))


READERS = [
    pytest.param(read_by_agg, id="agg"),
    pytest.param(read_by_derive, id="derive"),
    pytest.param(read_by_filter, id="filter"),
]


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize(
    "inputs, missing",
    [
        ([], "__rn__"),
        ([], "__group_id__"),
        ([], "__group_count__"),
        (["__rn__"], "__rn_1__"),
        (["__group_id__"], "__group_id_1__"),
        (["__group_count__"], "__group_count_1__"),
        (["__group_id__", "__group_count__", "__rn__"], "__rn_1__"),
    ],
)
def test_a_group_expression_does_not_read_a_grouping_column(read, inputs, missing):
    """A group expression reading a column the table lacks fails, also when
    the grouping column would have had its name (R1-F2 of #418's review):
    it used to read the group ids, counts or row numbers."""
    with pytest.raises(Exception, match=missing):
        read(numeric(inputs), missing)


@pytest.mark.parametrize("read", READERS)
@pytest.mark.parametrize(
    "inputs, name",
    [
        (["__rn__"], "__rn__"),
        (["__group_id__"], "__group_id__"),
        (["__rn__", "__rn_1__"], "__rn_1__"),
        (["__group_id__", "__group_count__", "__rn__", "__rn_1__"], "__rn_1__"),
    ],
)
def test_a_group_expression_reads_a_user_column_named_like_a_grouping_column(
    read, inputs, name
):
    """A user column with a grouping column's name is the one read."""
    neutral = [f"x{i}" for i in range(len(inputs))]
    expected = read(numeric(neutral), neutral[inputs.index(name)])
    back = dict(zip(inputs, neutral))
    got = read(numeric(inputs), name)
    assert [{back.get(c, c): v for c, v in row.items()} for row in got] == expected


@pytest.mark.parametrize(
    "inputs, outputs",
    [
        ([], ["__rn__"]),
        ([], ["__group_id__", "__group_count__", "__rn__"]),
        ([], ["__group_id__", "__group_id_1__", "__rn__", "__rn_1__"]),
        (["__rn__"], ["__rn_1__"]),
        (["__group_id__", "__group_count__", "__rn__"], ["__group_id_1__", "__group_count_1__", "__rn_1__"]),
    ],
)
def test_derive_outputs_may_have_any_name(inputs, outputs):
    """No output name of a group derive() is reserved: the grouping columns
    take names no output has (R1-F1 of #418's review). The derived values,
    the rows' order and the input columns are those of neutral names."""
    exprs = [lambda g: g.count(), lambda g: g.first().v, lambda g: g.sum("v")]
    neutral_in = [f"x{i}" for i in range(len(inputs))]
    neutral_out = [f"o{i}" for i in range(len(outputs))]
    got = rows(
        table(inputs)
        .group_ordered(lambda r: r.k)
        .derive(lambda g: {name: exprs[i % 3](g) for i, name in enumerate(outputs)})
    )
    expected = rows(
        table(neutral_in)
        .group_ordered(lambda r: r.k)
        .derive(lambda g: {name: exprs[i % 3](g) for i, name in enumerate(neutral_out)})
    )
    back = {**dict(zip(inputs, neutral_in)), **dict(zip(outputs, neutral_out))}
    assert [{back.get(c, c): v for c, v in row.items()} for row in got] == expected


# A group derive() or filter() whose expressions hold windows of several
# sort keys stages them window node by window node, under names such as
# `__ltseq_window__`: `g.first()` orders its group, `g.sum()` and
# `g.count()` do not. A staged name used to be chosen against the input's
# columns only, so an expression reading a column the table lacks could
# read a window staged beneath it (R2-F1 of #418's review).
STAGED = ["__ltseq_window__", "__ltseq_window_1__", "__ltseq_window_2__"]


def staged_derive(t: LTSeq, name: str) -> Any:
    return rows(
        t.group_ordered(lambda r: r.k).derive(
            lambda g: {"a": g.first().v, "b": g.sum(name), "c": g.count()}
        )
    )


def staged_filter(t: LTSeq, name: str) -> Any:
    return rows(
        t.group_ordered(lambda r: r.k).filter(
            lambda g: (g.first().v > 0) & (g.sum(name) > 0) & (g.count() > 0)
        )
    )


STAGED_READERS = [
    pytest.param(staged_derive, id="derive"),
    pytest.param(staged_filter, id="filter"),
]


@pytest.mark.parametrize("read", STAGED_READERS)
@pytest.mark.parametrize(
    "inputs, missing",
    [
        ([], "__ltseq_window__"),
        ([], "__ltseq_window_1__"),
        ([], "__ltseq_window_2__"),
        (["__ltseq_window__"], "__ltseq_window_1__"),
        (["__ltseq_window__", "__ltseq_window_1__"], "__ltseq_window_2__"),
    ],
)
def test_a_group_expression_does_not_read_a_staged_window(read, inputs, missing):
    """An expression reading a column the table lacks fails, also when a
    window staged for another expression would have had its name."""
    with pytest.raises(Exception, match=missing):
        read(numeric(inputs), missing)


@pytest.mark.parametrize("read", STAGED_READERS)
@pytest.mark.parametrize(
    "inputs, name",
    [
        (["__ltseq_window__"], "__ltseq_window__"),
        (["__ltseq_window_1__"], "__ltseq_window_1__"),
        (STAGED, "__ltseq_window__"),
        (STAGED, "__ltseq_window_2__"),
    ],
)
def test_a_group_expression_reads_a_user_column_named_like_a_staged_window(
    read, inputs, name
):
    """A user column with a staged window's name is the one read, beside
    windows of several sort keys."""
    neutral = [f"x{i}" for i in range(len(inputs))]
    expected = read(numeric(neutral), neutral[inputs.index(name)])
    back = dict(zip(inputs, neutral))
    got = read(numeric(inputs), name)
    assert [{back.get(c, c): v for c, v in row.items()} for row in got] == expected
