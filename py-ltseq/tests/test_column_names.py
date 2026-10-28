"""Operations take column names exactly as the schema spells them (#322).

DataFusion reads a column name given as a string as a SQL identifier: it
lowercases `Name` and reads `a.b` as column `b` of a table `a`. The joins,
`link`, the set operations, `lookup`, `align`, `pivot`, `search_pattern`
and the window methods' `partition_by=` built their column references that
way, so a column such as `Date` or `Close` made them fail during execution,
and `rename` quoted names in a way that a `"` in a name breaks.

Each test runs one operation twice: on tables whose columns have plain
lowercase names, and on the same tables with those columns renamed, on
every input, to names with uppercase letters, spaces, dots, double quotes
or non-ASCII characters. The second run must give the first run's result
under the new names.
"""

from __future__ import annotations

import re
import runpy
from pathlib import Path
from typing import Any, Callable, NamedTuple

import pytest

from ltseq import LTSeq
from ltseq.expr import rank


class Names(NamedTuple):
    key: str
    val: str
    txt: str
    tm: str


PLAIN = Names("key", "val", "txt", "tm")
RENAMED = {
    "uppercase": Names("Key", "Val", "Txt", "Tm"),
    "space": Names("my key", "my val", "my txt", "my tm"),
    "dot": Names("k.e", "v.a", "t.x", "t.m"),
    "quote": Names('k"y', 'v"l', 't"x', 't"m'),
    "non-ASCII": Names("键", "值", "文字", "时刻"),
    "mixed": Names("Clé.ID 1", 'Wert "€"', "Text.Ä", "Zeit T"),
}

_PLAIN_NAME = re.compile("|".join(PLAIN))


@pytest.fixture(params=list(RENAMED.values()), ids=list(RENAMED))
def names(request) -> Names:
    return request.param


def col(row: Any, name: str) -> Any:
    """The column `name` of a lambda's row, for names that are not identifiers."""
    return getattr(row, name)


def left(n: Names) -> LTSeq:
    return LTSeq.from_dict(
        {n.key: [1, 2, 3, 3], n.val: [10, 20, 30, 40], n.txt: ["a", "a", "b", "b"], n.tm: [1, 2, 3, 4]}
    )


def right(n: Names) -> LTSeq:
    # The first row equals the left table's first row.
    return LTSeq.from_dict(
        {n.key: [1, 2, 2, 5], n.val: [10, 8, 9, 6], n.txt: ["a", "x", "y", "b"], n.tm: [1, 2, 3, 5]}
    )


def comparable(value: Any, names: Names | None = None) -> Any:
    """`value` in a form two runs can be compared in: a table as its column
    names and its rows in a canonical order. With `names`, the plain names in
    every column name are replaced by `names`, as the renamed run has them."""

    def rename(name: str) -> str:
        if names is None:
            return name
        return _PLAIN_NAME.sub(lambda m: dict(zip(PLAIN, names))[m.group()], name)

    if isinstance(value, LTSeq):
        rows = [{rename(k): v for k, v in row.items()} for row in value.to_dicts()]
        return [rename(c) for c in value.columns], sorted(rows, key=repr)
    if isinstance(value, (list, tuple)):
        return type(value)(comparable(v, names) for v in value)
    return value


def check(op: Callable[[Names], Any], names: Names) -> None:
    """`op` gives the same result on the renamed columns as on plain ones."""
    expected = comparable(op(PLAIN), names)
    if isinstance(expected, tuple):
        assert expected[1], "the plain run returned no rows, so it checks nothing"
    assert comparable(op(names)) == expected


# -- joins -------------------------------------------------------------------


@pytest.mark.parametrize("how", ["inner", "left", "right", "full"])
def test_join_on_a_name(names, how):
    check(lambda n: left(n).join(right(n), on=n.key, how=how), names)


def test_join_on_two_names(names):
    check(lambda n: left(n).join(right(n), on=[n.key, n.txt]), names)


def test_join_left_on_right_on(names):
    check(lambda n: left(n).join(right(n), left_on=n.key, right_on=n.tm), names)


def test_join_on_a_lambda(names):
    check(
        lambda n: left(n).join(right(n), on=lambda a, b: col(a, n.key) == col(b, n.key)),
        names,
    )


def test_merge_join(names):
    check(
        lambda n: left(n).sort(n.key).join(right(n).sort(n.key), on=n.key, strategy="merge"),
        names,
    )


@pytest.mark.parametrize("side", ["left", "right"])
def test_join_with_renamed_columns_on_one_side(names, side):
    """The key is plain on both sides; only one input has renamed columns."""

    def op(n: Names) -> LTSeq:
        renamed = LTSeq.from_dict({"id": [1, 2, 3], n.val: [1, 2, 3], n.txt: ["a", "b", "c"]})
        plain = LTSeq.from_dict({"id": [1, 2, 4], "w": [7, 8, 9]})
        return renamed.join(plain, on="id") if side == "left" else plain.join(renamed, on="id")

    check(op, names)


@pytest.mark.parametrize("how", ["inner", "left"])
@pytest.mark.parametrize("alias", ["p", "Pr"])
def test_link(names, how, alias):
    def op(n: Names) -> LTSeq:
        linked = left(n).link(
            right(n), on=lambda a, b: col(a, n.key) == col(b, n.key), as_=alias, join_type=how
        )
        return linked.filter(lambda r: col(r, n.val) > 0)

    check(op, names)


@pytest.mark.parametrize("op", ["semi_join", "anti_join"])
@pytest.mark.parametrize("on", ["name", "names", "lambda"])
def test_filtering_join(names, op, on):
    def run(n: Names) -> LTSeq:
        keys = {
            "name": n.key,
            "names": [n.key, n.txt],
            "lambda": lambda a, b: col(a, n.key) == col(b, n.key),
        }
        return getattr(left(n), op)(right(n), on=keys[on])

    check(run, names)


@pytest.mark.parametrize("op", ["semi_join", "anti_join"])
def test_filtering_join_on_keys_of_different_types(names, op):
    def run(n: Names) -> LTSeq:
        strings = LTSeq.from_dict({n.key: ["1", "2", "x"], n.val: [1, 2, 3]})
        return getattr(left(n), op)(strings, on=n.key)

    check(run, names)


@pytest.mark.parametrize("by", [None, "name"])
def test_asof_join(names, by):
    def op(n: Names) -> LTSeq:
        return left(n).sort(n.tm).asof_join(
            right(n).sort(n.tm), on=n.tm, by=n.key if by else None
        )

    check(op, names)


# -- set operations ----------------------------------------------------------


def test_union(names):
    check(lambda n: left(n).union(right(n)), names)


@pytest.mark.parametrize("op", ["intersect", "except_", "xunion"])
@pytest.mark.parametrize("on", [None, "name", "lambda"])
def test_set_operation(names, op, on):
    def run(n: Names) -> LTSeq:
        keys = {None: None, "name": n.key, "lambda": lambda r: col(r, n.key)}
        return getattr(left(n), op)(right(n), on=keys[on])

    check(run, names)


def test_is_subset(names):
    assert left(names).head(1).is_subset(right(names))
    assert not left(names).is_subset(right(names))
    assert left(names).head(2).is_subset(right(names), on=names.key)
    assert not left(names).is_subset(right(names), on=names.key)


# -- lookup, align, pivot, partition -----------------------------------------


@pytest.mark.parametrize("join_key", [False, True])
def test_lookup(names, join_key):
    def op(n: Names) -> LTSeq:
        if join_key:
            return left(n).derive(out=lambda r: col(r, n.key).lookup(right(n), n.txt, join_key=n.tm))
        target = right(n).filter(lambda r: col(r, n.tm) != 3)  # one row per key
        return left(n).derive(out=lambda r: col(r, n.key).lookup(target, n.txt))

    check(op, names)


def test_align(names):
    def op(n: Names) -> LTSeq:
        unique = left(n).filter(lambda r: col(r, n.tm) < 4)
        return unique.align([3, 1, 7], key=lambda r: col(r, n.key))

    check(op, names)


@pytest.mark.parametrize("agg_fn", ["sum", "count"])
@pytest.mark.parametrize("index", ["name", "names"])
def test_pivot(names, agg_fn, index):
    def op(n: Names) -> LTSeq:
        idx = n.key if index == "name" else [n.key, n.tm]
        return left(n).pivot(index=idx, columns=n.txt, values=n.val, agg_fn=agg_fn)

    check(op, names)


@pytest.mark.parametrize("by", ["name", "names", "lambda"])
def test_partition(names, by):
    def op(n: Names) -> list:
        args = {"name": (n.txt,), "names": (n.txt, n.key), "lambda": (lambda r: col(r, n.txt),)}
        parts = left(n).partition(*args[by])
        return sorted(parts.items(), key=lambda kv: repr(kv[0]))

    check(op, names)


# -- windows and patterns ----------------------------------------------------

WINDOWS = {
    "shift": lambda v, p: v.shift(1, partition_by=p),
    "diff": lambda v, p: v.diff(1, partition_by=p),
    "rolling": lambda v, p: v.rolling(2, partition_by=p).sum(),
    "cum_sum": lambda v, p: v.cum_sum(partition_by=p),
}


@pytest.mark.parametrize("window", list(WINDOWS))
@pytest.mark.parametrize("form", ["name", "column"])
def test_window_partition_by(names, window, form):
    def op(n: Names) -> LTSeq:
        def out(r):
            part = n.txt if form == "name" else col(r, n.txt)
            return WINDOWS[window](col(r, n.val), part)

        return left(n).sort(n.tm).derive(out=out)

    check(op, names)


def test_over_partition_by(names):
    check(
        lambda n: left(n).derive(
            out=lambda r: rank().over(partition_by=col(r, n.txt), order_by=col(r, n.val))
        ),
        names,
    )


def test_search_pattern(names):
    def op(n: Names) -> LTSeq:
        return left(n).sort(n.tm).search_pattern(
            lambda r: col(r, n.val) > 5, lambda r: col(r, n.val) > 15, partition_by=n.txt
        )

    check(op, names)


def test_search_pattern_count(names):
    def op(n: Names) -> int:
        return left(n).sort(n.tm).search_pattern_count(
            lambda r: col(r, n.val) > 5, lambda r: col(r, n.val) > 15, partition_by=n.txt
        )

    assert op(PLAIN) == 2
    assert op(names) == 2


def test_rename(names):
    check(lambda n: left(n).rename({n.key: "id", n.val: n.val + "!"}), names)


# -- the example the issue names ---------------------------------------------


def test_country_example_runs(monkeypatch, tmp_path, capsys):
    """`examples/country.py` links tables on columns such as `CountryCode`."""
    script = Path(__file__).resolve().parents[2] / "examples" / "country.py"
    monkeypatch.chdir(tmp_path)
    runpy.run_path(str(script), run_name="__main__")
    out = capsys.readouterr().out
    assert "CountryCode" in out
