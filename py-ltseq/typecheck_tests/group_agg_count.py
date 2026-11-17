# pyright: reportUnnecessaryTypeIgnoreComment=true
"""`g.count` in `group_by().agg()` and `LTSeq.agg()` lambdas (#314).

`g.count()` counts a group's rows. A column named `count` keeps its name, so
`g.count.sum()` aggregates that column, and the `count_*` columns of a link
with `as_="count"` read as `g.count.v`; the `first_*` / `last_*` columns of
`as_="first"` / `as_="last"` read as `g.first.v` / `g.last.v`. The stubs must
accept every form in both entry points.
"""

from ltseq import LTSeq


def row_count(t: LTSeq) -> None:
    t.group_by("k").agg(n=lambda g: g.count())
    t.agg(n=lambda g: g.count())
    t.agg(by=lambda r: r.k, n=lambda g: g.count())


def count_column(t: LTSeq) -> None:
    t.group_by("k").agg(total=lambda g: g.count.sum())
    t.agg(total=lambda g: g.count.sum())
    t.group_by("k").agg(total=lambda g: g.count.max(), n=lambda g: g.count())


def linked_alias_columns(t: LTSeq) -> None:
    t.group_by("k").agg(total=lambda g: g.count.v.sum())
    t.agg(total=lambda g: g.count.v.sum())
    t.group_by("k").agg(total=lambda g: g.first.v.sum(), last=lambda g: g.last.v.max())
    t.agg(total=lambda g: g.first.v.sum(), last=lambda g: g.last.v.max())
