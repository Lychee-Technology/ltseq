# pyright: reportUnnecessaryTypeIgnoreComment=true
"""is_in() takes literal values only (#294).

An expression item, such as a column, raises TypeError when the lambda is
captured. The stubs must reject it too, so the mistake shows in the editor.
Each refused line carries an ignore for the error it has to produce; if the
stubs start accepting an expression item, pyright reports the ignore as
unnecessary and this check fails.
"""

from datetime import date, datetime
from decimal import Decimal

from ltseq import LTSeq, now
from ltseq.expr import LiteralExpr


def expression_items(t: LTSeq) -> None:
    t.derive(v=lambda r: r.x.is_in([r.y]))  # pyright: ignore[reportArgumentType]
    t.derive(v=lambda r: r.x.is_in([r.y, 3]))  # pyright: ignore[reportArgumentType]
    t.derive(v=lambda r: r.x.is_in([r.y + 1]))  # pyright: ignore[reportArgumentType]
    t.filter(lambda r: r.ts.is_in([now()]))  # pyright: ignore[reportArgumentType]


def literal_items(t: LTSeq) -> None:
    t.filter(lambda r: r.x.is_in([1, 2.5, None, True]))
    t.filter(lambda r: r.s.is_in(["a", "b"]))
    t.filter(lambda r: r.d.is_in([date(2024, 1, 1), datetime(2024, 1, 1, 6)]))
    t.filter(lambda r: r.p.is_in([Decimal("1.5")]))
    t.filter(lambda r: r.x.is_in(v for v in (1, 2)))
    t.filter(lambda r: r.x.is_in([LiteralExpr(1)]))
