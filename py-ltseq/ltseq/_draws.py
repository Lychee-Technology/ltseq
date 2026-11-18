"""One value per row for each ``rand()`` (#298).

DataFusion evaluates each copy of a volatile expression separately, and
``random()`` draws a new value for each, while several lowerings write one
operand into the expression they build more than once: ``x.s.islower()`` is
``x = lower(x)``, ``x.diff()`` is ``x - lag(x)``, ``update`` writes its
predicate into one ``CASE`` per updated column, and ``group_ordered`` compares
its key with the key of the previous row. With ``rand()`` in that operand,
each copy tested a different draw.

So the operations that lower a captured expression stage each ``rand()`` it
holds as a column of the table beneath them, compute on the column, and drop
it afterwards. Every copy a lowering makes then reads that one column. A
``rand()`` written twice is still two draws: each place it appears in the
serialized expression is a column of its own, except the copies that
``between`` and ``pct_change`` write themselves, which carry the same
``"draw"`` token (``expr.base._pinned``) and so share one column.

Staging stays on the lazy plan: it is a ``derive`` before the operation and a
``select`` after it. DataFusion keeps the column: it pushes no filter into a
volatile projection, and merges no projection whose non-trivial expression
the one above reads more than once.
"""

from typing import Any, Iterable

_RAND = "math_rand"
_PREFIX = "__ltseq_draw_"


def _holds_draw(value: Any) -> bool:
    if isinstance(value, list):
        return any(_holds_draw(v) for v in value)
    if not isinstance(value, dict) or value.get("type") == "Literal":
        return False
    if value.get("type") == "Call" and value.get("func") == _RAND:
        return True
    return any(_holds_draw(v) for v in value.values())


def stage_draws(
    inner: Any, exprs: list[Any], taken: Iterable[str] = ()
) -> tuple[Any, list[Any], list[str]]:
    """Stage the ``rand()`` calls of serialized expressions as columns.

    Returns the table with one new column per draw, the expressions reading
    those columns in place of the calls, and the new columns' names, which
    are none of the table's columns and none of ``taken``. Expressions
    without ``rand()`` come back unchanged, on the same table.
    """
    if not _holds_draw(exprs):
        return inner, exprs, []
    used = set(inner.get_schema_dict()) | set(taken)
    names: list[str] = []
    by_draw: dict[Any, str] = {}

    def fresh() -> str:
        index = len(names)
        while f"{_PREFIX}{index}" in used:
            index += 1
        name = f"{_PREFIX}{index}"
        used.add(name)
        names.append(name)
        return name

    def rewrite(value: Any) -> Any:
        if isinstance(value, list):
            return [rewrite(v) for v in value]
        if not isinstance(value, dict) or value.get("type") == "Literal":
            return value
        if value.get("type") == "Call" and value.get("func") == _RAND:
            draw = value.get("draw")
            if draw is None:
                return {"type": "Column", "name": fresh()}
            if draw not in by_draw:
                by_draw[draw] = fresh()
            return {"type": "Column", "name": by_draw[draw]}
        return {key: rewrite(v) for key, v in value.items()}

    rewritten = [rewrite(e) for e in exprs]
    call = {"type": "Call", "func": _RAND, "args": [], "kwargs": {}, "on": None}
    staged = inner.derive({name: dict(call) for name in names})
    return staged, rewritten, names


def unstage(inner: Any, names: list[str]) -> Any:
    """Drop the columns :func:`stage_draws` added."""
    if not names:
        return inner
    dropped = set(names)
    return inner.select(
        [
            {"type": "Column", "name": col}
            for col in inner.get_schema_dict()
            if col not in dropped
        ]
    )
