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

from typing import Any, Callable, Iterable, Iterator

_RAND = "math_rand"
_PREFIX = "__ltseq_draw_"


def containers(value: Any, skip: Callable[[dict], bool] = lambda node: False) -> Iterator[Any]:
    """Each dict and list of a serialized expression, once, but for the
    dicts ``skip`` accepts and what is beneath them.

    The walk keeps its own stack: recursing through ``any()`` or
    ``set().union()`` takes C stack per level, which a deep expression
    exhausts on a small thread (#335). It visits a dict or list reached
    twice, a subtree the expression reuses, once, and raises ``ValueError``
    on one that contains itself, which no expression does and which would
    otherwise keep the walk going forever (R1-F3 of #335's review).
    """
    on_path: set[int] = set()
    done: set[int] = set()
    # (container, True) leaves the container once its children are walked.
    pending: list[tuple[Any, bool]] = [(value, False)]
    while pending:
        item, leaving = pending.pop()
        if leaving:
            on_path.discard(id(item))
            done.add(id(item))
            continue
        if isinstance(item, dict):
            if skip(item):
                continue
            children: Iterable[Any] = item.values()
        elif isinstance(item, list):
            children = item
        else:
            continue
        key = id(item)
        if key in on_path:
            raise ValueError(
                "A serialized expression contains itself; an expression must be a tree"
            )
        if key in done:
            continue
        on_path.add(key)
        pending.append((item, True))
        yield item
        pending.extend((child, False) for child in children)


def _nodes(value: Any) -> Iterable[dict]:
    """The expression nodes of serialized expressions, but for literals."""
    for item in containers(value, skip=lambda node: node.get("type") == "Literal"):
        if isinstance(item, dict):
            yield item


def _holds_draw(value: Any) -> bool:
    return any(
        node.get("type") == "Call" and node.get("func") == _RAND
        for node in _nodes(value)
    )


# Group-dialect nodes naming the column they read in ``column`` (R3-F1 of
# #413's review): ``g.sum('x')``, ``g.first().x``.
_GROUP_COLUMN_NODES = ("GroupAgg", "GroupRowColumn")


def _columns(value: Any) -> set[str]:
    """The names of the columns serialized expressions read, in the row
    dialect and in the group dialect."""
    names = set()
    for node in _nodes(value):
        if node.get("type") == "Column" and isinstance(node.get("name"), str):
            names.add(node["name"])
        if node.get("type") in _GROUP_COLUMN_NODES and isinstance(node.get("column"), str):
            names.add(node["column"])
    return names


def stage_draws(
    inner: Any, exprs: list[Any], taken: Iterable[str] = ()
) -> tuple[Any, list[Any], list[str]]:
    """Stage the ``rand()`` calls of serialized expressions as columns.

    Returns the table with one new column per draw, the expressions reading
    those columns in place of the calls, and the new columns' names, which
    are none of the table's columns, none of ``taken`` and none of the
    columns the expressions read: a reference to a missing column stays one
    (R1-F5 of #413's review). Expressions without ``rand()`` come back
    unchanged, on the same table.
    """
    if not _holds_draw(exprs):
        return inner, exprs, []
    used = set(inner.get_schema_dict()) | set(taken) | _columns(exprs)
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


def rename_draws(
    inner: Any, exprs: list[Any], names: list[str], taken: Iterable[str]
) -> tuple[Any, list[Any], list[str]]:
    """Rename the staged columns ``names`` of ``inner`` that are in
    ``taken``, and the references of ``exprs`` to them (R3-F1 of #413's
    review).

    A table that keeps its draws staged across operations (a group filter's
    result) may meet a later expression reading a missing column named like
    one of them: renamed, the draw takes a name none of the table's columns,
    ``taken`` or ``exprs`` has, and the reference stays a missing column.
    Returns the table, the expressions and the staged columns' names.
    """
    clash = set(names) & set(taken)
    if not clash:
        return inner, exprs, list(names)
    schema = list(inner.get_schema_dict())
    used = set(schema) | set(taken) | _columns(exprs)
    renamed: dict[str, str] = {}
    for old in names:
        if old in clash:
            index = 0
            while f"{_PREFIX}{index}" in used:
                index += 1
            renamed[old] = f"{_PREFIX}{index}"
            used.add(renamed[old])

    def rewrite(value: Any) -> Any:
        if isinstance(value, list):
            return [rewrite(v) for v in value]
        if not isinstance(value, dict) or value.get("type") == "Literal":
            return value
        if value.get("type") == "Column" and value.get("name") in renamed:
            return {**value, "name": renamed[value["name"]]}
        return {key: rewrite(v) for key, v in value.items()}

    projection = [
        {"type": "Alias", "expr": {"type": "Column", "name": col}, "alias": renamed[col]}
        if col in renamed
        else {"type": "Column", "name": col}
        for col in schema
    ]
    return (
        inner.select(projection),
        [rewrite(e) for e in exprs],
        [renamed.get(name, name) for name in names],
    )


def kept_names(
    inner: Any, staged: Any, originals: list[Any], rewritten: list[Any]
) -> dict[str, str]:
    """The renames that give each expression :func:`stage_draws` changed
    the name its original has in a projection of ``inner`` (R1-F3 of #413's
    review): the name the rewritten expression takes over ``staged``, mapped
    to the original's.

    A projection or a group key that is not a column takes its expression's
    text as its name, and the text of a staged expression names the staged
    column (``round(__ltseq_draw_0,Int64(0))`` for ``round(random(),
    Int64(0))``). The operation renames its output column afterwards rather
    than wrap the expression in an alias, which would make it one level
    deeper than the caller wrote it, past the depth limit for an expression
    at the limit (R1-F2 of #335's review). An expression the plan cannot
    name is left as it is, for the operation to report.
    """
    renames: dict[str, str] = {}
    for original, new in zip(originals, rewritten):
        if new != original and isinstance(original, dict) and original.get("type") != "Alias":
            try:
                (name,) = inner.select([original]).get_schema_dict()
                (staged_name,) = staged.select([new]).get_schema_dict()
            except Exception:
                continue
            if staged_name != name:
                renames[staged_name] = name
    return renames


def renamed(inner: Any, renames: dict[str, str]) -> Any:
    """``inner`` with the columns :func:`kept_names` lists renamed."""
    return inner.rename_columns(renames) if renames else inner


def unstage(inner: Any, names: list[str]) -> Any:
    """Drop the columns :func:`stage_draws` added."""
    dropped = set(names) & set(inner.get_schema_dict())
    if not dropped:
        return inner
    return inner.select(
        [
            {"type": "Column", "name": col}
            for col in inner.get_schema_dict()
            if col not in dropped
        ]
    )
