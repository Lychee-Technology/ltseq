"""AST transformation and lambda expression capturing for LTSeq."""

import __future__
import ast
import collections
import copy
import dis
import functools
import inspect
import operator
import os
import pathlib
import sysconfig
import types
from typing import Any, Callable, Iterator, NamedTuple, cast

from .base import Expr
from .proxy import SchemaProxy


_NULL_CHECK_HELPER = "__ltseq_null_check__"

# ``co_flags`` bits recording the ``from __future__`` features of the defining
# module. They are part of a lambda's compilation context, not of the lambda.
_FUTURE_FLAGS = functools.reduce(
    operator.or_,
    (getattr(__future__, name).compiler_flag for name in __future__.all_feature_names),
    0,
)

# Opcodes the compiler emits for ``x is None`` / ``x is not None`` used as a
# branch condition (``if``, conditional expressions).
_NONE_JUMP_OPNAMES = frozenset({"POP_JUMP_IF_NONE", "POP_JUMP_IF_NOT_NONE"})


def _null_check(value: Any, negate: bool) -> Any:
    """Runtime target of a rewritten ``x is None`` / ``x is not None``.

    Expression operands become explicit null predicates; anything else keeps
    plain Python identity semantics, so captured Python values such as
    ``lambda r: r.a > th if th is not None else r.a`` behave as written.
    """
    if isinstance(value, Expr):
        return value.is_not_null() if negate else value.is_null()
    return (value is not None) if negate else (value is None)


def _is_none_constant(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _none_check_operand(node: ast.Compare) -> tuple[ast.expr, bool] | None:
    """Return ``(operand, negate)`` if ``node`` is ``x is None`` / ``x is not None``
    (or the reversed spelling), else ``None``. Chained comparisons are not rewritten."""
    if len(node.ops) != 1 or not isinstance(node.ops[0], (ast.Is, ast.IsNot)):
        return None
    left, right = node.left, node.comparators[0]
    if _is_none_constant(right) and not _is_none_constant(left):
        operand = left
    elif _is_none_constant(left) and not _is_none_constant(right):
        operand = right
    else:
        return None
    return operand, isinstance(node.ops[0], ast.IsNot)


def _contains_none_check(node: ast.AST) -> bool:
    return any(
        isinstance(sub, ast.Compare) and _none_check_operand(sub) is not None
        for sub in ast.walk(node)
    )


def _unused_name(node: ast.AST, base: str) -> str:
    """Return ``base``, suffixed if needed, so that no parameter or name
    reference inside ``node`` resolves to it."""
    used = {
        sub.id if isinstance(sub, ast.Name) else sub.arg
        for sub in ast.walk(node)
        if isinstance(sub, (ast.Name, ast.arg))
    }
    stem, dunder = (base[:-2], "__") if base.endswith("__") else (base, "")
    name, counter = base, 0
    while name in used:
        counter += 1
        # Keep the dunder form: ``__x1__`` is exempt from private-name
        # mangling inside class bodies, ``__x__1`` is not.
        name = f"{stem}{counter}{dunder}"
    return name


class _NoneCheckRewriter(ast.NodeTransformer):
    """Rewrite ``x is None`` / ``x is not None`` into ``helper(x, negate)``.

    ``is`` cannot be overloaded in Python, so the comparison is redirected to
    :func:`_null_check`, which yields ``x.is_null()`` / ``x.is_not_null()`` for
    expressions and plain identity checks for everything else. ``helper`` is an
    identifier the lambda does not otherwise use (see :func:`_unused_name`), so
    user bindings are never shadowed.
    """

    def __init__(self, helper: str) -> None:
        self.helper = helper

    def visit_Compare(self, node: ast.Compare) -> ast.expr:
        node = cast(ast.Compare, self.generic_visit(node))
        match = _none_check_operand(node)
        if match is None:
            return node
        operand, negate = match
        call = ast.Call(
            func=ast.Name(id=self.helper, ctx=ast.Load()),
            args=[operand, ast.Constant(value=negate)],
            keywords=[],
        )
        return ast.copy_location(call, node)


class _LambdaSite(NamedTuple):
    node: ast.Lambda
    # Innermost enclosing class, if any: names such as ``self.__x`` inside it
    # are privately mangled, so a candidate must be compiled in the same class.
    class_name: str | None


# filename -> (line list handed out by linecache, lineno -> lambdas that use `is None`)
_ModuleIndex = dict[int, list[_LambdaSite]]
_module_index_cache: dict[str, tuple[list[str], _ModuleIndex]] = {}


def _module_none_check_lambdas(fn: types.FunctionType) -> _ModuleIndex | None:
    """Parse the module that defines ``fn`` (cached) and index, by line, the
    lambdas whose body contains an ``is None`` / ``is not None`` comparison.

    Returns ``None`` when the source is unavailable (REPL, ``eval`` strings) or
    does not parse as the running interpreter's Python.
    """
    try:
        lines, _ = inspect.findsource(fn)
    except (OSError, TypeError):
        return None
    filename = fn.__code__.co_filename
    cached = _module_index_cache.get(filename)
    if cached is not None and cached[0] is lines:
        return cached[1]
    try:
        module = ast.parse("".join(lines), filename)
    except (SyntaxError, ValueError):
        return None
    index: _ModuleIndex = {}
    stack: list[tuple[ast.AST, str | None]] = [(module, None)]
    while stack:
        node, class_name = stack.pop()
        if isinstance(node, ast.ClassDef):
            class_name = node.name
        elif isinstance(node, ast.Lambda) and _contains_none_check(node):
            index.setdefault(node.lineno, []).append(_LambdaSite(node, class_name))
        stack.extend((child, class_name) for child in ast.iter_child_nodes(node))
    _module_index_cache[filename] = (lines, index)
    return index


def _nested_code(code: types.CodeType, depth: int) -> types.CodeType:
    """Follow the first code-object constant ``depth`` levels down."""
    for _ in range(depth):
        code = next(c for c in code.co_consts if isinstance(c, types.CodeType))
    return code


def _compile_lambda_in_scope(
    site: _LambdaSite, scope_names: tuple[str, ...], context: types.CodeType
) -> types.CodeType:
    """Compile the lambda at ``site`` as if it were nested in a function whose
    locals are ``scope_names`` and return the lambda's own code object.

    Names in ``scope_names`` that the lambda references compile to closure
    loads exactly as they did in the original function; every other name stays
    a global lookup. The compilation context otherwise matches the original:
    the ``from __future__`` features in effect for ``context`` are applied, and
    the wrapper is placed inside a class of the same name as the lambda's
    enclosing class so private names mangle identically. Only ``CO_NESTED``
    can differ from the original (see :func:`_locate_lambda`). The node is not
    mutated.
    """
    node = site.node
    wrapper = ast.Lambda(
        args=ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg=name) for name in scope_names],
            vararg=None,
            kwonlyargs=[],
            kw_defaults=[],
            kwarg=None,
            defaults=[],
        ),
        body=node,
    )
    ast.copy_location(wrapper, node)
    tree: ast.AST
    if site.class_name is None:
        tree, mode, depth = ast.Expression(body=wrapper), "eval", 2
    else:
        tree = ast.parse(f"class {site.class_name}:\n    pass")
        class_def = cast(ast.ClassDef, tree.body[0])
        class_def.body = [ast.Expr(value=wrapper)]
        ast.copy_location(class_def, node)
        mode, depth = "exec", 3
    compiled = compile(
        ast.fix_missing_locations(tree),
        context.co_filename,
        mode,
        flags=context.co_flags & _FUTURE_FLAGS,
        dont_inherit=True,
    )
    return _nested_code(compiled, depth)


def _without_nesting_flag(code: types.CodeType) -> types.CodeType:
    return code.replace(co_flags=code.co_flags & ~inspect.CO_NESTED)


def _locate_lambda(fn: types.FunctionType, index: _ModuleIndex) -> _LambdaSite | None:
    """Find the AST node ``fn`` was compiled from, by compiling each lambda on
    its first line (with the same free variables) and comparing code objects.

    Byte-for-byte identity is what makes this independent of how many lambdas
    share a line, of string literals on the line, and of line continuations.
    Candidates are always compiled inside a wrapper function, which sets
    ``CO_NESTED``; a lambda defined at module or class-body scope lacks that
    flag, so it is ignored on both sides.
    """
    code = fn.__code__
    target = _without_nesting_flag(code)
    for site in index.get(code.co_firstlineno, ()):
        compiled = _compile_lambda_in_scope(site, code.co_freevars, code)
        if _without_nesting_flag(compiled) == target:
            return site
    return None


def _transform_lambda_for_none_checks(fn: Callable) -> Callable:
    """Return ``fn`` with ``is None`` / ``is not None`` rewritten to explicit
    null checks, or ``fn`` itself when nothing needs rewriting.

    Only lambdas are rewritten: the lambda is located in its module's AST by
    compiled identity, rewritten by :class:`_NoneCheckRewriter`, recompiled
    with the same free variables, and rebuilt on the original ``__globals__``
    and the original closure cells (matched by name, so live bindings are
    preserved). A ``functools.partial`` of a lambda is rebuilt around the
    rewritten lambda with the same bound arguments. When the source cannot
    be inspected, ``fn`` is returned unchanged and :func:`_invoke_row_lambda`
    refuses to run it if it contains a null identity check.
    """
    if isinstance(fn, functools.partial):
        # Only an exact partial is rebuilt: a subclass may change how it calls
        # its function, so it is left to the check in _invoke_row_lambda.
        if type(fn) is not functools.partial:
            return fn
        inner = _transform_lambda_for_none_checks(fn.func)
        if inner is fn.func:
            return fn
        return functools.partial(inner, *fn.args, **fn.keywords)
    if not isinstance(fn, types.FunctionType) or fn.__code__.co_name != "<lambda>":
        return fn
    index = _module_none_check_lambdas(fn)
    if not index:
        return fn
    site = _locate_lambda(fn, index)
    if site is None:
        return fn

    original = fn.__code__
    helper = _unused_name(site.node, _NULL_CHECK_HELPER)
    rewritten = _NoneCheckRewriter(helper).visit(copy.deepcopy(site.node))
    new_code = _compile_lambda_in_scope(
        _LambdaSite(rewritten, site.class_name), original.co_freevars + (helper,), original
    )

    cells = dict(zip(original.co_freevars, fn.__closure__ or ()))
    cells[helper] = types.CellType(_null_check)
    closure = tuple(cells[name] for name in new_code.co_freevars)

    new_fn = types.FunctionType(
        new_code, fn.__globals__, fn.__name__, fn.__defaults__, closure
    )
    new_fn.__kwdefaults__ = fn.__kwdefaults__
    new_fn.__qualname__ = fn.__qualname__
    return new_fn


_NULL_METHOD_GUIDANCE = (
    "Use the is_null() or is_not_null() methods instead:\n"
    "  - r.col.is_null()      instead of  r.col is None\n"
    "  - r.col.is_not_null()  instead of  r.col is not None\n"
    "LTSeq rewrites 'is None' checks automatically only inside a lambda "
    "(or a functools.partial of one) whose source file is available. In a "
    "REPL, an exec/eval string, a plain def function, a helper the lambda "
    "calls or a lambda behind another wrapper (a decorator, "
    "functools.lru_cache, ...), use the methods, and test Python values "
    "(such as an optional threshold) before building the function, or "
    "against a sentinel object rather than None."
)


def _source_span(
    positions: dis.Positions | None,
) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """``((line, col), (end_line, end_col))``, or ``None`` if any part is
    missing (``-X no_debug_ranges`` drops column offsets)."""
    if positions is None:
        return None
    line, end_line, col, end_col = positions
    if line is None or end_line is None or col is None or end_col is None:
        return None
    return (line, col), (end_line, end_col)


def _may_be_operand_of(
    value: dis.Positions | None, comparison: dis.Positions | None
) -> bool:
    """Whether the expression at ``value`` can be an operand of the
    comparison at ``comparison``, judged by source span.

    An operand lies inside the comparison's span and is strictly smaller,
    since the span also covers the operator and the other operand. (The
    implicit ``return None`` of a function can inherit the span of its last
    ``if`` test: equal, not smaller.) Without a span the answer is yes, so
    the check stays closed.
    """
    inner, outer = _source_span(value), _source_span(comparison)
    if inner is None or outer is None:
        return True
    return outer[0] <= inner[0] and inner[1] <= outer[1] and inner != outer


@functools.lru_cache(maxsize=1024)
def _tests_identity_with_none(code: types.CodeType) -> bool:
    """Whether ``code``, or a function nested in it, compares a value with
    ``None`` by identity (``x is None`` / ``x is not None``).

    Read from bytecode, so it needs no source: a ``POP_JUMP_IF_NONE`` /
    ``POP_JUMP_IF_NOT_NONE`` (the check used as a branch condition), or an
    ``IS_OP`` whose source span encloses a load of the ``None`` constant
    (the check used as a value; see :func:`_may_be_operand_of`). Pairing by
    span rather than by stack position also covers ``None is (a if c else b)``,
    where the compiler emits one ``IS_OP`` per branch after the ``None`` load.
    A ``None`` nested deeper inside an operand (``f(None) is marker``) still
    matches; that errs towards rejecting a function, never towards running
    one whose check was not rewritten.
    """
    instructions = list(dis.get_instructions(code))
    if any(instr.opname in _NONE_JUMP_OPNAMES for instr in instructions):
        return True
    none_loads = [
        instr.positions
        for instr in instructions
        if instr.opname == "LOAD_CONST" and instr.argval is None
    ]
    if any(
        _may_be_operand_of(none_load, instr.positions)
        for instr in instructions
        if instr.opname == "IS_OP"
        for none_load in none_loads
    ):
        return True
    return any(
        _tests_identity_with_none(const)
        for const in code.co_consts
        if isinstance(const, types.CodeType)
    )


# The top-level package of this module. Its functions test Python values
# with `is None` as a matter of course (`log(x, base=None)`).
_PACKAGE = __name__.partition(".")[0]

# Directories that pip and Linux distributions install packages into.
_INSTALL_DIR_NAMES = frozenset({"site-packages", "dist-packages"})


def _normalized_path(path: str) -> str:
    return os.path.normcase(os.path.realpath(path))


@functools.cache
def _stdlib_dirs() -> tuple[str, ...]:
    paths = sysconfig.get_paths()
    return tuple(
        os.path.join(_normalized_path(paths[key]), "")
        for key in ("stdlib", "platstdlib")
        if key in paths
    )


@functools.lru_cache(maxsize=1024)
def _is_library_file(filename: str) -> bool:
    """Whether code compiled from ``filename`` belongs to the standard
    library or to an installed distribution rather than to the user."""
    if filename.startswith("<"):
        # `<frozen posixpath>` is a standard-library module frozen into the
        # interpreter; `<string>`, `<stdin>` and the like are user input.
        return filename.startswith("<frozen ")
    path = _normalized_path(filename)
    return bool(
        _INSTALL_DIR_NAMES.intersection(pathlib.PurePath(path).parts)
    ) or path.startswith(_stdlib_dirs())


def _is_library_code(obj: Any, code: types.CodeType) -> bool:
    module = getattr(obj, "__module__", None)
    if isinstance(module, str) and module.partition(".")[0] == _PACKAGE:
        return True
    return _is_library_file(code.co_filename)


@functools.lru_cache(maxsize=1024)
def _global_names(code: types.CodeType) -> tuple[str, ...]:
    """The global names that ``code``, or a function nested in it, loads,
    in order of first use."""
    names = dict.fromkeys(
        instr.argval
        for instr in dis.get_instructions(code)
        if instr.opname == "LOAD_GLOBAL"
    )
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names.update(dict.fromkeys(_global_names(const)))
    return tuple(names)


def _named_callables(func: types.FunctionType) -> Iterator[Any]:
    """The callables ``func`` refers to by name: the globals its code loads,
    looked up in ``func.__globals__`` and then its builtins as
    ``LOAD_GLOBAL`` does, and the contents of its closure cells."""
    values: list[Any] = []
    for name in _global_names(func.__code__):
        for namespace in (func.__globals__, func.__builtins__):
            if name in namespace:
                values.append(namespace[name])
                break
    for cell in func.__closure__ or ():
        try:
            values.append(cell.cell_contents)
        except ValueError:
            pass  # not bound yet, as for a function defined after the lambda
    return (value for value in values if callable(value))


class _ReachedCode(NamedTuple):
    code: types.CodeType
    # Qualified names of the functions it was reached through by name,
    # outermost first; empty for code that calling the function runs directly.
    via: tuple[str, ...]


# An object for _called_code to visit, with the `via` of _ReachedCode.
_Visit = tuple[Any, tuple[str, ...]]


def _called_code(fn: Callable) -> Iterator[_ReachedCode]:
    """The Python code objects that calling ``fn`` can run, as far as they
    are found without running it, nearest first.

    That is the code ``fn`` runs directly: a function's or bound method's
    own, a callable instance's ``__call__``, and those of the functions a
    wrapper forwards to (``functools.partial``, nested or not,
    ``staticmethod`` / ``classmethod``, and anything that sets
    ``__wrapped__``, such as ``functools.wraps`` decorators and
    ``functools.lru_cache``). It is also the code of every user function
    that a function found this way refers to by name (see
    :func:`_named_callables`), which is how a helper is usually called, and
    so on through those functions' names.

    Wrappers are followed rather than treated as opaque because the ones
    implemented in C have no code of their own: stopping at them would let
    the function they call run unchecked. Attributes are read where these
    wrappers keep them, never through ``__getattr__``, so an object that
    fabricates attributes on demand (a ``Mock``) cannot make the walk endless.

    Names are followed only into user code. Functions of this package, of
    the standard library and of installed distributions are skipped: they
    test Python values with ``is None`` routinely, and checking them would
    refuse ordinary lambdas. A callable reached other than by name is not
    followed: an attribute (``helpers.missing(r)``, ``self.check(r)``), an
    argument or a default value. Nor are the methods of a class, which
    ``type.__call__``, written in C, runs.
    """
    pending: collections.deque[_Visit] = collections.deque([(fn, ())])
    # Keyed by id, holding the object so the id cannot be reused mid-walk.
    seen: dict[int, Any] = {}
    while pending:
        obj, via = pending.popleft()
        if id(obj) in seen:
            continue
        seen[id(obj)] = obj
        try:
            reached, following = _visit(obj, via)
        except Exception:
            # The user never handed an object found by name to LTSeq, so its
            # failing attribute access (werkzeug's LocalProxy outside a
            # request, say) must not fail the capture; it is skipped.
            if not via:
                raise
            continue
        if reached is not None:
            yield reached
        pending.extend(following)


def _visit(obj: Any, via: tuple[str, ...]) -> tuple[_ReachedCode | None, list[_Visit]]:
    """One step of :func:`_called_code`: ``obj``'s code, unless ``obj`` is
    library code reached by name, and the objects to visit after it."""
    reached = None
    following: list[_Visit] = []
    code = getattr(obj, "__code__", None)
    has_code = isinstance(code, types.CodeType)
    if has_code and not (via and _is_library_code(obj, code)):
        reached = _ReachedCode(code, via)
        if isinstance(obj, types.FunctionType):
            inner = via + (obj.__qualname__,)
            following.extend((callee, inner) for callee in _named_callables(obj))
    if isinstance(obj, functools.partial):
        following.append((obj.func, via))
    elif isinstance(obj, (types.MethodType, staticmethod, classmethod)):
        following.append((obj.__func__, via))
    # functools.update_wrapper stores `__wrapped__` in the instance dict.
    attrs = getattr(obj, "__dict__", None)
    if isinstance(attrs, dict) and "__wrapped__" in attrs:
        following.append((attrs["__wrapped__"], via))
    if not has_code:
        # A C slot (as on functools.partial itself) has no code to follow.
        call = getattr(type(obj), "__call__", None)
        if call is not None and not isinstance(call, types.WrapperDescriptorType):
            following.append((call, via))
    return reached, following


def _code_testing_identity_with_none(fn: Callable) -> _ReachedCode | None:
    """The nearest code object that calling ``fn`` can run (see
    :func:`_called_code`) and that compares a value with ``None`` by
    identity, or ``None`` if there is none.

    Row and condition lambdas are both checked through this function."""
    return next(
        (reached for reached in _called_code(fn) if _tests_identity_with_none(reached.code)),
        None,
    )


def _describe_reached(reached: _ReachedCode) -> str:
    """``'name'``, followed for a helper by the functions that lead to it."""
    name = repr(reached.code.co_qualname)
    if not reached.via:
        return name
    caller, *through = reached.via
    route = f" via {' -> '.join(map(repr, through))}" if through else ""
    return f"{name} (called from {caller!r}{route})"


def _invoke_row_lambda(fn: Callable, schema: dict[str, str]) -> Any:
    """Run a user row lambda against a :class:`SchemaProxy` for ``schema``.

    Every public API that accepts ``lambda r: ...`` must call the lambda
    through this function rather than on a proxy of its own, so the
    ``is None`` / ``is not None`` rewrite applies uniformly. The raw result
    is returned; callers validate its shape (``Expr``, dict, list, ...).

    Raises:
        TypeError: If ``fn``, a function it wraps, or a user function either
            calls by name (see :func:`_called_code`) still compares a value
            with ``None`` by identity after the rewrite (source unavailable,
            a plain ``def``, a helper, ...). Such a check is a Python bool,
            not a null predicate, and ``and`` / ``or`` / conditional
            expressions would otherwise silently drop part of the captured
            expression. Helpers are only checked, never rewritten, so a
            helper's test on a Python value is refused too.
    """
    # `is` cannot be overloaded: rewrite `x is None` / `x is not None` in the
    # lambda into explicit null checks before running it against the proxy.
    fn = _transform_lambda_for_none_checks(fn)
    unrewritten = _code_testing_identity_with_none(fn)
    if unrewritten is not None:
        raise TypeError(
            f"{_describe_reached(unrewritten)} compares a value with None using 'is' / 'is not', "
            "which LTSeq could not rewrite into a null check. Unrewritten, "
            "the comparison is a Python bool rather than a column predicate "
            "and would silently change the captured expression.\n\n"
            f"{_NULL_METHOD_GUIDANCE}"
        )
    return fn(SchemaProxy(schema))


def _invoke_condition_lambda(
    fn: Callable, left_schema: dict[str, str], right_schema: dict[str, str]
) -> Any:
    """Run a two-table condition lambda (the ``on=`` of ``join``,
    ``semi_join``, ``anti_join``, ``asof_join`` and ``link``) against a
    :class:`SchemaProxy` for each table.

    Every public API that accepts ``lambda left, right: ...`` must call the
    lambda through this function rather than on proxies of its own. The raw
    result is returned; callers validate its shape.

    Unlike :func:`_invoke_row_lambda`, nothing is rewritten: a null test
    selects rows rather than pairing them, so it is never part of a join
    condition. The check reads bytecode, so it cannot tell a column from a
    Python value and rejects an ``is None`` test on a closure value too. It
    covers the helpers ``fn`` calls as row lambdas' does (see
    :func:`_called_code`): a helper named by a global or closure variable is
    checked, one reached through an attribute is not.

    Raises:
        TypeError: If ``fn``, a function it wraps, or a user function either
            calls by name (see :func:`_called_code`) compares a value with
            ``None`` by identity. Python evaluates such a test to a constant
            bool (a column is never ``None``), and ``and`` / ``or`` /
            conditional expressions would otherwise silently drop part of
            the condition.
    """
    reached = _code_testing_identity_with_none(fn)
    if reached is not None:
        raise TypeError(
            f"{_describe_reached(reached)} compares a value with None using 'is' / 'is not'. "
            "Join conditions support only comparisons between a column of each "
            "table: == (combined with &) for join, semi_join, anti_join and "
            "link, and one of >=, >, <=, < for asof_join. Python evaluates an "
            "identity test instead of capturing it, and a column is never None, "
            "so 'and', 'or' or a conditional expression would silently drop "
            "part of the condition.\n\n"
            "To keep or drop rows with nulls, filter a table before the join or "
            "the result after it:\n"
            "  - .filter(lambda r: r.col.is_not_null())  instead of  r.col is not None\n"
            "  - .filter(lambda r: r.col.is_null())      instead of  r.col is None\n"
            "Test Python values (such as an optional parameter) before building "
            "the condition, or against a sentinel object rather than None."
        )
    return fn(SchemaProxy(left_schema), SchemaProxy(right_schema))


def _none_check_hint(result: Any) -> str:
    """Guidance appended to a "lambda returned a non-Expr" error when the
    result looks like an un-rewritten ``is None`` check (a Python bool), for
    example one made in a helper that the lambda calls through an attribute,
    which :func:`_called_code` does not follow."""
    if result is True or result is False:
        return (
            "\n\nHint: a bool result often comes from an unrewritten "
            f"'is None' / 'is not None' check. {_NULL_METHOD_GUIDANCE}"
        )
    return ""


def _lambda_to_expr(fn: Callable, schema: dict[str, str]) -> dict[str, Any]:
    """
    Execute a lambda with a SchemaProxy to capture its expression tree.

    This is the core function that intercepts Python lambdas and converts them
    to serializable expression dicts without executing any Python logic.

    Args:
        fn: Lambda function, e.g., lambda r: r.age > 18 or lambda r: {"col": r.age}
        schema: Dict mapping column name -> type string

    Returns:
        Serialized expression dict, ready for Rust deserialization
        Or for dict lambdas: {"type": "Dict", "keys": [...], "values": [...]}

    Raises:
        TypeError: If lambda doesn't return an Expr or dict
        AttributeError: If lambda references a non-existent column

    Example:
        >>> schema = {"age": "int64", "name": "string"}
        >>> expr_dict = _lambda_to_expr(lambda r: r.age > 18, schema)
        >>> expr_dict["type"]
        'BinOp'
        >>> expr_dict["op"]
        'Gt'
        >>> # Or for dict returns:
        >>> expr_dict = _lambda_to_expr(lambda r: {"adult": r.age > 18}, schema)
        >>> expr_dict["type"]
        'Dict'
    """
    result = _invoke_row_lambda(fn, schema)

    if isinstance(result, dict):
        # Handle dict returns: {"col_name": Expr, "col_name2": Expr}
        keys = []
        values = []
        for key, value in result.items():
            if not isinstance(key, str):
                raise TypeError(f"Dict keys must be strings, got {type(key).__name__}")
            if not isinstance(value, Expr):
                raise TypeError(
                    f"Dict values must be Expr objects, got {type(value).__name__} for key '{key}'"
                )
            keys.append({"type": "Literal", "value": key, "dtype": "String"})
            values.append(value.serialize())

        return {"type": "Dict", "keys": keys, "values": values}
    elif isinstance(result, Expr):
        # Handle Expr returns: lambda r: r.age > 18
        return result.serialize()
    else:
        raise TypeError(
            f"Lambda must return an Expr or dict, got {type(result).__name__}. "
            f"Did you forget to use the 'r' parameter?{_none_check_hint(result)}"
        )
