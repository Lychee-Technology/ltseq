"""AST transformation and lambda expression capturing for LTSeq."""

import __future__
import ast
import copy
import functools
import inspect
import operator
import types
from typing import Any, Callable, NamedTuple, cast

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
    preserved). When the source cannot be inspected, ``fn`` is returned
    unchanged and :func:`_lambda_to_expr` reports how to spell the check.
    """
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
    proxy = SchemaProxy(schema)

    # `is` cannot be overloaded: rewrite `x is None` / `x is not None` in the
    # lambda into explicit null checks before running it against the proxy.
    fn = _transform_lambda_for_none_checks(fn)

    result = fn(proxy)

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
        # Check if this might be an 'is None' / 'is not None' issue
        hint = ""
        if result is True or result is False:
            hint = (
                "\n\nHint: If you're using 'is None' or 'is not None', use the "
                "is_null() or is_not_null() methods instead:\n"
                "  - r.col.is_null()      instead of  r.col is None\n"
                "  - r.col.is_not_null()  instead of  r.col is not None\n"
                "LTSeq rewrites 'is None' checks inside lambdas automatically "
                "when their source is available; the methods are required in "
                "a REPL or exec/eval string and in plain def functions."
            )
        raise TypeError(
            f"Lambda must return an Expr or dict, got {type(result).__name__}. "
            f"Did you forget to use the 'r' parameter?{hint}"
        )
