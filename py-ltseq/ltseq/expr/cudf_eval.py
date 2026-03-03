"""Evaluate serialized expression dicts against a cuDF DataFrame (GPU-side).

Reuses the same serialized dict format produced by Expr.serialize() / SchemaProxy,
so no changes to the expression system are needed. All operations stay on the GPU.

Main entry points:
    eval_expr_as_mask(gdf, expr_dict)   -> cuDF boolean Series (for filter)
    eval_expr_as_series(gdf, expr_dict) -> cuDF Series (for derive / select)
    is_simple_cmp(expr_dict)            -> bool (can use Rust CUDA kernel path?)
"""

from __future__ import annotations

import operator as _op
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass  # avoid circular import; cudf imported lazily

# Binary operators: expr_dict["op"] -> Python operator function
# These match the capitalized names produced by Expr.serialize()
_BINOPS: dict[str, Any] = {
    "Gt":       _op.gt,
    "Lt":       _op.lt,
    "Ge":       _op.ge,
    "Le":       _op.le,
    "Eq":       _op.eq,
    "Ne":       _op.ne,
    "Add":      _op.add,
    "Sub":      _op.sub,
    "Mul":      _op.mul,
    "Div":      _op.truediv,
    "FloorDiv": _op.floordiv,
    "Mod":      _op.mod,
    "And":      _op.and_,
    "Or":       _op.or_,
}

# Comparison operators eligible for the Rust CUDA kernel path
_CMP_OPS: frozenset[str] = frozenset({"Gt", "Lt", "Ge", "Le", "Eq", "Ne"})

# Dtypes that map directly to our CUDA kernel naming convention
_CUDF_TO_KERNEL_DTYPE: dict[str, str] = {
    "int8":    "i32",
    "int16":   "i32",
    "int32":   "i32",
    "int64":   "i64",
    "uint8":   "i32",
    "uint16":  "i32",
    "uint32":  "i64",
    "uint64":  "i64",
    "float32": "f32",
    "float64": "f64",
}

# Operators that map to our CUDA kernel naming convention
_OP_TO_KERNEL_OP: dict[str, str] = {
    "Gt": "gt",
    "Lt": "lt",
    "Ge": "gte",
    "Le": "lte",
    "Eq": "eq",
    "Ne": "neq",
}


def is_simple_cmp(expr_dict: dict) -> bool:
    """Return True if expr_dict is a simple `Column OP Literal` comparison.

    Simple comparisons can be executed via the Rust CUDA kernel path (no H2D),
    which is faster than going through cuDF's expression engine for very large
    columns.

    Supported: Column CMP Literal and Literal CMP Column (both normalized).
    """
    if expr_dict.get("type") != "BinOp":
        return False
    op = expr_dict.get("op", "")
    if op not in _CMP_OPS:
        return False
    left, right = expr_dict.get("left", {}), expr_dict.get("right", {})
    # Column CMP Literal
    if left.get("type") == "Column" and right.get("type") == "Literal":
        return True
    # Literal CMP Column (normalized to Column CMP Literal by caller)
    if left.get("type") == "Literal" and right.get("type") == "Column":
        return True
    return False


def extract_simple_cmp(expr_dict: dict) -> tuple[str, str, str, float]:
    """Extract (col_name, kernel_op, col_name_for_dtype_lookup, threshold) from a
    simple comparison dict.  The dtype is resolved against the actual cuDF column.

    Returns (col_name, kernel_op, col_name_for_dtype_lookup, threshold_float).
    Normalizes `Literal OP Column` to `Column OP' Literal`.
    """
    _FLIP = {"gt": "lt", "lt": "gt", "gte": "lte", "lte": "gte", "eq": "eq", "neq": "neq"}
    op = _OP_TO_KERNEL_OP[expr_dict["op"]]
    left, right = expr_dict["left"], expr_dict["right"]

    if left["type"] == "Column" and right["type"] == "Literal":
        return left["name"], op, left["name"], float(right["value"])
    else:
        # Literal OP Column → flip operator
        return right["name"], _FLIP[op], right["name"], float(left["value"])


def eval_expr_as_series(gdf, expr_dict: dict):
    """Evaluate expr_dict against cuDF DataFrame `gdf`, returning a cuDF Series.

    All computation stays on the GPU — no D2H transfer occurs.
    Supports: Column, Literal, BinOp, UnaryOp(Not/-/Neg), CallExpr.
    """
    import cudf  # lazy import

    t = expr_dict.get("type")

    if t == "Column":
        return gdf[expr_dict["name"]]

    elif t == "Literal":
        # Return a Python scalar; cuDF broadcasts it automatically in BinOp
        return expr_dict["value"]

    elif t == "BinOp":
        left  = eval_expr_as_series(gdf, expr_dict["left"])
        right = eval_expr_as_series(gdf, expr_dict["right"])
        fn = _BINOPS.get(expr_dict["op"])
        if fn is None:
            raise NotImplementedError(f"cuDF eval: unsupported BinOp '{expr_dict['op']}'")
        return fn(left, right)

    elif t == "UnaryOp":
        operand = eval_expr_as_series(gdf, expr_dict["operand"])
        uop = expr_dict.get("op")
        if uop == "Not":
            return ~operand
        elif uop in ("Neg", "Minus"):
            return -operand
        else:
            raise NotImplementedError(f"cuDF eval: unsupported UnaryOp '{uop}'")

    elif t == "Call":
        return _eval_call(gdf, expr_dict)

    elif t == "IfElse":
        cond  = eval_expr_as_series(gdf, expr_dict["condition"])
        true_ = eval_expr_as_series(gdf, expr_dict["true_value"])
        false_= eval_expr_as_series(gdf, expr_dict["false_value"])
        return cudf.Series(true_).where(cond, false_)

    else:
        raise NotImplementedError(f"cuDF eval: unsupported expression type '{t}'")


def eval_expr_as_mask(gdf, expr_dict: dict):
    """Evaluate expr_dict as a boolean mask Series for use in `gdf[mask]`.

    Returns a cuDF boolean Series (GPU-resident).
    """
    result = eval_expr_as_series(gdf, expr_dict)
    # If result is a Python scalar bool, broadcast to a Series
    if isinstance(result, bool):
        import cudf
        return cudf.Series([result] * len(gdf))
    return result


# ────────────────────────────────────────────────────────────
# Function call evaluation
# ────────────────────────────────────────────────────────────

def _eval_call(gdf, expr_dict: dict):
    """Evaluate a CallExpr dict against cuDF DataFrame."""
    fn_name = expr_dict.get("func") or expr_dict.get("name", "")
    args = [eval_expr_as_series(gdf, a) for a in expr_dict.get("args", [])]

    # ── Math ────────────────────────────────────────────────
    if fn_name == "abs":
        return args[0].abs()
    elif fn_name == "sqrt":
        return args[0] ** 0.5
    elif fn_name == "power":
        return args[0] ** args[1]
    elif fn_name == "sign":
        return args[0].sign()
    elif fn_name == "log":
        import cupy as cp
        return cp.log(args[0].values) if len(args) == 1 else cp.log(args[0].values) / cp.log(args[1])
    elif fn_name == "ln":
        import cupy as cp
        return cp.log(args[0].values)
    elif fn_name == "exp":
        import cupy as cp
        return cp.exp(args[0].values)
    elif fn_name in ("sin", "cos", "tan", "asin", "acos", "atan"):
        import cupy as cp
        return getattr(cp, fn_name)(args[0].values)
    elif fn_name == "atan2":
        import cupy as cp
        return cp.arctan2(args[0].values, args[1].values)

    # ── String ──────────────────────────────────────────────
    elif fn_name == "lower":
        return args[0].str.lower()
    elif fn_name == "upper":
        return args[0].str.upper()
    elif fn_name == "len" or fn_name == "length":
        return args[0].str.len()
    elif fn_name == "trim":
        return args[0].str.strip()
    elif fn_name == "ltrim":
        return args[0].str.lstrip()
    elif fn_name == "rtrim":
        return args[0].str.rstrip()
    elif fn_name == "contains":
        pattern = expr_dict["args"][1]["value"]
        return args[0].str.contains(pattern)
    elif fn_name == "starts_with":
        pattern = expr_dict["args"][1]["value"]
        return args[0].str.startswith(pattern)
    elif fn_name == "ends_with":
        pattern = expr_dict["args"][1]["value"]
        return args[0].str.endswith(pattern)

    # ── Null / Conditional ───────────────────────────────────
    elif fn_name == "coalesce":
        result = args[0]
        for a in args[1:]:
            result = result.fillna(a)
        return result
    elif fn_name in ("nvl", "ifnull"):
        return args[0].fillna(args[1])
    elif fn_name == "if_else":
        import cudf
        cond   = eval_expr_as_series(gdf, expr_dict["args"][0])
        true_  = eval_expr_as_series(gdf, expr_dict["args"][1])
        false_ = eval_expr_as_series(gdf, expr_dict["args"][2])
        return cudf.Series(true_).where(cond, false_)

    else:
        raise NotImplementedError(
            f"cuDF eval: function '{fn_name}' is not yet implemented for GPU path. "
            f"Call .to_ltseq() first to use DataFusion for complex expressions."
        )
