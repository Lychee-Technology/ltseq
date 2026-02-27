"""SQL parsing utilities for converting Python lambdas to SQL expressions."""

import ast
from typing import Dict, Optional


def extract_lambda_from_chain(source: str) -> str:
    """
    Extract just the lambda from a source that might include method chains.

    When inspect.getsource() is called on a lambda in a chained method call like
    .filter(...).derive(lambda g: {...}), it returns the whole chain.
    This extracts just the lambda part.

    Example:
        Input: '.derive(lambda g: {"total": g.sum("val")})'
        Output: 'lambda g: {"total": g.sum("val")}'
    """
    # Find where 'lambda' starts in the source
    lambda_pos = source.find("lambda")
    if lambda_pos == -1:
        return source

    # Extract from 'lambda' onwards
    lambda_source = source[lambda_pos:].strip()

    # Try to find the end of the lambda by parsing
    for end_idx in range(len(lambda_source), 0, -1):
        candidate = lambda_source[:end_idx]
        try:
            ast.parse(candidate)
            return candidate
        except SyntaxError:
            if candidate and candidate[-1] in ")]}":
                continue

    # If we can't find a valid lambda, remove trailing chars
    while lambda_source and lambda_source[-1] in "),]}":
        lambda_source = lambda_source[:-1]

    return lambda_source.strip()


def ast_op_to_sql(op) -> Optional[str]:
    """Convert AST operator to SQL operator."""
    op_map = {
        ast.Gt: ">",
        ast.GtE: ">=",
        ast.Lt: "<",
        ast.LtE: "<=",
        ast.Eq: "=",
        ast.NotEq: "!=",
    }
    return op_map.get(type(op))


def ast_binop_to_sql(op) -> Optional[str]:
    """Convert AST BinOp to SQL operator."""
    if isinstance(op, ast.Add):
        return "+"
    elif isinstance(op, ast.Sub):
        return "-"
    elif isinstance(op, ast.Mult):
        return "*"
    elif isinstance(op, ast.Div):
        return "/"
    elif isinstance(op, ast.FloorDiv):
        return "/"
    elif isinstance(op, ast.Mod):
        return "%"
    return ""


class FilterSQLParser:
    """Parser for converting filter predicates to SQL WHERE clauses."""

    def try_parse_filter_to_sql(self, group_predicate) -> Optional[str]:
        """
        Attempt to parse a group filter predicate to SQL with window functions.

        Returns SQL WHERE clause string if successful, None if pattern not supported.
        """
        import inspect

        try:
            source = inspect.getsource(group_predicate).strip()
            if "lambda" in source:
                source = source.split(":", 1)[1].strip()

            expr = ast.parse(source, mode="eval").body
            sql = self._ast_to_sql(expr)
            return sql if sql else None
        except Exception:
            return None

    def _ast_to_sql(self, node) -> Optional[str]:
        """Convert an AST node to SQL WHERE clause."""
        if isinstance(node, ast.Compare):
            left_sql = self._ast_to_sql(node.left)
            if not left_sql:
                return None

            for op, comp in zip(node.ops, node.comparators):
                comp_sql = self._ast_to_sql(comp)
                if not comp_sql:
                    return None

                op_str = ast_op_to_sql(op)
                if not op_str:
                    return None

                left_sql = f"{left_sql} {op_str} {comp_sql}"

            return left_sql

        elif isinstance(node, ast.BoolOp):
            op_str = " AND " if isinstance(node.op, ast.And) else " OR "
            values = []
            for v in node.values:
                v_sql = self._ast_to_sql(v)
                if not v_sql:
                    return None
                values.append(f"({v_sql})")

            return op_str.join(values)

        elif isinstance(node, ast.Call):
            return self._ast_call_to_sql(node)

        elif isinstance(node, ast.Attribute):
            return self._ast_attribute_to_sql(node)

        elif isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return f"'{node.value}'"
            elif isinstance(node.value, bool):
                return "TRUE" if node.value else "FALSE"
            elif node.value is None:
                return "NULL"
            else:
                return str(node.value)

        return None

    def _ast_call_to_sql(self, node) -> Optional[str]:
        """Convert AST function call to SQL window function."""
        if not (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "g"
        ):
            return None

        method_name = node.func.attr

        if method_name == "count":
            return "COUNT(*) OVER (PARTITION BY __group_id__)"

        elif method_name in ("first", "last"):
            return None

        elif method_name in ("max", "min", "sum", "avg"):
            if len(node.args) != 1:
                return None

            col_arg = node.args[0]
            if isinstance(col_arg, ast.Constant):
                col_name = col_arg.value
            else:
                return None

            func_upper = method_name.upper()
            return f"{func_upper}({col_name}) OVER (PARTITION BY __group_id__)"

        elif method_name in ("all", "any", "none"):
            if len(node.args) != 1:
                return None

            inner_predicate = node.args[0]
            if not isinstance(inner_predicate, ast.Lambda):
                return None

            inner_sql = self._ast_inner_predicate_to_sql(inner_predicate.body)
            if not inner_sql:
                return None

            case_expr = f"CASE WHEN {inner_sql} THEN 1 ELSE 0 END"

            if method_name == "all":
                return f"MIN({case_expr}) OVER (PARTITION BY __group_id__) = 1"
            elif method_name == "any":
                return f"MAX({case_expr}) OVER (PARTITION BY __group_id__) = 1"
            else:  # none
                return f"MAX({case_expr}) OVER (PARTITION BY __group_id__) = 0"

        return None

    def _ast_inner_predicate_to_sql(self, node) -> Optional[str]:
        """Convert an inner predicate (r.col op val) to SQL condition."""
        if isinstance(node, ast.Compare):
            left_sql = self._ast_row_expr_to_sql(node.left)
            if not left_sql:
                return None

            for op, comp in zip(node.ops, node.comparators):
                op_str = ast_op_to_sql(op)
                if not op_str:
                    return None

                if isinstance(comp, ast.Constant):
                    if isinstance(comp.value, str):
                        right_sql = f"'{comp.value}'"
                    elif isinstance(comp.value, bool):
                        right_sql = "TRUE" if comp.value else "FALSE"
                    elif comp.value is None:
                        right_sql = "NULL"
                    else:
                        right_sql = str(comp.value)
                elif isinstance(comp, ast.Attribute):
                    right_sql = self._ast_row_expr_to_sql(comp)
                    if not right_sql:
                        return None
                else:
                    return None

                left_sql = f"{left_sql} {op_str} {right_sql}"

            return left_sql

        elif isinstance(node, ast.BoolOp):
            op_str = " AND " if isinstance(node.op, ast.And) else " OR "
            values = []
            for v in node.values:
                v_sql = self._ast_inner_predicate_to_sql(v)
                if not v_sql:
                    return None
                values.append(f"({v_sql})")
            return op_str.join(values)

        elif isinstance(node, ast.Call):
            return self._ast_row_method_to_sql(node)

        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            inner = self._ast_inner_predicate_to_sql(node.operand)
            if inner:
                return f"NOT ({inner})"
            return None

        return None

    def _ast_row_expr_to_sql(self, node) -> Optional[str]:
        """Convert r.col to column name."""
        if isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "r":
                return node.attr
        return None

    def _ast_row_method_to_sql(self, node) -> Optional[str]:
        """Convert r.col.is_null() or similar method calls to SQL."""
        if not isinstance(node.func, ast.Attribute):
            return None

        method_name = node.func.attr

        if method_name == "is_null" and isinstance(node.func.value, ast.Attribute):
            col_name = self._ast_row_expr_to_sql(node.func.value)
            if col_name:
                return f"{col_name} IS NULL"

        if method_name == "is_not_null" and isinstance(node.func.value, ast.Attribute):
            col_name = self._ast_row_expr_to_sql(node.func.value)
            if col_name:
                return f"{col_name} IS NOT NULL"

        return None

    def _ast_attribute_to_sql(self, node) -> Optional[str]:
        """Convert AST attribute access to SQL."""
        if isinstance(node.value, ast.Call):
            call_node = node.value
            if (
                isinstance(call_node.func, ast.Attribute)
                and isinstance(call_node.func.value, ast.Name)
                and call_node.func.value.id == "g"
            ):
                method_name = call_node.func.attr
                col_name = node.attr

                if method_name == "first":
                    return f"FIRST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__)"
                elif method_name == "last":
                    return f"LAST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"

        return None


class DeriveSQLParser:
    """Parser for converting derive expressions to SQL window functions."""

    def extract_derive_expressions(self, ast_tree, schema: Dict) -> Dict[str, str]:
        """
        Extract derive expressions from lambda g: {dict} AST.

        Returns dict mapping column_name -> SQL expression for window functions.
        """
        dict_node = None
        for node in ast.walk(ast_tree):
            if isinstance(node, ast.Lambda):
                if isinstance(node.body, ast.Dict):
                    dict_node = node.body
                    break

        if not dict_node:
            return {}

        result = {}

        for key_node, value_node in zip(dict_node.keys, dict_node.values):
            if not isinstance(key_node, ast.Constant) or not isinstance(
                key_node.value, str
            ):
                return {}

            col_name = key_node.value
            sql_expr = self._process_derive_expr(value_node, schema)
            if not sql_expr:
                return {}

            result[col_name] = sql_expr

        return result

    def _process_derive_expr(self, expr, schema: Dict) -> str:
        """Process a derive expression and return SQL window function."""
        if isinstance(expr, ast.Call):
            return self._process_derive_call(expr, schema)
        elif isinstance(expr, ast.BinOp):
            return self._process_derive_binop(expr, schema)
        elif isinstance(expr, ast.Attribute):
            return self._process_derive_attribute(expr, schema)
        return ""

    def _process_derive_call(self, call_node, schema: Dict) -> str:
        """Process function calls like g.count(), g.max('col'), etc."""
        if not isinstance(call_node.func, ast.Attribute):
            return ""

        method_name = call_node.func.attr

        if method_name == "count" and len(call_node.args) == 0:
            return "COUNT(*) OVER (PARTITION BY __group_id__)"

        if method_name in ("max", "min", "sum", "avg"):
            if len(call_node.args) == 1 and isinstance(call_node.args[0], ast.Constant):
                col_name = call_node.args[0].value
                if isinstance(col_name, str):
                    agg_func = method_name.upper()
                    return f"{agg_func}({col_name}) OVER (PARTITION BY __group_id__)"

        if method_name in ("first", "last") and len(call_node.args) == 0:
            return ""

        return ""

    def _process_derive_attribute(self, attr_node, schema: Dict) -> str:
        """Process attribute access chains like g.first().col or g.last().col."""
        if not isinstance(attr_node.value, ast.Call):
            return ""

        inner_call = attr_node.value
        col_name = attr_node.attr

        if not isinstance(inner_call.func, ast.Attribute):
            return ""

        method_name = inner_call.func.attr

        if method_name == "first" and len(inner_call.args) == 0:
            return f"FIRST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__)"
        elif method_name == "last" and len(inner_call.args) == 0:
            return f"LAST_VALUE({col_name}) OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"

        return ""

    def _process_derive_binop(self, binop_node, schema: Dict) -> str:
        """Process binary operations like (expr1) - (expr2), (expr1) / (expr2)."""
        op_str = ast_binop_to_sql(binop_node.op)
        if not op_str:
            return ""

        left_sql = self._process_derive_expr(binop_node.left, schema)
        right_sql = self._process_derive_expr(binop_node.right, schema)

        if not left_sql or not right_sql:
            return ""

        return f"({left_sql} {op_str} {right_sql})"


def get_unsupported_derive_error(source: str) -> str:
    """Generate error for unsupported derive patterns."""
    return (
        f"Unsupported derive pattern: {source.strip()}\n"
        "Supported: g.count(), g.first().col, g.last().col, g.max/min/sum/avg('col'), "
        "arithmetic combinations like (g.last().price - g.first().price)"
    )


def get_derive_parse_error_message(source: str, error: str) -> str:
    """Generate error for syntax/parse errors in derive lambda."""
    src = source.strip() if source else "<unavailable>"
    return f"Failed to parse derive: {src}\nError: {error}"


def group_expr_to_sql(expr: Dict) -> str:
    """
    Convert a serialized GroupExpr to SQL window function.

    Args:
        expr: Serialized GroupExpr dictionary with 'type' key

    Returns:
        SQL window function string

    Raises:
        ValueError: If expression type is not supported
    """
    expr_type = expr.get("type")

    if expr_type == "GroupCount":
        return "COUNT(*) OVER (PARTITION BY __group_id__)"

    elif expr_type == "GroupAgg":
        func = expr["func"].upper()
        col = expr["column"]
        return f'{func}("{col}") OVER (PARTITION BY __group_id__)'

    elif expr_type == "GroupRowColumn":
        row = expr["row"]  # "first" or "last"
        col = expr["column"]
        if row == "first":
            return (
                f'FIRST_VALUE("{col}") OVER (PARTITION BY __group_id__ ORDER BY __rn__)'
            )
        else:  # last
            return f'LAST_VALUE("{col}") OVER (PARTITION BY __group_id__ ORDER BY __rn__ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'

    elif expr_type == "BinOp":
        left = group_expr_to_sql(expr["left"])
        right = group_expr_to_sql(expr["right"])
        op = expr["op"]
        return f"({left} {op} {right})"

    elif expr_type == "Literal":
        value = expr["value"]
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
        else:
            return str(value)

    else:
        raise ValueError(f"Unsupported GroupExpr type: {expr_type}")
