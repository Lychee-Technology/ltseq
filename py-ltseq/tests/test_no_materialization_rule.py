"""Architecture guard for table-returning query APIs."""

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

FILES_TO_CHECK = {
    "py-ltseq/ltseq/advanced_ops.py": {
        "AdvancedOpsMixin": {"align", "search_first", "pivot"},
        "SetOpsMixin": {"union", "intersect", "except_", "xunion", "rvs", "step"},
    },
    "py-ltseq/ltseq/joins.py": {
        "JoinMixin": {"_execute_join", "join", "_join_with_sort_validation", "asof_join", "semi_join", "anti_join"},
    },
    "py-ltseq/ltseq/mutation_mixin.py": {
        "MutationMixin": {"insert", "delete", "update", "modify"},
    },
    "py-ltseq/ltseq/partitioning.py": {
        "SQLPartitionedTable": {"__getitem__", "map"},
        "PartitionedTable": {"map"},
    },
    "py-ltseq/ltseq/grouping/nested_table.py": {
        "NestedTable": {"first", "last", "flatten", "filter", "_filter_via_sql", "derive", "_derive_via_sql"},
    },
    "py-ltseq/ltseq/transforms.py": {
        "TransformMixin": {"filter", "select", "derive", "rename", "sort", "distinct", "slice"},
    },
    "py-ltseq/ltseq/aggregation.py": {
        "GroupBy": {"agg"},
        "AggregationMixin": {"cum_sum", "group_ordered", "group_sorted", "agg"},
    },
    "py-ltseq/ltseq/linking.py": {
        "LinkedTable": {"_materialize", "filter", "select", "derive", "sort", "slice", "distinct", "link"},
    },
}

FORBIDDEN_INSTANCE_CALLS = {"to_pandas", "to_arrow", "_from_rows"}
FORBIDDEN_LTSEQ_CLASS_CALLS = {"from_arrow", "from_pandas", "_from_rows"}


class ForbiddenMaterializationVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.matches: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call) -> None:
        target = node.func

        if isinstance(target, ast.Attribute):
            if target.attr in FORBIDDEN_INSTANCE_CALLS:
                self.matches.append((node.lineno, f".{target.attr}()"))
            elif (
                isinstance(target.value, ast.Name)
                and target.value.id == "LTSeq"
                and target.attr in FORBIDDEN_LTSEQ_CLASS_CALLS
            ):
                self.matches.append((node.lineno, f"LTSeq.{target.attr}()"))

        self.generic_visit(node)


def _function_node(module: ast.Module, class_name: str, function_name: str) -> ast.FunctionDef:
    for node in module.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for inner in node.body:
                if isinstance(inner, ast.FunctionDef) and inner.name == function_name:
                    return inner
    raise AssertionError(f"Could not find {class_name}.{function_name}")


def test_table_returning_query_apis_do_not_materialize() -> None:
    violations: list[str] = []

    for rel_path, classes in FILES_TO_CHECK.items():
        source_path = REPO_ROOT / rel_path
        module = ast.parse(source_path.read_text())

        for class_name, function_names in classes.items():
            for function_name in sorted(function_names):
                fn = _function_node(module, class_name, function_name)
                visitor = ForbiddenMaterializationVisitor()
                visitor.visit(fn)

                for lineno, call in visitor.matches:
                    violations.append(
                        f"{rel_path}:{lineno} {class_name}.{function_name} uses forbidden materialization via {call}"
                    )

    assert not violations, "\n".join(violations)
