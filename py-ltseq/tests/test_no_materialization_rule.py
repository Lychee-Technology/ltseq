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
        "NestedTable": {"first", "last", "flatten", "filter", "derive"},
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


# =============================================================================
# Rust-side guard (issue #91): table-returning ops must not round-trip through
# "collect → MemTable → session.sql() → collect".
#
# Function-level allowlist per the issue's matrix. Modules still on the SQL
# path are strict-xfail: when a migration PR lands, its xpass forces the
# marker's removal, turning the guard into the module's acceptance lock.
# =============================================================================

import re

import pytest

# Tokens that indicate a SQL round-trip / temp-table materialization.
RUST_ROUNDTRIP_TOKENS = (
    "session.sql",
    ".sql(&",
    "execute_sql_query(",
    "MemTable::try_new",
    "register_table(",
)

# Matches only column-0 `fn` declarations. Assumption (holds for all scanned
# ops modules): helpers are top-level fns, not methods in `impl` blocks.
# Anything between two top-level fns — including an indented `impl` block —
# is attributed to the PRECEDING fn's segment, so its tokens are still
# scanned (just reported under that fn's name); text before the first fn is
# scanned as "<module-prelude>". A token can therefore never hide entirely,
# but if an impl block ever follows an allowlisted fn, move it or split the
# allowlist entry.
_RUST_FN_RE = re.compile(r"^(?:pub(?:\(crate\))? )?(?:async )?fn (\w+)", re.M)


def _rust_functions(source: str) -> list[tuple[str, str]]:
    """Split a Rust source file into (name, segment) chunks covering the file."""
    matches = list(_RUST_FN_RE.finditer(source))
    out = []
    if matches and matches[0].start() > 0:
        out.append(("<module-prelude>", source[: matches[0].start()]))
    elif not matches:
        out.append(("<module-prelude>", source))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(source)
        out.append((m.group(1), source[m.start():end]))
    return out


_RUST_LINE_COMMENT_RE = re.compile(r"//[^\n]*")


def _scan_rust_file(rel_path: str, allowed_fns: set) -> list[str]:
    # Strip line comments so documentation may mention the forbidden tokens.
    source = _RUST_LINE_COMMENT_RE.sub("", (REPO_ROOT / rel_path).read_text())
    violations = []
    for fn_name, body in _rust_functions(source):
        if fn_name in allowed_fns:
            continue
        for token in RUST_ROUNDTRIP_TOKENS:
            if token in body:
                violations.append(f"{rel_path}: fn {fn_name} uses {token}")
    return violations


RUST_GUARD_PARAMS = [
    # (file, allowlisted fns, xfail reason or None)
    pytest.param(
        "src/ops/aggregation.rs",
        # filter_where uses session.sql as a parser-as-library (empty table,
        # no data round-trip) — explicitly allowlisted by issue #91.
        {"filter_where_impl"},
        id="aggregation",
    ),
    pytest.param(
        "src/ops/window.rs",
        set(),
        id="window",
    ),
    pytest.param(
        "src/ops/grouping.rs",
        set(),
        id="grouping",
    ),
    pytest.param(
        "src/ops/group_window.rs",
        set(),
        id="group_window",
    ),
    pytest.param(
        "src/ops/align.rs",
        set(),
        marks=pytest.mark.xfail(reason="pending PR 5 of #91 (native align join)", strict=True),
        id="align",
    ),
    pytest.param(
        "src/ops/set_ops.rs",
        set(),
        marks=pytest.mark.xfail(reason="pending PR 6 of #91 (native set ops)", strict=True),
        id="set_ops",
    ),
    pytest.param(
        "src/ops/common.rs",
        set(),
        marks=pytest.mark.xfail(reason="pending PR 6/7 of #91 (SQL helper removal)", strict=True),
        id="common",
    ),
]


@pytest.mark.parametrize("rel_path,allowed_fns", RUST_GUARD_PARAMS)
def test_rust_table_ops_do_not_sql_roundtrip(rel_path: str, allowed_fns: set) -> None:
    violations = _scan_rust_file(rel_path, allowed_fns)
    assert not violations, "\n".join(violations)
