"""Architecture guard: expression types come from DataFusion's coercion (#225, decision D-a).

`Expr::get_type` and `Expr::cast_to` read an expression's type as it is
before DataFusion coerces it, which is wrong for a CASE and for anything
containing one. Both are methods of the `ExprSchemable` trait, which
`datafusion::prelude` does not export, so code that never names the trait
cannot call them. Only the resolver, `src/transpiler/resolve.rs`, may.
(`ScalarValue::cast_to` and PyO3's `get_type()` are other methods and are
not affected.)

ltseq also computes no common or widened type of its own. DataFusion's
helpers that compute one (`comparison_coercion`, `type_union_resolution`,
`get_coerce_type_for_*`, ...) are not called. Two `type_coercion` items are
allowed:
- `TypeCoercionRewriter`, the analyzer's expression coercion, which the
  resolver applies;
- `BinaryTypeCoercer`, the analyzer's rule for one binary pair, which literal
  placement, `dt.diff` and the linear-scan eligibility check ask what
  DataFusion does with a pair before the expression exists.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src"
RESOLVER = SRC / "transpiler" / "resolve.rs"

ALLOWED_TYPE_COERCION = {
    "BinaryTypeCoercer": None,  # anywhere
    "TypeCoercionRewriter": RESOLVER,
}

# Globs that would bring `ExprSchemable` into scope without naming it.
TRAIT_GLOBS = re.compile(r"\b(?:datafusion::logical_expr|datafusion_expr)(?:::expr_schema)?::\*")
TYPE_COERCION_PATH = re.compile(r"\btype_coercion::(?:\w+::)*\{?([\w, ]+)\}?")
COERCION_HELPER_CALL = re.compile(
    r"\b(type_union_resolution|get_coerce_type_for_\w+|\w+_coercion)\s*\("
)


def _code_lines(path: Path):
    """(line number, line without its `//` comment) for each line of `path`."""
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        yield number, line.split("//", 1)[0]


def _violations() -> list[str]:
    found = []
    for path in sorted(SRC.rglob("*.rs")):
        where = path.relative_to(REPO_ROOT)
        for number, code in _code_lines(path):
            if "ExprSchemable" in code and path != RESOLVER:
                found.append(f"{where}:{number}: ExprSchemable outside the resolver: {code.strip()}")
            if TRAIT_GLOBS.search(code):
                found.append(f"{where}:{number}: a glob import that brings ExprSchemable: {code.strip()}")
            for match in TYPE_COERCION_PATH.finditer(code):
                for item in (name.strip() for name in match.group(1).split(",")):
                    if not item:
                        continue
                    if item not in ALLOWED_TYPE_COERCION:
                        found.append(f"{where}:{number}: type_coercion::{item} is not allowed")
                    elif ALLOWED_TYPE_COERCION[item] not in (None, path):
                        found.append(f"{where}:{number}: type_coercion::{item} belongs to the resolver")
            for match in COERCION_HELPER_CALL.finditer(code):
                found.append(f"{where}:{number}: a common-type helper is called: {match.group(1)}")
    return found


def test_only_the_resolver_asks_expressions_for_their_types():
    # The guard looks at real code: the resolver itself uses both.
    resolver = RESOLVER.read_text()
    assert "ExprSchemable" in resolver and "TypeCoercionRewriter" in resolver
    assert _violations() == []
