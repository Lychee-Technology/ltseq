//! Expression optimization: Constant folding and boolean simplification
//!
//! This module provides compile-time optimizations for PyExpr trees:
//! - **Constant Folding**: Arithmetic operations on literals are evaluated at compile time
//!   (e.g., `1 + 2 + r.col` → `3 + r.col`)
//! - **Boolean Simplification**: Trivial boolean expressions are simplified
//!   (e.g., `x & True` → `x`, `x | False` → `x`)

use crate::types::{LiteralValue, PyExpr};

/// A numeric literal operand: the two literal kinds that fold.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Num {
    Int(i64),
    Float(f64),
}

impl Num {
    /// The operand as DataFusion sees it once an Int64 is coerced to Float64.
    fn as_f64(self) -> f64 {
        match self {
            Num::Int(v) => v as f64,
            Num::Float(v) => v,
        }
    }
}

fn numeric_literal(expr: &PyExpr) -> Option<Num> {
    match expr {
        PyExpr::Literal(LiteralValue::Int64(v)) => Some(Num::Int(*v)),
        PyExpr::Literal(LiteralValue::Float64(v)) => Some(Num::Float(*v)),
        _ => None,
    }
}

/// Extract boolean value from a literal PyExpr
fn get_literal_bool(expr: &PyExpr) -> Option<bool> {
    match expr {
        PyExpr::Literal(LiteralValue::Boolean(v)) => Some(*v),
        _ => None,
    }
}

fn make_literal_bool(value: bool) -> PyExpr {
    PyExpr::Literal(LiteralValue::Boolean(value))
}

/// `l op r` for two numeric literals, with the type and value DataFusion
/// would compute: two Int64 stay Int64 (`/` and `%` truncate), anything with
/// a Float64 is Float64. `None` leaves the operation to DataFusion: integer
/// overflow (an error there), a zero divisor, an operator that is not
/// arithmetic.
fn fold_arithmetic(op: &str, l: Num, r: Num) -> Option<LiteralValue> {
    if let (Num::Int(l), Num::Int(r)) = (l, r) {
        let value = match op {
            "Add" => l.checked_add(r),
            "Sub" => l.checked_sub(r),
            "Mul" => l.checked_mul(r),
            "Div" => l.checked_div(r),
            "Mod" => l.checked_rem(r),
            _ => None,
        }?;
        return Some(LiteralValue::Int64(value));
    }
    let (l, r) = (l.as_f64(), r.as_f64());
    let value = match op {
        "Add" => l + r,
        "Sub" => l - r,
        "Mul" => l * r,
        "Div" if r != 0.0 => l / r,
        "Mod" if r != 0.0 => l % r,
        _ => return None,
    };
    Some(LiteralValue::Float64(value))
}

/// `l op r` for a comparison of two numeric literals: exact for two Int64,
/// in f64 otherwise. A NaN operand is left to DataFusion, which orders NaN
/// above every number and equal to itself.
fn fold_comparison(op: &str, l: Num, r: Num) -> Option<bool> {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let ordering = match (l, r) {
        (Num::Int(l), Num::Int(r)) => l.cmp(&r),
        _ => l.as_f64().partial_cmp(&r.as_f64())?,
    };
    Some(match op {
        "Eq" => ordering == Equal,
        "Ne" => ordering != Equal,
        "Lt" => ordering == Less,
        "Le" => ordering != Greater,
        "Gt" => ordering == Greater,
        "Ge" => ordering != Less,
        _ => return None,
    })
}

/// Try to fold a binary operation on two literals into a single literal
fn try_fold_binop(op: &str, left: &PyExpr, right: &PyExpr) -> Option<PyExpr> {
    if let (Some(l), Some(r)) = (numeric_literal(left), numeric_literal(right)) {
        if let Some(value) = fold_arithmetic(op, l, r) {
            return Some(PyExpr::Literal(value));
        }
        return fold_comparison(op, l, r).map(make_literal_bool);
    }

    // Try boolean folding
    if let (Some(l), Some(r)) = (get_literal_bool(left), get_literal_bool(right)) {
        let result = match op {
            "And" => Some(l && r),
            "Or" => Some(l || r),
            _ => None,
        };
        if let Some(b) = result {
            return Some(make_literal_bool(b));
        }
    }

    None
}

/// Boolean simplification: x & True → x, x | False → x, etc.
fn try_simplify_boolean(op: &str, left: &PyExpr, right: &PyExpr) -> Option<PyExpr> {
    // Check for identity operations with True/False literals
    let left_bool = get_literal_bool(left);
    let right_bool = get_literal_bool(right);

    match op {
        "And" => {
            // x & True → x
            if right_bool == Some(true) {
                return Some(left.clone());
            }
            // True & x → x
            if left_bool == Some(true) {
                return Some(right.clone());
            }
            // x & False → False
            if right_bool == Some(false) || left_bool == Some(false) {
                return Some(make_literal_bool(false));
            }
        }
        "Or" => {
            // x | False → x
            if right_bool == Some(false) {
                return Some(left.clone());
            }
            // False | x → x
            if left_bool == Some(false) {
                return Some(right.clone());
            }
            // x | True → True
            if right_bool == Some(true) || left_bool == Some(true) {
                return Some(make_literal_bool(true));
            }
        }
        _ => {}
    }

    None
}

/// Recursively optimize a PyExpr tree with constant folding
pub fn optimize_expr(expr: PyExpr) -> PyExpr {
    match expr {
        PyExpr::BinOp { op, left, right } => {
            // First, recursively optimize children
            let opt_left = optimize_expr(*left);
            let opt_right = optimize_expr(*right);

            // Try constant folding (both operands are literals)
            if let Some(folded) = try_fold_binop(&op, &opt_left, &opt_right) {
                return folded;
            }

            // Try boolean simplification (one operand is True/False literal)
            if let Some(simplified) = try_simplify_boolean(&op, &opt_left, &opt_right) {
                return simplified;
            }

            // No optimization possible, return optimized children
            PyExpr::BinOp {
                op,
                left: Box::new(opt_left),
                right: Box::new(opt_right),
            }
        }
        PyExpr::UnaryOp { op, operand } => {
            let opt_operand = optimize_expr(*operand);

            // Try to fold unary operations
            if op == "Not" {
                if let Some(b) = get_literal_bool(&opt_operand) {
                    return make_literal_bool(!b);
                }
            }

            PyExpr::UnaryOp {
                op,
                operand: Box::new(opt_operand),
            }
        }
        PyExpr::Call {
            func,
            on,
            args,
            kwargs,
        } => {
            // Optimize the 'on' expression and all arguments
            let opt_on = on.map(|on| Box::new(optimize_expr(*on)));
            let opt_args: Vec<PyExpr> = args.into_iter().map(optimize_expr).collect();

            PyExpr::Call {
                func,
                on: opt_on,
                args: opt_args,
                kwargs,
            }
        }
        // Column and Literal expressions are already optimal
        _ => expr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{create_session_context, RUNTIME};
    use crate::transpiler::{pyexpr_to_datafusion_inner, Resolver};
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn int(value: i64) -> PyExpr {
        PyExpr::Literal(LiteralValue::Int64(value))
    }

    fn float(value: f64) -> PyExpr {
        PyExpr::Literal(LiteralValue::Float64(value))
    }

    fn string(value: &str) -> PyExpr {
        PyExpr::Literal(LiteralValue::String(value.to_string()))
    }

    fn boolean(value: bool) -> PyExpr {
        PyExpr::Literal(LiteralValue::Boolean(value))
    }

    /// A boolean as the optimizer builds it.
    fn folded_bool(value: bool) -> PyExpr {
        boolean(value)
    }

    fn null() -> PyExpr {
        PyExpr::Literal(LiteralValue::Null)
    }

    fn decimal(value: i128, precision: u8, scale: i8) -> PyExpr {
        PyExpr::Literal(LiteralValue::Decimal128 {
            value,
            precision,
            scale,
        })
    }

    fn date(days: i32) -> PyExpr {
        PyExpr::Literal(LiteralValue::Date32(days))
    }

    fn col(name: &str) -> PyExpr {
        PyExpr::Column(name.to_string())
    }

    fn binop(op: &str, left: PyExpr, right: PyExpr) -> PyExpr {
        PyExpr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn unary(op: &str, operand: PyExpr) -> PyExpr {
        PyExpr::UnaryOp {
            op: op.to_string(),
            operand: Box::new(operand),
        }
    }

    fn call(func: &str, on: Option<PyExpr>, args: Vec<PyExpr>) -> PyExpr {
        PyExpr::Call {
            func: func.to_string(),
            args,
            kwargs: HashMap::new(),
            on: on.map(Box::new),
        }
    }

    // ---- DataFusion as the reference: a rewrite must not change the result ----

    /// One row per truth value of `p`, NULL included, so a boolean identity is
    /// checked under three-valued logic. `a`/`s` are non-boolean operands and
    /// `z` is a zero divisor DataFusion only sees at execution time.
    fn reference_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("z", DataType::Int64, false),
            Field::new("p", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![5, 5, 5])),
                Arc::new(Int64Array::from(vec![0, 0, 0])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(StringArray::from(vec!["x", "x", "x"])),
            ],
        )
        .expect("reference batch")
    }

    /// Transpiles `expr` without the optimization pass and evaluates it, so the
    /// result is what DataFusion makes of the tree as written.
    fn evaluate(expr: PyExpr) -> Result<ArrayRef, String> {
        let batch = reference_batch();
        let schema = batch.schema();
        let df_expr = pyexpr_to_datafusion_inner(expr, &Resolver::new(&schema)?)?;
        let ctx = create_session_context();
        RUNTIME.block_on(async {
            let df = ctx
                .read_batch(batch)
                .and_then(|df| df.select(vec![df_expr.alias("v")]))
                .map_err(|e| e.to_string())?;
            let schema = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect().await.map_err(|e| e.to_string())?;
            let out = concat_batches(&schema, &batches).map_err(|e| e.to_string())?;
            Ok(Arc::clone(out.column(0)))
        })
    }

    /// `None` when DataFusion gives the same column (type, values, NULLs) for
    /// both trees or rejects both; otherwise a description of the difference.
    fn semantic_diff(original: &PyExpr, rewritten: &PyExpr) -> Option<String> {
        match (evaluate(original.clone()), evaluate(rewritten.clone())) {
            (Ok(want), Ok(got)) if got.as_ref() == want.as_ref() => None,
            (Err(_), Err(_)) => None,
            (want, got) => Some(format!(
                "{original:?}\n  -> {rewritten:?}\n  as written: {want:?}\n  rewritten:  {got:?}"
            )),
        }
    }

    fn assert_same_result(original: &PyExpr, rewritten: &PyExpr) {
        if let Some(diff) = semantic_diff(original, rewritten) {
            panic!("rewrite changes the result: {diff}");
        }
    }

    // ---- numeric_literal / get_literal_bool: what counts as a foldable literal ----

    #[test]
    fn numeric_literal_table() {
        let table = [
            (int(42), Some(Num::Int(42))),
            (int(-7), Some(Num::Int(-7))),
            (float(2.5), Some(Num::Float(2.5))),
            (float(f64::INFINITY), Some(Num::Float(f64::INFINITY))),
            // Booleans, numeric-looking strings, Decimals and NULL do not fold.
            (boolean(true), None),
            (string("1"), None),
            (decimal(1, 1, 0), None),
            (null(), None),
            (col("a"), None),
        ];
        for (expr, expected) in table {
            assert_eq!(numeric_literal(&expr), expected, "{expr:?}");
        }
    }

    #[test]
    fn boolean_literal_table() {
        let table = [
            (boolean(true), Some(true)),
            (boolean(false), Some(false)),
            (string("true"), None),
            (int(1), None),
            (null(), None),
            (col("p"), None),
        ];
        for (expr, expected) in table {
            assert_eq!(get_literal_bool(&expr), expected, "{expr:?}");
        }
    }

    // ---- try_fold_binop ----

    #[test]
    fn fold_table() {
        let table = [
            // Two Int64 literals fold as integers.
            ("Add", int(2), int(3), int(5)),
            ("Sub", int(2), int(5), int(-3)),
            ("Mul", int(4), int(-3), int(-12)),
            ("Div", int(6), int(3), int(2)),
            // Integer division truncates, as in DataFusion.
            ("Div", int(7), int(2), int(3)),
            ("Div", int(-7), int(2), int(-3)),
            // Exact beyond f64's 53-bit mantissa.
            ("Add", int(1 << 53), int(1), int((1 << 53) + 1)),
            ("Eq", int((1 << 53) + 1), int(1 << 53), folded_bool(false)),
            // The remainder takes the dividend's sign, as in DataFusion
            // (Python's -7 % 3 would be 2).
            ("Mod", int(-7), int(3), int(-1)),
            ("Add", float(1.5), float(1.25), float(2.75)),
            ("Sub", float(0.5), float(2.0), float(-1.5)),
            ("Mul", float(2.5), float(0.5), float(1.25)),
            ("Div", float(7.0), float(2.0), float(3.5)),
            // A whole Float64 result stays Float64.
            ("Add", float(2.0), float(3.0), float(5.0)),
            ("Mod", float(-5.5), float(2.0), float(-1.5)),
            // Int64 with Float64 widens to Float64, like DataFusion's coercion.
            ("Add", int(1), float(2.5), float(3.5)),
            ("Div", int(7), float(2.0), float(3.5)),
            ("Mul", float(1e19), float(1.0), float(1e19)),
            ("Mul", float(1e308), float(10.0), float(f64::INFINITY)),
            ("Eq", int(1), int(1), folded_bool(true)),
            ("Eq", int(1), float(1.0), folded_bool(true)),
            ("Eq", float(-0.0), float(0.0), folded_bool(true)),
            ("Ne", int(1), int(2), folded_bool(true)),
            ("Lt", int(1), int(2), folded_bool(true)),
            ("Le", int(2), int(2), folded_bool(true)),
            ("Gt", float(2.5), int(3), folded_bool(false)),
            ("Ge", int(3), float(2.5), folded_bool(true)),
            ("And", boolean(true), boolean(false), folded_bool(false)),
            ("And", boolean(true), boolean(true), folded_bool(true)),
            ("Or", boolean(false), boolean(false), folded_bool(false)),
            ("Or", boolean(false), boolean(true), folded_bool(true)),
        ];
        for (op, left, right, expected) in table {
            let folded = try_fold_binop(op, &left, &right);
            assert_eq!(folded.as_ref(), Some(&expected), "{op} {left:?} {right:?}");
            assert_same_result(&binop(op, left, right), &expected);
        }
    }

    #[test]
    fn fold_declines() {
        let table = [
            ("Add", col("a"), int(1)),
            ("Add", int(1), col("a")),
            // Literal kinds that do not fold together.
            ("Add", int(1), boolean(true)),
            ("And", int(1), boolean(true)),
            ("Add", boolean(true), boolean(true)),
            ("Eq", boolean(true), boolean(true)),
            ("Add", string("a"), string("b")),
            ("Eq", string("1"), int(1)),
            ("Add", null(), int(1)),
            ("And", null(), boolean(false)),
            // A zero divisor stays for DataFusion: an error for integers,
            // inf/NaN for floats.
            ("Div", int(1), int(0)),
            ("Mod", int(1), int(0)),
            ("Div", float(1.0), float(0.0)),
            ("Div", float(1.0), float(-0.0)),
            ("Mod", float(5.5), float(0.0)),
            // Integer overflow is left for DataFusion to report.
            ("Add", int(i64::MAX), int(1)),
            ("Sub", int(i64::MIN), int(1)),
            ("Mul", int(i64::MAX), int(2)),
            ("Div", int(i64::MIN), int(-1)),
            ("Mod", int(i64::MIN), int(-1)),
            // DataFusion orders NaN above every number and equal to itself.
            ("Eq", float(f64::NAN), float(f64::NAN)),
            ("Gt", float(f64::NAN), float(1.0)),
            ("Lt", int(1), float(f64::NAN)),
            // Literals other than Int64/Float64/Boolean never fold.
            ("Add", decimal(15, 2, 1), int(1)),
            ("Eq", date(1), date(1)),
            // FloorDiv is left to the floor_div kernel: folding through f64
            // would lose integer precision and Python's floor semantics.
            ("FloorDiv", int(7), int(2)),
            ("Pow", int(2), int(3)),
            ("And", int(1), int(0)),
            ("Xor", boolean(true), boolean(false)),
        ];
        for (op, left, right) in table {
            assert_eq!(
                try_fold_binop(op, &left, &right),
                None,
                "{op} {left:?} {right:?}"
            );
        }
    }

    // ---- try_simplify_boolean ----

    #[test]
    fn simplify_table() {
        let p = || col("p");
        // a / z fails at execution; DataFusion drops it next to an absorbing
        // literal as well, so dropping it here changes nothing.
        let failing = || binop("Gt", binop("Div", col("a"), col("z")), int(1));
        let table = [
            ("And", p(), boolean(true), p()),
            ("And", boolean(true), p(), p()),
            ("And", p(), boolean(false), folded_bool(false)),
            ("And", boolean(false), p(), folded_bool(false)),
            ("Or", p(), boolean(false), p()),
            ("Or", boolean(false), p(), p()),
            ("Or", p(), boolean(true), folded_bool(true)),
            ("Or", boolean(true), p(), folded_bool(true)),
            ("And", null(), boolean(false), folded_bool(false)),
            ("Or", null(), boolean(true), folded_bool(true)),
            ("And", failing(), boolean(false), folded_bool(false)),
            ("Or", failing(), boolean(true), folded_bool(true)),
        ];
        for (op, left, right, expected) in table {
            let simplified = try_simplify_boolean(op, &left, &right);
            assert_eq!(
                simplified.as_ref(),
                Some(&expected),
                "{op} {left:?} {right:?}"
            );
            assert_same_result(&binop(op, left, right), &expected);
        }
    }

    #[test]
    fn simplify_declines() {
        let p = || col("p");
        let table = [
            ("And", p(), col("a")),
            ("Or", p(), p()),
            // Literals that are not booleans: p AND NULL is NULL or false,
            // depending on p.
            ("And", p(), int(1)),
            ("Or", p(), string("true")),
            ("And", p(), null()),
            ("Or", null(), p()),
            // Only And/Or have identities here.
            ("Eq", p(), boolean(true)),
            ("Ne", p(), boolean(false)),
            ("Add", p(), boolean(true)),
            ("Xor", p(), boolean(true)),
        ];
        for (op, left, right) in table {
            assert_eq!(
                try_simplify_boolean(op, &left, &right),
                None,
                "{op} {left:?} {right:?}"
            );
        }
    }

    // ---- optimize_expr: the recursive entry point ----

    #[test]
    fn optimize_rewrites_bottom_up() {
        let table = [
            // Children fold first, so the parent sees two literals.
            (binop("Mul", binop("Add", int(1), int(2)), int(3)), int(9)),
            (
                binop("Add", col("a"), binop("Mul", int(2), int(3))),
                binop("Add", col("a"), int(6)),
            ),
            // A folded comparison feeds boolean simplification.
            (
                binop("And", col("p"), binop("Lt", int(1), int(2))),
                col("p"),
            ),
            (unary("Not", boolean(true)), folded_bool(false)),
            (
                unary("Not", binop("And", boolean(true), boolean(false))),
                folded_bool(true),
            ),
            // A call's receiver and positional arguments are optimized.
            (
                call("abs", Some(binop("Sub", int(1), int(4))), vec![]),
                call("abs", Some(int(-3)), vec![]),
            ),
            (
                call(
                    "coalesce",
                    None,
                    vec![col("a"), binop("Add", int(1), int(2))],
                ),
                call("coalesce", None, vec![col("a"), int(3)]),
            ),
        ];
        for (input, expected) in table {
            let optimized = optimize_expr(input.clone());
            assert_eq!(optimized, expected, "{input:?}");
            assert_same_result(&input, &optimized);
        }
    }

    #[test]
    fn optimize_leaves_tree_unchanged() {
        let three = || binop("Add", int(1), int(2));
        let table = [
            col("a"),
            int(1),
            null(),
            // No reassociation: (a + 1) + 2 does not become a + 3.
            binop("Add", binop("Add", col("a"), int(1)), int(2)),
            binop("FloorDiv", int(7), int(2)),
            // Only Not of a boolean literal folds.
            unary("Not", col("p")),
            unary("Not", int(1)),
            unary("Not", null()),
            unary("Neg", int(5)),
            unary("Neg", boolean(true)),
            // The optimizer does not descend into kwargs, Alias or Window.
            PyExpr::Call {
                func: "shift".to_string(),
                args: vec![int(1)],
                kwargs: HashMap::from([("default".to_string(), three())]),
                on: Some(Box::new(col("a"))),
            },
            PyExpr::Alias {
                expr: Box::new(three()),
                alias: "v".to_string(),
            },
            PyExpr::Window {
                expr: Box::new(three()),
                partition_by: None,
                order_by: None,
                descending: false,
            },
        ];
        for expr in table {
            assert_eq!(optimize_expr(expr.clone()), expr, "{expr:?}");
        }
    }

    // ---- Known divergences ----
    //
    // Rewrites that change the result today. `cargo test -- --ignored` runs
    // them; each lists every row that still diverges.

    fn diverging(rows: Vec<PyExpr>) -> Vec<String> {
        rows.into_iter()
            .filter_map(|row| semantic_diff(&row, &optimize_expr(row.clone())))
            .collect()
    }

    #[test]
    fn numeric_folding_keeps_datafusion_semantics() {
        let rows = vec![
            // Int64 / Int64 is integer division in DataFusion.
            binop("Div", int(7), int(2)),
            // f64 has 53 bits of mantissa.
            binop("Add", int(1 << 53), int(1)),
            binop("Eq", int((1 << 53) + 1), int(1 << 53)),
            // A whole Float64 result comes back as Int64.
            binop("Add", float(2.0), float(3.0)),
            // Overflow folds to a Float64; DataFusion wraps or errors.
            binop("Add", int(i64::MAX), int(1)),
            binop("Div", int(i64::MIN), int(-1)),
            // DataFusion compares NaN as equal to itself and above every number.
            binop("Eq", float(f64::NAN), float(f64::NAN)),
            binop("Gt", float(f64::NAN), float(1.0)),
        ];
        let diffs = diverging(rows);
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    }

    #[test]
    #[ignore = "boolean simplification ignores operand types (#209)"]
    fn boolean_simplification_keeps_operand_types() {
        let rows = vec![
            // DataFusion rejects AND/OR on a non-boolean operand at planning.
            binop("And", col("a"), boolean(true)),
            binop("Or", col("a"), boolean(false)),
            binop("And", col("s"), boolean(false)),
            binop("Or", col("s"), boolean(true)),
            // DataFusion types NULL AND true as Boolean; the kept literal is Null.
            binop("And", null(), boolean(true)),
            binop("Or", null(), boolean(false)),
        ];
        let diffs = diverging(rows);
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    }
}
