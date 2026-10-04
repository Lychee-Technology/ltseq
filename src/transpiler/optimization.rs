//! Expression optimization: Constant folding and boolean simplification
//!
//! This module provides compile-time optimizations for PyExpr trees:
//! - **Constant Folding**: Arithmetic operations on literals are evaluated at compile time
//!   (e.g., `1 + 2 + r.col` → `3 + r.col`)
//! - **Boolean Simplification**: Trivial boolean expressions are simplified
//!   (e.g., `x & True` → `x`, `x | False` → `x`)

use crate::types::PyExpr;

/// Extract numeric value from a literal PyExpr (for constant folding)
fn get_literal_f64(expr: &PyExpr) -> Option<f64> {
    match expr {
        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "Int64" | "Int32" => value.parse::<i64>().ok().map(|v| v as f64),
            "Float64" | "Float32" => value.parse::<f64>().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Extract boolean value from a literal PyExpr
fn get_literal_bool(expr: &PyExpr) -> Option<bool> {
    match expr {
        PyExpr::Literal { value, dtype } => match dtype.as_str() {
            "Boolean" | "Bool" => match value.to_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Create a literal PyExpr from a float value
fn make_literal_f64(value: f64) -> PyExpr {
    // If it's a whole number, prefer Int64 representation
    if value.fract() == 0.0 && value.abs() < i64::MAX as f64 {
        PyExpr::Literal {
            value: (value as i64).to_string(),
            dtype: "Int64".to_string(),
        }
    } else {
        PyExpr::Literal {
            value: value.to_string(),
            dtype: "Float64".to_string(),
        }
    }
}

/// Create a literal PyExpr from a boolean value
fn make_literal_bool(value: bool) -> PyExpr {
    PyExpr::Literal {
        value: value.to_string(),
        dtype: "Boolean".to_string(),
    }
}

/// Try to fold a binary operation on two literals into a single literal
fn try_fold_binop(op: &str, left: &PyExpr, right: &PyExpr) -> Option<PyExpr> {
    // Try arithmetic folding
    if let (Some(l), Some(r)) = (get_literal_f64(left), get_literal_f64(right)) {
        let result = match op {
            "Add" => Some(l + r),
            "Sub" => Some(l - r),
            "Mul" => Some(l * r),
            "Div" if r != 0.0 => Some(l / r),
            "Mod" if r != 0.0 => Some(l % r),
            _ => None,
        };
        if let Some(v) = result {
            return Some(make_literal_f64(v));
        }

        // Try comparison folding
        let cmp_result = match op {
            "Eq" => Some(l == r),
            "Ne" => Some(l != r),
            "Lt" => Some(l < r),
            "Le" => Some(l <= r),
            "Gt" => Some(l > r),
            "Ge" => Some(l >= r),
            _ => None,
        };
        if let Some(b) = cmp_result {
            return Some(make_literal_bool(b));
        }
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
    use crate::transpiler::pyexpr_to_datafusion_inner;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn lit(value: &str, dtype: &str) -> PyExpr {
        PyExpr::Literal {
            value: value.to_string(),
            dtype: dtype.to_string(),
        }
    }

    fn int(value: i64) -> PyExpr {
        lit(&value.to_string(), "Int64")
    }

    fn float(value: f64) -> PyExpr {
        lit(&value.to_string(), "Float64")
    }

    /// A boolean as Python serializes it (`str(True)`).
    fn boolean(value: bool) -> PyExpr {
        lit(if value { "True" } else { "False" }, "Boolean")
    }

    /// A boolean as the optimizer builds it.
    fn folded_bool(value: bool) -> PyExpr {
        lit(if value { "true" } else { "false" }, "Boolean")
    }

    fn null() -> PyExpr {
        lit("None", "Null")
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
        let df_expr = pyexpr_to_datafusion_inner(expr, &batch.schema())?;
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

    // ---- get_literal_f64 / get_literal_bool: what counts as a foldable literal ----

    #[test]
    fn numeric_literal_table() {
        let table = [
            (int(42), Some(42.0)),
            (int(-7), Some(-7.0)),
            (lit("7", "Int32"), Some(7.0)),
            (float(2.5), Some(2.5)),
            (lit("1.5", "Float32"), Some(1.5)),
            (lit("inf", "Float64"), Some(f64::INFINITY)),
            // An unparseable payload is left for the transpiler to report.
            (lit("1.5", "Int64"), None),
            (lit("abc", "Float64"), None),
            // Booleans, numeric-looking strings and NULL are not numbers.
            (boolean(true), None),
            (lit("1", "String"), None),
            (null(), None),
            (col("a"), None),
        ];
        for (expr, expected) in table {
            assert_eq!(get_literal_f64(&expr), expected, "{expr:?}");
        }
    }

    #[test]
    fn boolean_literal_table() {
        let table = [
            (boolean(true), Some(true)),
            (boolean(false), Some(false)),
            (lit("true", "Boolean"), Some(true)),
            (lit("FALSE", "Bool"), Some(false)),
            (lit("yes", "Boolean"), None),
            (lit("1", "Boolean"), None),
            (lit("true", "String"), None),
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
            // Integer results come back as Int64 (make_literal_f64's first branch).
            ("Add", int(2), int(3), int(5)),
            ("Sub", int(2), int(5), int(-3)),
            ("Mul", int(4), int(-3), int(-12)),
            ("Div", int(6), int(3), int(2)),
            // The remainder takes the dividend's sign, as in DataFusion
            // (Python's -7 % 3 would be 2).
            ("Mod", int(-7), int(3), int(-1)),
            ("Add", float(1.5), float(1.25), float(2.75)),
            ("Sub", float(0.5), float(2.0), float(-1.5)),
            ("Mul", float(2.5), float(0.5), float(1.25)),
            ("Div", float(7.0), float(2.0), float(3.5)),
            ("Mod", float(-5.5), float(2.0), float(-1.5)),
            // Int64 with Float64 widens to Float64, like DataFusion's coercion.
            ("Add", int(1), float(2.5), float(3.5)),
            ("Div", int(7), float(2.0), float(3.5)),
            // Whole but beyond Int64, and non-finite: make_literal_f64 keeps Float64.
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
            ("Add", lit("a", "String"), lit("b", "String")),
            ("Eq", lit("1", "String"), int(1)),
            ("Add", null(), int(1)),
            ("And", null(), boolean(false)),
            ("Add", lit("abc", "Int64"), int(1)),
            // A zero divisor stays for DataFusion: an error for integers,
            // inf/NaN for floats.
            ("Div", int(1), int(0)),
            ("Mod", int(1), int(0)),
            ("Div", float(1.0), float(0.0)),
            ("Div", float(1.0), float(-0.0)),
            ("Mod", float(5.5), float(0.0)),
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
            ("Or", p(), lit("true", "String")),
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
            lit("abc", "Int64"),
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
    #[ignore = "constant folding goes through f64 (#193)"]
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
            // Int32/Float32 operands fold to 64-bit literals.
            binop("Add", lit("2", "Int32"), lit("3", "Int32")),
            binop("Add", lit("0.1", "Float32"), lit("0.2", "Float32")),
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
