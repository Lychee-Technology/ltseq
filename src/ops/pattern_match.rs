//! Streaming pattern matcher for sequential funnel queries.
//!
//! Given N step predicates and an optional partition_by column, finds all rows
//! where consecutive rows (within the same partition) match the step predicates
//! in sequence.
//!
//! The algorithm:
//! 1. Execute the table's plan once, in table order, keeping every column
//!    (`search_pattern_count` prunes to the columns its steps read instead)
//! 2. Evaluate each step predicate as a BooleanArray (vectorized, per batch)
//! 3. Single-pass scan: for each step1 match at row i, check step2 at i+1, step3 at i+2, etc.
//!    Skip if any row crosses a partition boundary.
//! 4. Take the matching rows from the batches of that same execution.
//!
//! This avoids computing LEAD/shift for ALL rows — instead, only ~0.1% of rows
//! (those matching step 1) trigger any further evaluation.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, BooleanArray, Int64Array, LargeStringArray,
    RecordBatch, RecordBatchOptions, StringArray, StringViewArray, UInt64Array,
};
use datafusion::arrow::compute::{concat, concat_batches, interleave};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::*;

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::linear_scan::{build_sort_exprs, extract_referenced_columns};
use crate::types::{LiteralValue, PyExpr};
use crate::LTSeqTable;

/// Evaluate a predicate PyExpr against a RecordBatch, returning a BooleanArray.
///
/// Supports a subset of expressions commonly used in funnel predicates:
/// - Column references
/// - Literal values
/// - BinOp: + - * / // %, ==, !=, <, >, <=, >=, &, |
/// - UnaryOp: Not
/// - Call: starts_with, ends_with, contains (string methods via .s accessor)
/// - Call: is_null, is_not_null
///
/// Comparison and arithmetic operands take DataFusion's coercion, float
/// comparisons treat `-0.0` as `0.0`, and a comparison places a literal
/// operand as the transpiler places it ([`eval_lowered_binop`]), as in
/// `filter`.
///
/// NULL follows SQL three-valued logic, as in `filter`, and stays NULL in the
/// returned mask; callers read a NULL slot as no match.
pub(crate) fn eval_predicate(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Result<BooleanArray, String> {
    let arr = null_as_boolean(&eval_expr(expr, batch, name_to_idx)?)?;
    // Convert result to BooleanArray
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            format!(
                "Predicate must evaluate to boolean, got {:?}",
                arr.data_type()
            )
        })
}

/// Recursively evaluate an expression tree against a RecordBatch.
fn eval_expr(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Result<ArrayRef, String> {
    match expr {
        PyExpr::Column(name) => {
            let idx = name_to_idx
                .get(name)
                .ok_or_else(|| format!("Column '{}' not found in batch", name))?;
            Ok(Arc::clone(batch.column(*idx)))
        }

        PyExpr::Literal(value) => {
            let n = batch.num_rows();
            match value {
                // A bare NULL is NULL-typed, as DataFusion types `lit(NULL)`:
                // each operator reads it as its other operand's type, so
                // `r.x / None` is integer true division and `flag & None`
                // is Boolean (`eval_binop`).
                LiteralValue::Null => {
                    Ok(datafusion::arrow::array::new_null_array(&DataType::Null, n))
                }
                _ => value
                    .to_scalar_value()
                    .to_array_of_size(n)
                    .map_err(|e| format!("Cannot build literal array: {e}")),
            }
        }

        PyExpr::BinOp { op, left, right } if meets_literal(op, left, right)? => {
            eval_lowered_binop(op, left, right, batch, name_to_idx)
        }

        PyExpr::BinOp { op, left, right } => {
            let left_arr = eval_expr(left, batch, name_to_idx)?;
            let right_arr = eval_expr(right, batch, name_to_idx)?;
            eval_binop(op, &left_arr, &right_arr)
        }

        PyExpr::UnaryOp { op, operand } => {
            let arr = eval_expr(operand, batch, name_to_idx)?;
            match op.as_str() {
                "Not" => {
                    let arr = null_as_boolean(&arr)?;
                    let bool_arr = arr
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or("NOT requires boolean operand")?;
                    let result =
                        datafusion::arrow::compute::kernels::boolean::not(bool_arr)
                            .map_err(|e| format!("NOT failed: {}", e))?;
                    Ok(Arc::new(result))
                }
                _ => Err(format!("Unsupported unary op: {}", op)),
            }
        }

        PyExpr::Call {
            func,
            args,
            on,
            kwargs: _,
        } => eval_call(func, args, on.as_deref(), batch, name_to_idx),

        PyExpr::Window { .. } => {
            Err("Window expressions not supported in search_pattern predicates".to_string())
        }
        PyExpr::Alias { expr, .. } => eval_expr(expr, batch, name_to_idx),
    }
}

/// Whether `left op right` is a comparison with a constant operand
/// ([`constant`]), which places the literal among the other operand's
/// values. Arithmetic keeps this evaluator's checked kernels (#221).
fn meets_literal(op: &str, left: &PyExpr, right: &PyExpr) -> Result<bool, String> {
    use crate::transpiler::BinaryOp;
    use datafusion::logical_expr::Operator;

    let comparison = matches!(
        crate::transpiler::parse_binary_op(op)?,
        BinaryOp::Native(
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        )
    );
    Ok(comparison && (constant(left) || constant(right)))
}

/// Whether `expr` is a constant the transpiler folds into a literal: a
/// literal, or literals combined by operators. A function call is not one
/// here, so this evaluator still reports the functions it does not support.
fn constant(expr: &PyExpr) -> bool {
    match expr {
        PyExpr::Literal(_) => true,
        PyExpr::BinOp { left, right, .. } => constant(left) && constant(right),
        PyExpr::UnaryOp { operand, .. } => constant(operand),
        PyExpr::Alias { expr, .. } => constant(expr),
        _ => false,
    }
}

/// The comparison `left op right` with a constant operand, lowered by the
/// transpiler and evaluated by DataFusion, so that the literal reads as it
/// does in `filter`. It places the literal among the values of the other
/// operand when that operand is of an exact domain (an integer, a decimal,
/// a date or timestamp), rather than casting both to a common type: next to
/// an Int64, `x > 2.0**53` keeps `2**53 + 1`, which a Float64 cast would
/// round to `2**53`, and NaN is refused as `filter` refuses it.
///
/// The other operand, unless it is constant too, is evaluated here and
/// handed to the transpiler as a column of its type, named as the column it
/// is so that errors name it; a constant operand is handed over as written,
/// so that it folds as it does in `filter`. The constant is evaluated here
/// first, on one row, so that its integer arithmetic raises on overflow as
/// this evaluator's checked kernels do (#221); DataFusion's simplifier,
/// which folds it for the transpiler, wraps.
fn eval_lowered_binop(
    op: &str,
    left: &PyExpr,
    right: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Result<ArrayRef, String> {
    use datafusion::arrow::array::RecordBatchOptions;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::physical_expr::create_physical_expr;

    let rows = batch.num_rows();
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let one_row = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|e| e.to_string())?;
    let mut operand = |expr: &PyExpr| -> Result<Box<PyExpr>, String> {
        if constant(expr) {
            eval_expr(expr, &one_row, name_to_idx)?;
            return Ok(Box::new(expr.clone()));
        }
        let values = eval_expr(expr, batch, name_to_idx)?;
        let name = match expr {
            PyExpr::Column(name) => name.clone(),
            _ => "__ltseq_operand".to_string(),
        };
        fields.push(Field::new(&name, values.data_type().clone(), true));
        columns.push(values);
        Ok(Box::new(PyExpr::Column(name)))
    };
    let lowered = PyExpr::BinOp {
        op: op.to_string(),
        left: operand(left)?,
        right: operand(right)?,
    };
    let schema = Arc::new(Schema::new(fields));
    let expr = crate::transpiler::pyexpr_to_datafusion(lowered, &schema)?;
    let df_schema = DFSchema::try_from(Arc::clone(&schema)).map_err(|e| e.to_string())?;
    let physical = create_physical_expr(
        &expr,
        &df_schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
    .map_err(|e| format!("{op} failed: {e}"))?;
    let input = RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| e.to_string())?;
    physical
        .evaluate(&input)
        .and_then(|result| result.into_array(rows))
        .map_err(|e| format!("{op} failed: {e}"))
}

/// Evaluate a binary operation on two arrays.
///
/// Operator names are parsed by the transpiler's `parse_binary_op` — the
/// single source of truth shared with the DataFusion path — so the serializer
/// (expr/base.py), the transpiler, and this evaluator can never drift apart.
fn eval_binop(op: &str, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    use crate::transpiler::BinaryOp;
    use datafusion::arrow::compute::kernels::boolean;
    use datafusion::arrow::compute::kernels::numeric;
    use datafusion::logical_expr::Operator;

    let operator = match crate::transpiler::parse_binary_op(op)? {
        // Integer `/` is true division, as the DataFusion path computes it
        // (#218); arrow's `div` would truncate. `integer_pair` is the test
        // the transpiler applies: it reads encoded operands by their value
        // type and takes a NULL literal next to an integer.
        BinaryOp::Native(Operator::Divide)
            if crate::transpiler::true_div::integer_pair(left.data_type(), right.data_type()) =>
        {
            return crate::transpiler::true_div::true_div_arrays(left, right)
                .map_err(|e| format!("Div failed: {}", e));
        }
        BinaryOp::Native(operator) => operator,
        BinaryOp::FloorDiv => {
            return crate::transpiler::floor_div::floor_div_arrays(left, right)
                .map_err(|e| format!("FloorDiv failed: {}", e));
        }
    };

    // Comparisons and arithmetic take DataFusion's operand coercion, as the
    // DataFusion path does: `r.x / 2 > r.i` compares a Float64 quotient with
    // an Int64 column, and `(r.x / 2) + 1` adds an Int64 literal to it;
    // Arrow's kernels refuse operands of two types. (A comparison with a
    // literal operand is lowered by the transpiler instead,
    // `eval_lowered_binop`.) A NULL-typed operand is coerced to its other
    // operand's type; next to `&` or `|` it is Boolean.
    let coerced = match operator {
        Operator::And | Operator::Or => Some((null_as_boolean(left)?, null_as_boolean(right)?)),
        _ => coerce_operands(&operator, left, right)?,
    };
    let (left, right) = match &coerced {
        Some((l, r)) => (l, r),
        None => (left, right),
    };

    // An operand still NULL-typed (both were NULL literals, or coercion
    // found no common type) makes the result NULL on every row, as in
    // DataFusion; Arrow's kernels take no NULL-typed operand.
    if left.data_type() == &DataType::Null || right.data_type() == &DataType::Null {
        let result_type = match operator {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => DataType::Boolean,
            _ => DataType::Null,
        };
        return Ok(datafusion::arrow::array::new_null_array(
            &result_type,
            left.len(),
        ));
    }

    match operator {
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => compare(operator, left, right),
        Operator::Plus => numeric::add(left, right).map_err(|e| format!("Add failed: {}", e)),
        Operator::Minus => numeric::sub(left, right).map_err(|e| format!("Sub failed: {}", e)),
        Operator::Multiply => numeric::mul(left, right).map_err(|e| format!("Mul failed: {}", e)),
        Operator::Divide => numeric::div(left, right).map_err(|e| format!("Div failed: {}", e)),
        Operator::Modulo => numeric::rem(left, right).map_err(|e| format!("Mod failed: {}", e)),
        // `&` and `|` are Kleene, as in `filter`: `NULL AND FALSE` is FALSE
        // and `NULL OR TRUE` is TRUE. Only the caller's final mask reads NULL
        // as no match; read earlier, it would be wrong under `~`.
        Operator::And => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("AND requires boolean operands")?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("AND requires boolean operands")?;
            Ok(Arc::new(
                boolean::and_kleene(l, r).map_err(|e| format!("AND failed: {}", e))?,
            ))
        }
        Operator::Or => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("OR requires boolean operands")?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("OR requires boolean operands")?;
            Ok(Arc::new(
                boolean::or_kleene(l, r).map_err(|e| format!("OR failed: {}", e))?,
            ))
        }
        // parse_binary_op yields none of the remaining operators; if it ever
        // does, error explicitly so the parallel path falls back and never
        // counts 0.
        other => Err(format!(
            "Unsupported binary operator in search_pattern predicate: {:?}",
            other
        )),
    }
}

/// `arr` as Boolean when it is NULL-typed (a NULL literal, or an expression
/// over NULL literals only), so that boolean operators and the final mask
/// read it as Boolean NULL, as DataFusion coerces it; otherwise `arr`.
fn null_as_boolean(arr: &ArrayRef) -> Result<ArrayRef, String> {
    if arr.data_type() == &DataType::Null {
        Ok(datafusion::arrow::array::new_null_array(
            &DataType::Boolean,
            arr.len(),
        ))
    } else {
        Ok(Arc::clone(arr))
    }
}

/// `left` and `right` cast to the types DataFusion coerces the operands of
/// `left op right` to (`BinaryTypeCoercer`, as the transpiler resolves
/// them), or `None` when they already have those types. Operands DataFusion
/// cannot coerce are left as they are, so the kernel reports them as before.
fn coerce_operands(
    op: &datafusion::logical_expr::Operator,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>, String> {
    use datafusion::arrow::compute::{cast_with_options, CastOptions};

    let Ok((left_type, right_type)) =
        crate::transpiler::binary_input_types(left.data_type(), op, right.data_type())
    else {
        return Ok(None);
    };
    if left.data_type() == &left_type && right.data_type() == &right_type {
        return Ok(None);
    }
    // The coerced types hold every operand value; `safe: false` makes a value
    // that did not fit an error rather than a silent NULL.
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let cast = |arr: &ArrayRef, to: &DataType| {
        cast_with_options(arr, to, &options)
            .map_err(|e| format!("Cannot cast {} to {to}: {e}", arr.data_type()))
    };
    Ok(Some((cast(left, &left_type)?, cast(right, &right_type)?)))
}

/// Compare two arrays with `apply_cmp`, the function DataFusion's
/// `BinaryExpr` compares with. It turns `-0.0` into `0.0` before Arrow's
/// `cmp` kernels, which order floats by IEEE 754 totalOrder and would put
/// `-0.0` below `0.0`: `0 / -2 == 0.0` holds in `filter` and must here.
/// It does so only for a plain float array, so a dictionary or run-end
/// encoded float is decoded first, as the transpiler decodes it in
/// `filter` (#292).
fn compare(
    op: datafusion::logical_expr::Operator,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef, String> {
    use datafusion::arrow::compute::cast;
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_expr_common::datum::apply_cmp;

    let decoded = |arr: &ArrayRef| match crate::transpiler::encoded_float(arr.data_type()) {
        Some(value_type) => cast(arr, &value_type).map_err(|e| format!("{op} failed: {e}")),
        None => Ok(Arc::clone(arr)),
    };
    let left_value = ColumnarValue::Array(decoded(left)?);
    let right_value = ColumnarValue::Array(decoded(right)?);
    apply_cmp(op, &left_value, &right_value)
        .and_then(|result| result.into_array(left.len()))
        .map_err(|e| format!("{op} failed: {e}"))
}

/// Evaluate a function call expression.
fn eval_call(
    func: &str,
    args: &[PyExpr],
    on: Option<&PyExpr>,
    batch: &RecordBatch,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Result<ArrayRef, String> {
    // The receiver is required only by the functions this evaluator supports;
    // an unsupported function is reported as unsupported whatever its shape.
    let receiver = || crate::transpiler::require_on(on, func);
    match func {
        "starts_with" | "str_starts_with" => {
            let source = eval_expr(receiver()?, batch, name_to_idx)?;
            let prefix = extract_literal_string(&args[0])?;
            eval_string_predicate(&source, "starts_with", |s| s.starts_with(prefix.as_str()))
        }
        "ends_with" | "str_ends_with" => {
            let source = eval_expr(receiver()?, batch, name_to_idx)?;
            let suffix = extract_literal_string(&args[0])?;
            eval_string_predicate(&source, "ends_with", |s| s.ends_with(suffix.as_str()))
        }
        "contains" | "str_contains" => {
            let source = eval_expr(receiver()?, batch, name_to_idx)?;
            let substr = extract_literal_string(&args[0])?;
            eval_string_predicate(&source, "contains", |s| s.contains(substr.as_str()))
        }
        // Arrow's `is_null` kernels read logical nulls, as DataFusion's
        // IS NULL does: a NULL-typed array (a NULL literal) has no null
        // buffer, yet every slot is NULL.
        "is_null" => {
            let source = eval_expr(receiver()?, batch, name_to_idx)?;
            let result = datafusion::arrow::compute::is_null(&source)
                .map_err(|e| format!("is_null failed: {e}"))?;
            Ok(Arc::new(result))
        }
        "is_not_null" => {
            let source = eval_expr(receiver()?, batch, name_to_idx)?;
            let result = datafusion::arrow::compute::is_not_null(&source)
                .map_err(|e| format!("is_not_null failed: {e}"))?;
            Ok(Arc::new(result))
        }
        // The .s accessor wraps string methods — the transpiler unwraps it
        // so func might come through as the direct method name.
        _ => Err(format!(
            "Unsupported function in search_pattern predicate: '{}'",
            func
        )),
    }
}

/// Extract a literal string value from a PyExpr.
fn extract_literal_string(expr: &PyExpr) -> Result<String, String> {
    match expr {
        PyExpr::Literal(value) => value
            .require_str("search_pattern string argument")
            .map(str::to_string),
        _ => Err("Expected literal string argument".to_string()),
    }
}

pub(crate) struct StartsWithFastPathPlan {
    pub(crate) column: String,
    pub(crate) prefixes: Vec<String>,
}

/// Detect the fast-path shape: every predicate is `col.starts_with("lit")`
/// on the SAME string column. Returns `None` for any other shape (mixed
/// columns, non-literal arguments, other functions).
pub(crate) fn same_string_column_starts_with_plan(
    step_predicates: &[PyExpr],
) -> Option<StartsWithFastPathPlan> {
    if step_predicates.is_empty() {
        return None;
    }

    let mut column_name: Option<String> = None;
    let mut prefixes = Vec::with_capacity(step_predicates.len());

    for expr in step_predicates {
        let PyExpr::Call { func, args, on, kwargs } = expr else {
            return None;
        };
        if !kwargs.is_empty() || args.len() != 1 {
            return None;
        }
        if func != "starts_with" && func != "str_starts_with" {
            return None;
        }
        let Some(PyExpr::Column(name)) = on.as_deref() else {
            return None;
        };
        let PyExpr::Literal(LiteralValue::String(prefix)) = &args[0] else {
            return None;
        };
        let prefix = prefix.clone();

        match &column_name {
            Some(existing) if existing != name => return None,
            Some(_) => {}
            None => column_name = Some(name.clone()),
        }
        prefixes.push(prefix);
    }

    Some(StartsWithFastPathPlan {
        column: column_name?,
        prefixes,
    })
}

/// Count sequential prefix matches on a string column, dispatching on the
/// concrete Arrow string array type. Returns `None` if the column is not a
/// supported string array — callers must then fall back to vectorized
/// predicate evaluation instead of assuming a zero count.
///
/// When `step1_mask` is `Some`, `prefixes` covers steps 2..N and is checked
/// starting at row `i + 1`; when `None`, `prefixes` covers all steps starting
/// at row `i`.
pub(crate) fn count_prefix_matches_on(
    col: &ArrayRef,
    step1_mask: Option<&BooleanArray>,
    prefixes: &[&str],
    max_start: usize,
    same_partition: impl Fn(usize) -> bool + Copy,
) -> Option<usize> {
    let any = col.as_any();
    if let Some(arr) = any.downcast_ref::<StringViewArray>() {
        Some(count_prefix_matches(arr, step1_mask, prefixes, max_start, same_partition))
    } else if let Some(arr) = any.downcast_ref::<StringArray>() {
        Some(count_prefix_matches(arr, step1_mask, prefixes, max_start, same_partition))
    } else {
        any.downcast_ref::<LargeStringArray>()
            .map(|arr| count_prefix_matches(arr, step1_mask, prefixes, max_start, same_partition))
    }
}

fn count_prefix_matches<'a, A>(
    arr: A,
    step1_mask: Option<&BooleanArray>,
    prefixes: &[&str],
    max_start: usize,
    same_partition: impl Fn(usize) -> bool,
) -> usize
where
    A: ArrayAccessor<Item = &'a str> + Copy,
{
    let offset = if step1_mask.is_some() { 1 } else { 0 };
    let mut count = 0;
    for i in 0..max_start {
        if let Some(mask) = step1_mask {
            if !mask.is_valid(i) || !mask.value(i) {
                continue;
            }
        }
        if !same_partition(i) {
            continue;
        }
        let mut all_match = true;
        for (k, prefix) in prefixes.iter().enumerate() {
            let row = i + k + offset;
            if arr.is_null(row) || !arr.value(row).starts_with(prefix) {
                all_match = false;
                break;
            }
        }
        if all_match {
            count += 1;
        }
    }
    count
}

/// Evaluate a string test (`starts_with`, `ends_with`, `contains`) on every
/// value of a Utf8, LargeUtf8 or Utf8View array; NULL stays NULL. `func`
/// names the operation in the error for any other array type.
fn eval_string_predicate(
    source: &ArrayRef,
    func: &str,
    test: impl Fn(&str) -> bool,
) -> Result<ArrayRef, String> {
    fn apply<'a>(arr: impl ArrayAccessor<Item = &'a str>, test: impl Fn(&str) -> bool) -> ArrayRef {
        let result: BooleanArray = ArrayIter::new(arr).map(|v| v.map(&test)).collect();
        Arc::new(result)
    }

    let any = source.as_any();
    if let Some(arr) = any.downcast_ref::<StringArray>() {
        Ok(apply(arr, test))
    } else if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
        Ok(apply(arr, test))
    } else if let Some(arr) = any.downcast_ref::<StringViewArray>() {
        Ok(apply(arr, test))
    } else {
        Err(format!(
            "{} requires string column, got {:?}",
            func,
            source.data_type()
        ))
    }
}

/// Compute partition boundaries from a partition column.
///
/// Returns a BooleanArray where true = start of a new partition.
/// Row 0 is always true. Consecutive rows with the same partition value
/// are in the same partition, a NULL key being equal to NULL only.
///
/// Float keys are equal as the contract's §18 says partitions compare them:
/// `-0.0` equals `0.0`, and every NaN equals every other NaN (#289). Arrow's
/// `eq` compares floats bit for bit, so they are made one value first.
fn compute_partition_boundaries(
    partition_col: &ArrayRef,
) -> Result<BooleanArray, String> {
    let partition_col = &crate::ops::common::equal_floats_as_one(Arc::clone(partition_col));
    let n = partition_col.len();
    if n == 0 {
        return Ok(BooleanArray::from(Vec::<bool>::new()));
    }

    let mut boundaries = vec![false; n];
    boundaries[0] = true; // First row always starts a new partition

    // Use Arrow comparison: compare partition_col with itself shifted by 1
    // A boundary occurs where partition_col[i] != partition_col[i-1]
    match partition_col.data_type() {
        DataType::Int64 => {
            let arr = partition_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Failed to downcast partition column to Int64Array")?;
            for (i, boundary) in boundaries.iter_mut().enumerate().skip(1) {
                if arr.is_null(i) != arr.is_null(i - 1)
                    || (!arr.is_null(i) && arr.value(i) != arr.value(i - 1))
                {
                    *boundary = true;
                }
            }
        }
        DataType::UInt64 => {
            let arr = partition_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Failed to downcast partition column to UInt64Array")?;
            for (i, boundary) in boundaries.iter_mut().enumerate().skip(1) {
                if arr.is_null(i) != arr.is_null(i - 1)
                    || (!arr.is_null(i) && arr.value(i) != arr.value(i - 1))
                {
                    *boundary = true;
                }
            }
        }
        DataType::Utf8 => {
            let arr = partition_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Failed to downcast partition column to StringArray")?;
            for (i, boundary) in boundaries.iter_mut().enumerate().skip(1) {
                if arr.is_null(i) != arr.is_null(i - 1)
                    || (!arr.is_null(i) && arr.value(i) != arr.value(i - 1))
                {
                    *boundary = true;
                }
            }
        }
        dt => {
            // Generic fallback using string representation
            for (i, boundary) in boundaries.iter_mut().enumerate().skip(1) {
                if partition_col.is_null(i) != partition_col.is_null(i - 1) {
                    *boundary = true;
                } else if !partition_col.is_null(i) {
                    // Use Arrow's eq kernel for single-element comparison
                    // This is slower but handles any data type
                    let slice_cur = partition_col.slice(i, 1);
                    let slice_prev = partition_col.slice(i - 1, 1);
                    let eq_result = datafusion::arrow::compute::kernels::cmp::eq(
                        &slice_cur, &slice_prev,
                    )
                    .map_err(|e| {
                        format!(
                            "Failed to compare partition values at row {}: {} (dtype: {:?})",
                            i, e, dt
                        )
                    })?;
                    if !eq_result.value(0) {
                        *boundary = true;
                    }
                }
            }
        }
    }

    Ok(BooleanArray::from(boundaries))
}

/// Build a prefix-sum array that maps each row to its partition index.
/// Used to quickly check if two rows are in the same partition.
fn build_partition_ids(boundaries: &BooleanArray) -> Vec<u64> {
    let n = boundaries.len();
    let mut ids = Vec::with_capacity(n);
    let mut current_id: u64 = 0;
    for i in 0..n {
        if boundaries.value(i) {
            current_id = i as u64; // Use start index as partition ID
        }
        ids.push(current_id);
    }
    ids
}

/// Main entry point: pattern matching over one execution of the table.
///
/// Given a sorted LTSeqTable, N step predicates, and an optional partition_by column,
/// returns a new LTSeqTable containing only the rows where the sequential pattern matches.
///
/// The returned table contains the rows where step 1 matched (i.e., row index i
/// where step1(i), step2(i+1), ..., stepN(i+N-1) all matched within the same partition).
///
/// The plan runs once, in table order (`ordered_df`), and the matching rows
/// are taken from the batches of that same execution (#149). They used to
/// come from a second execution, which scanned the source and ran the
/// upstream plan again, and named rows by their position in the first one:
/// rows with equal sort keys may come back in another order on another run,
/// so a position could name a row the steps never matched. The predicates
/// are evaluated batch by batch, and only their masks and the partition
/// column are joined across batches, so the full rows are never copied into
/// one batch: peak memory is the table plus the matching rows.
///
/// Runs with the GIL released: `lib.rs::search_pattern` deserializes the
/// predicates under the GIL and wraps this call in `gil::detached`.
pub fn search_pattern_impl(
    table: &LTSeqTable,
    py_exprs: &[PyExpr],
    partition_by: Option<String>,
) -> Result<LTSeqTable, LtseqError> {
    let schema = table.require_schema()?;
    let num_steps = py_exprs.len();

    if num_steps == 0 {
        return Err(LtseqError::Validation(
            "search_pattern requires at least one step predicate".into(),
        ));
    }

    // 1. A pattern reads at least one column of the table: one its steps or
    //    partition_by name, or a sort key that orders them.
    let mut referenced_cols = HashSet::new();
    for expr in py_exprs {
        extract_referenced_columns(expr, &mut referenced_cols);
    }
    if let Some(ref part_col) = partition_by {
        referenced_cols.insert(part_col.clone());
    }
    for sk in table.sort_specs.iter() {
        referenced_cols.insert(sk.column.clone());
    }
    if !schema
        .fields()
        .iter()
        .any(|f| referenced_cols.contains(f.name()))
    {
        return Err(LtseqError::Validation(
            "No valid columns found in step predicates".into(),
        ));
    }

    // 2. Execute the plan once, in table order, keeping every column
    let batches: Vec<RecordBatch> = RUNTIME
        .block_on(table.ordered_df()?.collect())
        .map_err(LtseqError::collect)?
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();

    let n: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if n < num_steps {
        return LTSeqTable::empty(
            Arc::clone(&table.session),
            Arc::clone(schema),
            table.sort_specs.clone(),
        );
    }

    // 3. Build column name → index mapping
    let batch_schema = batches[0].schema();
    let name_to_idx: std::collections::HashMap<String, usize> = batch_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name().clone(), i))
        .collect();

    // 4. Compute partition boundaries if needed, over the partition column
    //    joined across batches
    let partition_ids = if let Some(ref part_col) = partition_by {
        let idx = *name_to_idx.get(part_col).ok_or_else(|| {
            LtseqError::Validation(format!(
                "partition_by column '{}' not found",
                part_col
            ))
        })?;
        let column = concat_column(&batches, idx)?;
        let boundaries =
            compute_partition_boundaries(&column).map_err(LtseqError::Runtime)?;
        Some(build_partition_ids(&boundaries))
    } else {
        None
    };

    // 5. Evaluate every step predicate (vectorized, batch by batch), step 1
    //    first: it is the fast rejection filter
    let masks: Vec<BooleanArray> = py_exprs
        .iter()
        .map(|expr| step_mask(expr, &batches, &name_to_idx))
        .collect::<Result<_, _>>()?;
    let (step1_mask, step_masks) = masks.split_first().expect("at least one step");

    // 6. Single-pass scan
    let max_start = n - (num_steps - 1);
    let mut matching_indices: Vec<usize> = Vec::new();
    for i in 0..max_start {
        // Fast rejection: check step 1
        if !step1_mask.is_valid(i) || !step1_mask.value(i) {
            continue;
        }

        // Check partition boundary: all steps must be in same partition
        if let Some(ref pids) = partition_ids {
            if pids[i] != pids[i + num_steps - 1] {
                continue;
            }
        }

        // Check remaining steps
        let all_match = step_masks.iter().enumerate().all(|(step_offset, mask)| {
            let row = i + step_offset + 1;
            mask.is_valid(row) && mask.value(row)
        });
        if all_match {
            matching_indices.push(i);
        }
    }

    // 7. If no matches, return empty table with original schema
    if matching_indices.is_empty() {
        return LTSeqTable::empty(
            Arc::clone(&table.session),
            Arc::clone(schema),
            table.sort_specs.clone(),
        );
    }

    // 8. Take the matching rows, every column, from the batches of this
    //    execution
    let result_batch = take_rows(&batches, &matching_indices)?;

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        vec![result_batch],
        batch_schema,
        table.sort_specs.clone(),
    )
}

/// A column of every batch, joined into one array.
fn concat_column(batches: &[RecordBatch], idx: usize) -> Result<ArrayRef, LtseqError> {
    if let [batch] = batches {
        return Ok(Arc::clone(batch.column(idx)));
    }
    let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(idx).as_ref()).collect();
    concat(&parts).map_err(|e| LtseqError::Runtime(format!("concat failed: {}", e)))
}

/// A step predicate's mask over the rows of all `batches`, in order: the
/// predicate is evaluated on each batch, and only the masks are joined.
fn step_mask(
    expr: &PyExpr,
    batches: &[RecordBatch],
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Result<BooleanArray, LtseqError> {
    let masks = batches
        .iter()
        .map(|batch| eval_predicate(expr, batch, name_to_idx).map_err(LtseqError::Runtime))
        .collect::<Result<Vec<_>, _>>()?;
    if masks.len() == 1 {
        return Ok(masks.into_iter().next().expect("one mask"));
    }
    let parts: Vec<&dyn Array> = masks.iter().map(|m| m as &dyn Array).collect();
    let joined = concat(&parts)
        .map_err(|e| LtseqError::Runtime(format!("concat failed: {}", e)))?;
    Ok(joined
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("masks are boolean")
        .clone())
}

/// The rows at ascending positions `rows` of the batches read as one
/// sequence, every column, as one batch. Each row is taken from the batch
/// that holds it; the batches are not joined first.
fn take_rows(batches: &[RecordBatch], rows: &[usize]) -> Result<RecordBatch, LtseqError> {
    let mut positions = Vec::with_capacity(rows.len());
    let (mut batch_idx, mut batch_start) = (0, 0);
    for &row in rows {
        while row >= batch_start + batches[batch_idx].num_rows() {
            batch_start += batches[batch_idx].num_rows();
            batch_idx += 1;
        }
        positions.push((batch_idx, row - batch_start));
    }

    let schema = batches[0].schema();
    let columns = (0..schema.fields().len())
        .map(|c| {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            interleave(&parts, &positions)
                .map_err(|e| LtseqError::Runtime(format!("take failed: {}", e)))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
    .map_err(|e| LtseqError::Runtime(format!("RecordBatch creation failed: {}", e)))
}

/// Optimized version: return just the count of matching patterns.
/// Avoids collecting the full table — only collects pruned columns.
///
/// Runs with the GIL released: `lib.rs::search_pattern_count` deserializes
/// the predicates under the GIL and wraps this call in `gil::detached`.
pub fn search_pattern_count_impl(
    table: &LTSeqTable,
    py_exprs: &[PyExpr],
    partition_by: Option<String>,
) -> Result<usize, LtseqError> {
    let num_steps = py_exprs.len();

    if num_steps == 0 {
        return Err(LtseqError::Validation(
            "search_pattern requires at least one step predicate".into(),
        ));
    }

    // Try parallel fast path BEFORE creating DataFusion session.
    // This avoids the expensive require_df_and_schema() when the parallel path succeeds.
    if let (Some(ref parquet_path), Some(ref part_col)) =
        (&table.source_parquet_path, &partition_by)
    {
        if !table.sort_specs.is_empty() {
            match crate::ops::parallel_scan::parallel_pattern_match_count(
                table,
                py_exprs,
                part_col,
                parquet_path,
            ) {
                Ok(count) => {
                    return Ok(count);
                }
                Err(e) => {
                    let msg = e.to_string();
                    if !msg.contains("PARALLEL_FALLBACK") {
                        return Err(e);
                    }
                    // Fall through to general path
                }
            }
        }
    }

    // Fallback: use DataFusion path (only reached if parallel path not viable)
    let (df, schema) = table.require_df_and_schema()?;

    // 2. Extract referenced columns for projection pruning
    let mut referenced_cols = HashSet::new();
    for expr in py_exprs {
        extract_referenced_columns(expr, &mut referenced_cols);
    }
    if let Some(ref part_col) = partition_by {
        referenced_cols.insert(part_col.clone());
    }

    // 3. Project only needed columns, add sort, collect
    let sort_keys = &table.sort_specs;

    let batches = collect_projected_sorted(df, &referenced_cols, schema, sort_keys)?;

    if batches.is_empty() {
        return Ok(0);
    }

    let batch_schema = batches[0].schema();
    let combined = concat_batches(&batch_schema, &batches).map_err(|e| {
        LtseqError::Runtime(format!("concat_batches failed: {}", e))
    })?;

    let n = combined.num_rows();
    if n < num_steps {
        return Ok(0);
    }

    // 5. Build column name → index mapping
    let mut name_to_idx = std::collections::HashMap::new();
    for (i, field) in combined.schema().fields().iter().enumerate() {
        name_to_idx.insert(field.name().clone(), i);
    }

    // 6. Compute partition boundaries if needed
    let partition_ids = if let Some(ref part_col) = partition_by {
        let idx = name_to_idx.get(part_col).ok_or_else(|| {
            LtseqError::Validation(format!(
                "partition_by column '{}' not found",
                part_col
            ))
        })?;
        let boundaries = compute_partition_boundaries(combined.column(*idx))
            .map_err(LtseqError::Runtime)?;
        Some(build_partition_ids(&boundaries))
    } else {
        None
    };

    // 7. Evaluate step 1 predicate vectorized (fast rejection filter)
    let step1_mask = eval_predicate(&py_exprs[0], &combined, &name_to_idx)
        .map_err(LtseqError::Runtime)?;

    // 8. Single-pass scan — count only, with lazy per-row evaluation for steps 2..N
    let max_start = n.saturating_sub(num_steps - 1);
    let mut count: usize = 0;

    if num_steps == 1 {
        for i in 0..n {
            if step1_mask.is_valid(i) && step1_mask.value(i) {
                count += 1;
            }
        }
    } else {
        // For multi-step patterns: evaluate steps 2..N lazily per-row
        // instead of vectorized on all 1M rows. Step 1 matches are rare (~0.1%),
        // so we only check steps 2..N on ~1600 rows instead of 1M.
        //
        // The lazy path applies only when steps 2..N are all `starts_with`
        // on the SAME string column — the prefixes are checked against that
        // column, whichever it is (never a hardcoded name).
        let fast_plan = same_string_column_starts_with_plan(&py_exprs[1..]);
        let same_partition = |i: usize| {
            partition_ids
                .as_ref()
                .is_none_or(|pids| pids[i] == pids[i + num_steps - 1])
        };
        let fast_counted = fast_plan.as_ref().and_then(|plan| {
            let col_idx = *name_to_idx.get(plan.column.as_str())?;
            let prefixes: Vec<&str> = plan.prefixes.iter().map(|p| p.as_str()).collect();
            count_prefix_matches_on(
                combined.column(col_idx),
                Some(&step1_mask),
                &prefixes,
                max_start,
                same_partition,
            )
        });

        if let Some(fast_count) = fast_counted {
            count = fast_count;
        } else {
            // Fallback: evaluate all predicates vectorized (original approach)
            let step_masks: Vec<BooleanArray> = py_exprs
                .iter()
                .map(|expr| {
                    eval_predicate(expr, &combined, &name_to_idx)
                        .map_err(LtseqError::Runtime)
                })
                .collect::<Result<Vec<_>, _>>()?;

            for i in 0..max_start {
                if !step_masks[0].is_valid(i) || !step_masks[0].value(i) {
                    continue;
                }
                if let Some(ref pids) = partition_ids {
                    if pids[i] != pids[i + num_steps - 1] {
                        continue;
                    }
                }
                let mut all_match = true;
                for (step_offset, mask) in step_masks[1..].iter().enumerate() {
                    let row = i + step_offset + 1;
                    if !mask.is_valid(row) || !mask.value(row) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Helper: project, sort, and collect a DataFrame using the general (multi-partition) path.
fn collect_projected_sorted(
    df: &Arc<datafusion::dataframe::DataFrame>,
    referenced_cols: &HashSet<String>,
    schema: &Arc<datafusion::arrow::datatypes::Schema>,
    sort_keys: &[crate::SortSpec],
) -> Result<Vec<RecordBatch>, LtseqError> {
    let select_exprs: Vec<Expr> = referenced_cols
        .iter()
        .filter(|col_name| {
            schema
                .fields()
                .iter()
                .any(|f| f.name() == col_name.as_str())
        })
        .map(|col_name| ident(col_name.as_str()))
        .collect();

    if select_exprs.is_empty() {
        return Err(LtseqError::Validation(
            "No valid columns found in step predicates".into(),
        ));
    }

    let proj = (**df)
        .clone()
        .select(select_exprs)
        .map_err(|e| {
            LtseqError::Runtime(format!("Projection failed: {}", e))
        })?;

    let projected_df = if !sort_keys.is_empty() {
        let sort_exprs = build_sort_exprs(sort_keys);
        proj.sort(sort_exprs).map_err(|e| {
            LtseqError::Runtime(format!("Sort failed: {}", e))
        })?
    } else {
        proj
    };

    RUNTIME
        .block_on(projected_df.collect())
        .map_err(LtseqError::collect)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::collections::HashMap;

    fn string_call(func: &str, column: &str, arg: &str) -> PyExpr {
        PyExpr::Call {
            func: func.to_string(),
            args: vec![PyExpr::Literal(LiteralValue::String(arg.to_string()))],
            kwargs: HashMap::new(),
            on: Some(Box::new(PyExpr::Column(column.to_string()))),
        }
    }

    /// Integer `/` is true division in Float64, as on the DataFusion path
    /// (#218), and a zero divisor raises.
    #[test]
    fn integer_div_is_true_division() {
        use datafusion::arrow::array::{Float64Array, Int64Array, UInt64Array};
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), Some(-7), None]));
        let divisors: ArrayRef = Arc::new(UInt64Array::from(vec![Some(2), Some(2), Some(0)]));
        let result = eval_binop("Div", &ints, &divisors).unwrap();
        let result = result.as_any().downcast_ref::<Float64Array>().expect("Float64");
        assert_eq!(result.iter().collect::<Vec<_>>(), [Some(3.5), Some(-3.5), None]);

        let zeros: ArrayRef = Arc::new(Int64Array::from(vec![1, 0, 1]));
        let err = eval_binop("Div", &ints, &zeros).unwrap_err();
        assert!(err.contains("Divide by zero"), "{err}");
    }

    /// A float literal next to an integer column is placed among the
    /// column's values, as the transpiler places it in `filter` (R3-F1):
    /// 2.0**53 + 1 is no Float64, and a cast to Float64 would round
    /// 2**53 + 1 down to 2**53.
    #[test]
    fn float_literal_is_exact_next_to_integers() {
        let n = 1_i64 << 53;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let x: ArrayRef = Arc::new(Int64Array::from(vec![Some(n), Some(n + 1), None]));
        let batch = RecordBatch::try_new(schema, vec![x]).unwrap();
        let idx = HashMap::from([("x".to_string(), 0)]);
        let column = || Box::new(PyExpr::Column("x".to_string()));
        let boundary = || Box::new(PyExpr::Literal(LiteralValue::Float64(n as f64)));
        let mask = |op: &str, reversed: bool| {
            let (left, right) = if reversed {
                (boundary(), column())
            } else {
                (column(), boundary())
            };
            let expr = PyExpr::BinOp { op: op.to_string(), left, right };
            eval_predicate(&expr, &batch, &idx).unwrap().iter().collect::<Vec<_>>()
        };
        assert_eq!(mask("Gt", false), [Some(false), Some(true), None]);
        assert_eq!(mask("Eq", false), [Some(true), Some(false), None]);
        assert_eq!(mask("Lt", true), [Some(false), Some(true), None]);
        assert_eq!(mask("Eq", true), [Some(true), Some(false), None]);
    }

    /// A dictionary-encoded integer operand, on either side, is integer true
    /// division too: the transpiler reads it by its value type (R2-F1).
    #[test]
    fn encoded_integer_div_is_true_division() {
        use datafusion::arrow::array::{DictionaryArray, Float64Array, Int8Array};
        use datafusion::arrow::datatypes::Int8Type;
        let encoded: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), None]),
            Arc::new(Int64Array::from(vec![7, 8, 9])),
        ));
        let twos: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2, 2]));
        let quotient = eval_binop("Div", &encoded, &twos).unwrap();
        let quotient = quotient
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(
            quotient.iter().collect::<Vec<_>>(),
            [Some(3.5), Some(4.0), Some(4.5), None]
        );
        let sevens: ArrayRef = Arc::new(Int64Array::from(vec![7, 7, 7, 7]));
        let reversed = eval_binop("Div", &sevens, &encoded).unwrap();
        let reversed = reversed
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(reversed.value(1), 0.875);
        assert!(reversed.is_null(3));
    }

    /// `x / None` and `None / x` are NULL, as DataFusion types `lit(NULL)`
    /// as its other operand (R2-F2); the NULL literal does not make integer
    /// division an error or a Boolean operation.
    #[test]
    fn null_literal_div_is_null() {
        use datafusion::arrow::array::Float64Array;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(7), Some(0), None]))],
        )
        .expect("valid batch");
        let idx = HashMap::from([("x".to_string(), 0)]);
        let x = || PyExpr::Column("x".to_string());
        let null = || PyExpr::Literal(LiteralValue::Null);
        let binop = |op: &str, left: PyExpr, right: PyExpr| PyExpr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        };
        let call = |func: &str, on: PyExpr| PyExpr::Call {
            func: func.to_string(),
            args: vec![],
            on: Some(Box::new(on)),
            kwargs: Default::default(),
        };
        for quotient in [binop("Div", x(), null()), binop("Div", null(), x())] {
            let values = eval_expr(&quotient, &batch, &idx).expect("NULL quotient");
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64");
            assert_eq!(values.null_count(), 3, "{quotient:?}");
            let is_null = eval_predicate(&call("is_null", quotient.clone()), &batch, &idx).unwrap();
            assert_eq!(is_null.iter().collect::<Vec<_>>(), [Some(true); 3]);
            let gt = eval_predicate(
                &binop("Gt", quotient, PyExpr::Literal(LiteralValue::Int64(1))),
                &batch,
                &idx,
            )
            .unwrap();
            assert_eq!(gt.null_count(), 3);
        }
        let both = binop("Div", null(), null());
        let is_null = eval_predicate(&call("is_null", both), &batch, &idx).unwrap();
        assert_eq!(is_null.iter().collect::<Vec<_>>(), [Some(true); 3]);
        let compared = eval_predicate(&binop("Eq", x(), null()), &batch, &idx).unwrap();
        assert_eq!(compared.null_count(), 3);
        let is_not_null = eval_predicate(&call("is_not_null", null()), &batch, &idx).unwrap();
        assert_eq!(is_not_null.iter().collect::<Vec<_>>(), [Some(false); 3]);
        let bare = eval_predicate(&null(), &batch, &idx).unwrap();
        assert_eq!(bare.null_count(), 3);
    }

    /// The same values in each Arrow string layout, plus an Int64 column.
    fn string_batch() -> (RecordBatch, HashMap<String, usize>) {
        let values = vec![Some("apple"), None, Some("pineapple"), Some("app")];
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("utf8", Arc::new(StringArray::from(values.clone()))),
            ("large", Arc::new(LargeStringArray::from(values.clone()))),
            ("view", Arc::new(StringViewArray::from(values))),
            ("int", Arc::new(Int64Array::from(vec![1, 2, 3, 4]))),
        ];
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            columns.into_iter().map(|(_, arr)| arr).collect(),
        )
        .expect("valid string batch");
        let idx = (0..batch.num_columns())
            .map(|i| (batch.schema().field(i).name().clone(), i))
            .collect();
        (batch, idx)
    }

    /// Every string test gives the same answer on all three string layouts,
    /// and NULL input stays NULL.
    #[test]
    fn string_predicates_agree_across_string_layouts() {
        let (batch, idx) = string_batch();
        let cases = [
            (
                "starts_with",
                "app",
                [Some(true), None, Some(false), Some(true)],
            ),
            (
                "ends_with",
                "le",
                [Some(true), None, Some(true), Some(false)],
            ),
            (
                "contains",
                "pple",
                [Some(true), None, Some(true), Some(false)],
            ),
        ];
        for (func, arg, expected) in cases {
            for column in ["utf8", "large", "view"] {
                let result = eval_predicate(&string_call(func, column, arg), &batch, &idx)
                    .expect("string predicate");
                assert_eq!(
                    result.iter().collect::<Vec<_>>(),
                    expected,
                    "{func}({arg}) on {column}"
                );
            }
        }
    }

    /// `&`, `|` and `~` agree with the Kleene truth tables over every pair of
    /// {TRUE, FALSE, NULL} (#220), with NULL from a column and from a `None`
    /// literal. The model ranks FALSE < NULL < TRUE: AND is the minimum, OR
    /// the maximum, and NOT the reflection.
    #[test]
    fn boolean_operators_are_kleene() {
        const TRUTH: [Option<bool>; 3] = [Some(false), None, Some(true)];
        fn rank(v: Option<bool>) -> usize {
            TRUTH.iter().position(|&t| t == v).expect("a truth value")
        }
        /// The value of `expr` on a row where `a` is `x` and `b` is `y`.
        fn model(expr: &PyExpr, x: Option<bool>, y: Option<bool>) -> Option<bool> {
            match expr {
                PyExpr::Column(name) if name == "a" => x,
                PyExpr::Column(_) => y,
                PyExpr::Literal(LiteralValue::Null) => None,
                PyExpr::UnaryOp { operand, .. } => TRUTH[2 - rank(model(operand, x, y))],
                PyExpr::BinOp { op, left, right } => {
                    let (l, r) = (rank(model(left, x, y)), rank(model(right, x, y)));
                    TRUTH[if op == "And" { l.min(r) } else { l.max(r) }]
                }
                other => panic!("outside the model: {other:?}"),
            }
        }
        fn binop(op: &str, left: PyExpr, right: PyExpr) -> PyExpr {
            PyExpr::BinOp {
                op: op.to_string(),
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        let and = |left, right| binop("And", left, right);
        let or = |left, right| binop("Or", left, right);
        let not = |operand: PyExpr| PyExpr::UnaryOp {
            op: "Not".to_string(),
            operand: Box::new(operand),
        };
        let a = || PyExpr::Column("a".to_string());
        let b = || PyExpr::Column("b".to_string());
        let null = || PyExpr::Literal(LiteralValue::Null);

        let pairs: Vec<_> = TRUTH
            .iter()
            .flat_map(|&x| TRUTH.iter().map(move |&y| (x, y)))
            .collect();
        let a_values: BooleanArray = pairs.iter().map(|&(x, _)| x).collect();
        let b_values: BooleanArray = pairs.iter().map(|&(_, y)| y).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Boolean, true),
                Field::new("b", DataType::Boolean, true),
            ])),
            vec![Arc::new(a_values), Arc::new(b_values)],
        )
        .expect("valid boolean batch");
        let idx = HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]);

        let cases = [
            and(a(), b()),
            or(a(), b()),
            not(and(a(), b())),
            not(or(a(), b())),
            or(not(a()), b()),
            and(or(a(), b()), not(a())),
            and(a(), null()),
            or(null(), b()),
            not(and(a(), null())),
            or(null(), null()),
            not(null()),
            and(not(null()), b()),
        ];
        for expr in cases {
            let result = eval_predicate(&expr, &batch, &idx).expect("boolean predicate");
            let expected: Vec<_> = pairs.iter().map(|&(x, y)| model(&expr, x, y)).collect();
            assert_eq!(result.iter().collect::<Vec<_>>(), expected, "{expr:?}");
        }
    }

    #[test]
    fn string_predicate_on_non_string_column_names_the_operation() {
        let (batch, idx) = string_batch();
        for func in ["starts_with", "ends_with", "contains"] {
            let err = eval_predicate(&string_call(func, "int", "1"), &batch, &idx)
                .expect_err("non-string column");
            assert_eq!(err, format!("{func} requires string column, got Int64"));
        }
    }

    /// Float partition keys follow §18: `-0.0` equals `0.0` and every NaN
    /// equals every NaN, a NULL equals only NULL (#289). Plain and
    /// dictionary-encoded columns split at the same rows.
    #[test]
    fn float_partition_keys_follow_contract_equality() {
        use datafusion::arrow::array::{DictionaryArray, Float32Array, Float64Array, Int32Array};

        let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let keys = [
            Some(0.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(-f64::NAN),
            Some(other_nan),
            None,
            None,
            Some(-0.0),
            Some(1.5),
        ];
        let expected = vec![true, false, false, true, false, false, true, false, true, true];
        let boundaries = |col: ArrayRef| {
            let b = compute_partition_boundaries(&col).expect("boundaries");
            (0..b.len()).map(|i| b.value(i)).collect::<Vec<_>>()
        };

        let f64s: ArrayRef = Arc::new(Float64Array::from(keys.to_vec()));
        assert_eq!(boundaries(Arc::clone(&f64s)), expected, "float64");

        let f32s: ArrayRef = Arc::new(Float32Array::from(
            keys.iter().map(|k| k.map(|v| v as f32)).collect::<Vec<_>>(),
        ));
        assert_eq!(boundaries(f32s), expected, "float32");

        // NULL rows are NULL keys, the usual encoding of a NULL value.
        let dict_keys = Int32Array::from(
            keys.iter()
                .zip(0..)
                .map(|(k, i)| k.map(|_| i))
                .collect::<Vec<Option<i32>>>(),
        );
        let dict: ArrayRef =
            Arc::new(DictionaryArray::try_new(dict_keys, f64s).expect("dictionary"));
        assert_eq!(boundaries(dict), expected, "dictionary of float64");
    }

    /// A dictionary or run-end encoded float compares as its decoded column
    /// does, on either side: `-0.0` equals `0.0` and neither orders below
    /// the other (#292). Arrow's kernels alone decide an encoded zero by its
    /// sign.
    #[test]
    fn encoded_float_comparisons_follow_the_decoded_column() {
        use datafusion::arrow::array::{DictionaryArray, Float64Array, Int32Array, RunArray};
        use datafusion::arrow::datatypes::Int32Type;

        let (z, n) = (Some(0.0), Some(-0.0));
        let left = [z, n, n, Some(1.0), None, Some(-1.0)];
        let right = [n, z, n, n, z, n];
        let plain =
            |values: &[Option<f64>]| -> ArrayRef { Arc::new(Float64Array::from(values.to_vec())) };
        let dictionary = |values: &[Option<f64>]| -> ArrayRef {
            let keys = Int32Array::from((0..values.len() as i32).map(Some).collect::<Vec<_>>());
            Arc::new(DictionaryArray::try_new(keys, plain(values)).expect("dictionary"))
        };
        let run_end_encoded = |values: &[Option<f64>]| -> ArrayRef {
            let run_ends = Int32Array::from((1..=values.len() as i32).collect::<Vec<_>>());
            Arc::new(
                RunArray::<Int32Type>::try_new(&run_ends, plain(values).as_ref())
                    .expect("run-end encoded"),
            )
        };
        let mask = |op: &str, l: &ArrayRef, r: &ArrayRef| {
            let result = eval_binop(op, l, r).expect("comparison");
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean");
            result.iter().collect::<Vec<_>>()
        };
        type Encode<'a> = &'a dyn Fn(&[Option<f64>]) -> ArrayRef;
        let encodings: [(&str, Encode); 3] = [
            ("plain", &plain),
            ("dictionary", &dictionary),
            ("run-end encoded", &run_end_encoded),
        ];
        for op in ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"] {
            let reference = mask(op, &plain(&left), &plain(&right));
            for ((l_name, l), (r_name, r)) in encodings
                .iter()
                .flat_map(|l| encodings.iter().map(move |r| (l, r)))
            {
                assert_eq!(
                    mask(op, &l(&left), &r(&right)),
                    reference,
                    "{l_name} {op} {r_name}"
                );
            }
        }
        let (t, f) = (Some(true), Some(false));
        assert_eq!(
            mask("Eq", &dictionary(&left), &plain(&right)),
            [t, t, t, f, None, f]
        );
    }

    /// A source that counts how often its plan is executed.
    #[derive(Debug)]
    struct CountingSource {
        schema: datafusion::arrow::datatypes::SchemaRef,
        batches: Vec<RecordBatch>,
        executions: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl datafusion::physical_plan::streaming::PartitionStream for CountingSource {
        fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
            &self.schema
        }

        fn execute(
            &self,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::execution::SendableRecordBatchStream {
            self.executions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let rows = self.batches.clone().into_iter().map(Ok);
            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures_util::stream::iter(rows),
                ),
            )
        }
    }

    /// Rows `(k, p, flag, id)`, read through a [`CountingSource`] as one
    /// batch per inner slice, by a session that emits batches of 3 rows.
    fn counting_table(
        batches: &[&[(i64, i64, i64, i64)]],
        sort_by_k: bool,
    ) -> (LTSeqTable, Arc<std::sync::atomic::AtomicUsize>) {
        use datafusion::catalog::streaming::StreamingTable;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("p", DataType::Int64, false),
            Field::new("flag", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let batches: Vec<RecordBatch> = batches
            .iter()
            .map(|rows| {
                let column = |f: fn(&(i64, i64, i64, i64)) -> i64| -> ArrayRef {
                    Arc::new(Int64Array::from(rows.iter().map(f).collect::<Vec<_>>()))
                };
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        column(|r| r.0),
                        column(|r| r.1),
                        column(|r| r.2),
                        column(|r| r.3),
                    ],
                )
                .expect("batch")
            })
            .collect();
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = StreamingTable::try_new(
            Arc::clone(&schema),
            vec![Arc::new(CountingSource {
                schema: Arc::clone(&schema),
                batches,
                executions: Arc::clone(&executions),
            })],
        )
        .expect("source");
        let session = Arc::new(SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(3)
                .with_target_partitions(2),
        ));
        let df = session.read_table(Arc::new(source)).expect("read");
        let sort_specs = if sort_by_k {
            vec![crate::SortSpec {
                column: "k".into(),
                descending: false,
                nulls_first: false,
                float_key: false,
            }]
        } else {
            Vec::new()
        };
        let table = LTSeqTable::from_df_with_schema(session, df, schema, sort_specs, None);
        (table, executions)
    }

    fn column_values(table: &LTSeqTable, name: &str) -> Vec<i64> {
        let batches = RUNTIME
            .block_on(table.ordered_df().expect("plan").collect())
            .expect("collect");
        batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of(name).expect("column");
                let arr = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .clone();
                (0..arr.len()).map(move |i| arr.value(i))
            })
            .collect()
    }

    fn flag_is(value: i64) -> PyExpr {
        PyExpr::BinOp {
            op: "Eq".into(),
            left: Box::new(PyExpr::Column("flag".into())),
            right: Box::new(PyExpr::Literal(LiteralValue::Int64(value))),
        }
    }

    /// `search_pattern` executes the table's plan once, and takes every
    /// column of its matches, in table order, from the batches of that
    /// execution, matches across batch and partition boundaries included
    /// (#149).
    #[test]
    fn search_pattern_executes_the_plan_once() {
        // In k order: k, p, flag, id. Steps flag == 1, flag == 2 match at
        // k = 0, 3 (across p 1 → 2 unless partitioned), 5 and 8.
        let rows: Vec<(i64, i64, i64, i64)> = [
            (1, 1),
            (1, 2),
            (1, 0),
            (1, 1),
            (2, 2),
            (2, 1),
            (2, 2),
            (3, 0),
            (3, 1),
            (3, 2),
        ]
        .iter()
        .enumerate()
        .map(|(k, &(p, flag))| (k as i64, p, flag, 100 + k as i64))
        .collect();
        let row = |k: usize| rows[k];
        let steps = [flag_is(1), flag_is(2)];

        // Out of order in the source, so the plan sorts.
        let shuffled: [&[_]; 3] = [
            &[row(5), row(9), row(0)],
            &[row(3), row(8), row(1), row(6)],
            &[row(2), row(7), row(4)],
        ];
        for (partition_by, expected) in
            [(None, vec![100, 103, 105, 108]), (Some("p"), vec![100, 105, 108])]
        {
            let (table, executions) = counting_table(&shuffled, true);
            let result =
                search_pattern_impl(&table, &steps, partition_by.map(String::from))
                    .expect("search_pattern");
            assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(column_values(&result, "id"), expected, "{partition_by:?}");
            assert_eq!(column_values(&result, "flag"), vec![1; expected.len()]);
        }

        // In table order in the source, split so that matches cross batches.
        let in_order: [&[_]; 3] = [&rows[0..4], &rows[4..6], &rows[6..10]];
        let (table, executions) = counting_table(&in_order, false);
        let result = search_pattern_impl(&table, &steps, Some("p".into())).expect("search");
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(column_values(&result, "id"), vec![100, 105, 108]);
        assert_eq!(column_values(&result, "k"), vec![0, 5, 8]);
    }
}
