//! Single-pass linear scan engine for `group_ordered` with boundary predicates.
//!
//! For predicates like:
//!   `(r.userid != r.userid.shift(1)) | (r.eventtime - r.eventtime.shift(1) > 1800)`
//!
//! Instead of multiple DataFusion passes (LAG window → boundary → SUM window → group_id),
//! this evaluates the predicate in a single O(n) scan:
//!   - Row 0: always a new group (boundary = true)
//!   - Row i: evaluate predicate using row[i] for column refs and row[i-1] for shift(1)
//!   - Increment group_id on each boundary
//!
//! Then computes `__group_count__` and `__rn__` in a second O(n) pass.
//!
//! # Supported Expression Subset
//!
//! | PyExpr | Evaluation |
//! |--------|-----------|
//! | `Column("x")` | Read column value at current row |
//! | `Call { func: "shift", on: Column("x"), args: [1] }`, no kwargs | Read column value at previous row |
//! | `Call { func: "is_null" / "is_not_null", on: expr }` | Check if evaluated value is (not) null |
//! | `BinOp { op: Ne/Eq/Gt/Lt/Ge/Le }` | Compare two values |
//! | `BinOp { op: Or/And }` | Logical combination |
//! | `BinOp { op: Add/Sub/Mul/Div }` | Arithmetic |
//! | `Literal` (Null, Boolean, Int64, Float64, String) | Constant value |
//! | `UnaryOp { op: "Not" }` | Logical negation |

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::types::{LiteralValue, PyExpr};
use crate::LTSeqTable;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, SortExpr};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ============================================================================
// Expression eligibility check
// ============================================================================

/// Check if a PyExpr can be evaluated by the linear scan engine on a table
/// of `schema`.
///
/// Returns true if the expression tree:
/// 1. Contains at least one shift(1) call (otherwise, it's a simple column/expression
///    and should use the standard DataFusion IS DISTINCT FROM LAG path)
/// 2. Only contains operations we support: Column, shift(1) without keyword
///    arguments, is_null, is_not_null, BinOp, UnaryOp, and literals of the
///    kinds the evaluator has values for (`Value`)
/// 3. Computes, at every node, what DataFusion computes for it on these
///    column types ([`kernel_type`]), so the count equals the materialized
///    reference. NULL handling still differs (#189).
pub fn can_linear_scan(expr: &PyExpr, schema: &ArrowSchema) -> bool {
    is_supported_expr(expr)
        && contains_shift(expr)
        && kernel_type(expr, schema) == Some(DataType::Boolean)
}

/// The type DataFusion gives `expr` on a table of `schema`, when the kernel
/// computes the same values for it; `None` when it does not.
///
/// The kernel compares and subtracts integers as `i64`, timestamps as raw
/// ticks, and has no integer/float coercion. DataFusion instead coerces both
/// operands to a common type (`BinaryTypeCoercer`, the rule its analyzer
/// uses) and computes there. The two agree when:
///
/// - a comparison is between integers that `i64` holds exactly (`Int64`,
///   `Int32`, `UInt32`), between timestamps of one type, between `UInt64`s
///   for `==`/`!=` (equal bit patterns), between `Float64`s for the operators
///   the kernel has float arms for (`==`, `!=`, `>`), or between strings for
///   `!=`; or an integer is compared `>` with an integral float literal
///   below 2^53, which the fused path reads as that integer (#145 PR-4),
///   or with a NULL literal (NULL in both);
/// - arithmetic is computed by DataFusion in `Int64` (`+ - * /`) or `Float64`
///   (`-`). `Int32`/`UInt32` arithmetic DataFusion computes in 32 bits,
///   wrapping (`3 - 5` is 4294967294 in `UInt32`), timestamp arithmetic
///   gives a duration, and `UInt64` past `i64::MAX` is negative to the kernel.
fn kernel_type(expr: &PyExpr, schema: &ArrowSchema) -> Option<DataType> {
    use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
    use DataType as T;
    let exact_integer = |t: &DataType| matches!(t, T::Int64 | T::Int32 | T::UInt32);
    match expr {
        PyExpr::Column(name) => {
            let t = schema.field_with_name(name).ok()?.data_type().clone();
            let supported = exact_integer(&t)
                || matches!(t, T::UInt64 | T::Float64 | T::Boolean | T::Utf8 | T::Timestamp(..));
            supported.then_some(t)
        }
        PyExpr::Literal(value) => match value {
            LiteralValue::Int64(_) => Some(T::Int64),
            LiteralValue::Float64(_) => Some(T::Float64),
            LiteralValue::Boolean(_) => Some(T::Boolean),
            LiteralValue::String(_) => Some(T::Utf8),
            // `x > None` is NULL in both (#145 R6-14).
            LiteralValue::Null => Some(T::Null),
            _ => None,
        },
        PyExpr::Call { func, on, .. } => {
            let on_type = kernel_type(on.as_deref()?, schema)?;
            match func.as_str() {
                "shift" => Some(on_type),
                "is_null" | "is_not_null" => Some(T::Boolean),
                _ => None,
            }
        }
        PyExpr::UnaryOp { op, operand } => {
            (op == "Not" && kernel_type(operand, schema)? == T::Boolean).then_some(T::Boolean)
        }
        PyExpr::Alias { expr, .. } => kernel_type(expr, schema),
        PyExpr::Window { .. } => None,
        PyExpr::BinOp { op, left, right } => {
            let (l, r) = (kernel_type(left, schema)?, kernel_type(right, schema)?);
            match op.as_str() {
                "And" | "Or" => (l == T::Boolean && r == T::Boolean).then_some(T::Boolean),
                "Eq" | "Ne" | "Gt" | "Lt" | "Ge" | "Le" => {
                    let same = |t: &DataType| l == *t && r == *t;
                    let agrees = (exact_integer(&l) && exact_integer(&r))
                        || (matches!(l, T::Timestamp(..)) && l == r)
                        || (same(&T::UInt64) && matches!(op.as_str(), "Eq" | "Ne"))
                        || (same(&T::Float64) && matches!(op.as_str(), "Eq" | "Ne" | "Gt"))
                        || (same(&T::Utf8) && op == "Ne")
                        || (exact_integer(&l) && r == T::Null)
                        || (l == T::Null && exact_integer(&r))
                        || (exact_integer(&l) && op == "Gt" && get_literal_i64(right).is_some());
                    agrees.then_some(T::Boolean)
                }
                "Add" | "Sub" | "Mul" | "Div" => {
                    let operator = match crate::transpiler::parse_binary_op(op).ok()? {
                        crate::transpiler::BinaryOp::Native(operator) => operator,
                        crate::transpiler::BinaryOp::FloorDiv => return None,
                    };
                    match BinaryTypeCoercer::new(&l, &operator, &r).get_input_types().ok()? {
                        (T::Int64, T::Int64) => Some(T::Int64),
                        (T::Float64, T::Float64) if op == "Sub" => Some(T::Float64),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
    }
}

/// Check if the expression tree contains at least one shift() call.
fn contains_shift(expr: &PyExpr) -> bool {
    match expr {
        PyExpr::Column(_) => false,
        PyExpr::Literal { .. } => false,
        PyExpr::BinOp { left, right, .. } => contains_shift(left) || contains_shift(right),
        PyExpr::UnaryOp { operand, .. } => contains_shift(operand),
        PyExpr::Call { func, on, .. } => {
            if func == "shift" {
                true
            } else {
                on.as_deref().is_some_and(contains_shift)
            }
        }
        PyExpr::Window { .. } => false,
        PyExpr::Alias { expr, .. } => contains_shift(expr),
    }
}

/// Binary operators admitted by `is_supported_expr`. Each one must have an
/// arm in `vectorized_binop`: an admitted operator the evaluator rejects
/// fails the whole linear scan at run time.
///
/// `Mod` and `FloorDiv` are left out on purpose. A predicate that is not
/// admitted is counted by the DataFusion path, which is the reference; one
/// that is admitted is counted by `vectorized_binop`, whose NULL and UInt64
/// handling still disagrees with that reference (#189). Admit them once the
/// evaluator matches it.
const SUPPORTED_BINARY_OPS: [&str; 12] = [
    "Ne", "Eq", "Gt", "Lt", "Ge", "Le", "Or", "And", "Add", "Sub", "Mul", "Div",
];

/// Check if all nodes in the expression tree are supported by the linear scan evaluator.
fn is_supported_expr(expr: &PyExpr) -> bool {
    match expr {
        PyExpr::Column(_) => true,
        // Decimal, date and timestamp literals have no `Value`; the
        // DataFusion path evaluates predicates that use them.
        PyExpr::Literal(value) => literal_to_value(value).is_some(),
        PyExpr::BinOp { op, left, right } => {
            SUPPORTED_BINARY_OPS.contains(&op.as_str())
                && is_supported_expr(left)
                && is_supported_expr(right)
        }
        PyExpr::UnaryOp { op, operand } => op == "Not" && is_supported_expr(operand),
        PyExpr::Call {
            func,
            args,
            kwargs,
            on,
        } => {
            match func.as_str() {
                "shift" => {
                    // Only support shift(1) on a column reference
                    if !matches!(on.as_deref(), Some(PyExpr::Column(_))) {
                        return false;
                    }
                    // The only argument must be the integer literal 1, with no
                    // keyword arguments: the evaluator reads the previous row of
                    // the whole table, so it has no `partition_by`, and it does
                    // not check a `default` against the column (decision D-c on
                    // #225). The reference evaluates both.
                    kwargs.is_empty()
                        && matches!(args.as_slice(), [PyExpr::Literal(LiteralValue::Int64(1))])
                }
                "is_null" | "is_not_null" => {
                    // is_null() / is_not_null() (also what `== None` /
                    // `!= None` build) on a supported sub-expression
                    on.as_deref().is_some_and(is_supported_expr)
                }
                _ => false,
            }
        }
        PyExpr::Window { .. } => false,
        PyExpr::Alias { expr, .. } => is_supported_expr(expr),
    }
}

// ============================================================================
// Value type for the mini-evaluator
// ============================================================================

/// Runtime value during linear scan evaluation.
#[derive(Debug, Clone)]
enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Str(String),
}

/// The evaluator's value for a literal, or `None` for the kinds it has no
/// value for: Decimals, dates and timestamps. An integral Decimal is not
/// an `Int64` here: the evaluator would divide by it as integers, where
/// DataFusion keeps the fraction (`x / Decimal("2")`).
fn literal_to_value(value: &LiteralValue) -> Option<Value> {
    match value {
        LiteralValue::Null => Some(Value::Null),
        LiteralValue::Boolean(v) => Some(Value::Bool(*v)),
        LiteralValue::Int64(v) => Some(Value::Int64(*v)),
        LiteralValue::Float64(v) => Some(Value::Float64(*v)),
        LiteralValue::String(v) => Some(Value::Str(v.clone())),
        LiteralValue::Decimal128 { .. } | LiteralValue::Date32(_) | LiteralValue::Timestamp { .. } => None,
    }
}

// ============================================================================
// Column extraction from expression tree
// ============================================================================

/// Extract all column names referenced in a PyExpr (including inside shift() calls).
pub(crate) fn extract_referenced_columns(expr: &PyExpr, cols: &mut HashSet<String>) {
    match expr {
        PyExpr::Column(name) => {
            cols.insert(name.clone());
        }
        PyExpr::Literal { .. } => {}
        PyExpr::BinOp { left, right, .. } => {
            extract_referenced_columns(left, cols);
            extract_referenced_columns(right, cols);
        }
        PyExpr::UnaryOp { operand, .. } => {
            extract_referenced_columns(operand, cols);
        }
        PyExpr::Call { on, args, .. } => {
            if let Some(on) = on {
                extract_referenced_columns(on, cols);
            }
            for arg in args {
                extract_referenced_columns(arg, cols);
            }
        }
        PyExpr::Window { expr, .. } => {
            extract_referenced_columns(expr, cols);
        }
        PyExpr::Alias { expr, .. } => {
            extract_referenced_columns(expr, cols);
        }
    }
}

// ============================================================================
// Fused boundary evaluator — one pass, no intermediate arrays
// ============================================================================

/// Boundary flags for `batch` from a fused single-pass evaluation, or `None`
/// if `expr` has no fused form (callers then use `vectorized_eval_expr` or
/// fall back to the general path).
///
/// Fused shapes, combined with `|` and `&`:
/// - `Column != Column.shift(1)` → `arr[i] != arr[i-1]`
/// - `(Column - Column.shift(1)) > Literal` → `arr[i] - arr[i-1] > threshold`
///
/// `prev` is the row just before `batch` in the sequence, as a one-row batch
/// with the same schema, or `None` when `batch` starts the sequence. Row 0
/// is compared against `prev`, so a sequence evaluated batch by batch gets
/// the same flags as the concatenated batch; with no `prev`, row 0 is a
/// boundary. A NULL on either side of a comparison is a boundary, as on the
/// DataFusion path, where a NULL predicate starts a new group. Because each
/// leaf maps NULL to `true` and `&`/`|` are monotone, combining the leaves
/// as plain `bool`s agrees with evaluating the predicate in SQL three-valued
/// logic and then counting NULL as a boundary.
pub(crate) fn fused_boundaries(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    prev: Option<&RecordBatch>,
) -> Option<Vec<bool>> {
    if batch.num_rows() == 0 {
        return Some(Vec::new());
    }
    let mut out = vec![false; batch.num_rows()];
    fuse_eval(expr, batch, name_to_idx, prev, &mut out).then_some(out)
}

/// Recursively fuse-evaluate `expr` into `out` (one flag per row of `batch`).
/// Returns false if the expression can't be fused.
fn fuse_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    prev: Option<&RecordBatch>,
    out: &mut [bool],
) -> bool {
    match expr {
        // Pattern: expr1 | expr2, expr1 & expr2
        PyExpr::BinOp { op, left, right } if op == "Or" || op == "And" => {
            let mut left_out = vec![false; out.len()];
            if !fuse_eval(left, batch, name_to_idx, prev, &mut left_out)
                || !fuse_eval(right, batch, name_to_idx, prev, out)
            {
                return false;
            }
            if op == "Or" {
                for (o, l) in out.iter_mut().zip(left_out) {
                    *o |= l;
                }
            } else {
                for (o, l) in out.iter_mut().zip(left_out) {
                    *o &= l;
                }
            }
            true
        }
        // Pattern: Column != Column.shift(1)
        PyExpr::BinOp { op, left, right } if op == "Ne" => match shifted_column(left, right) {
            Some(name) => fuse_shifted_compare(name, batch, name_to_idx, prev, out, |cur, prev| {
                cur != prev
            }),
            None => false,
        },
        // Pattern: (Column - Column.shift(1)) > Literal
        PyExpr::BinOp { op, left, right } if op == "Gt" => {
            let PyExpr::BinOp {
                op: sub_op,
                left: sub_left,
                right: sub_right,
            } = left.as_ref()
            else {
                return false;
            };
            match (sub_op == "Sub", shifted_column(sub_left, sub_right), get_literal_i64(right)) {
                (true, Some(name), Some(threshold)) => {
                    fuse_shifted_compare(name, batch, name_to_idx, prev, out, |cur, prev| {
                        cur.wrapping_sub(prev) > threshold
                    })
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// The leaf both fused shapes share: `out[i] = differs(c[i], c[i-1])` over
/// column `name` coerced to i64, where `c[-1]` is `prev`'s row. Row 0 with
/// no `prev` is a boundary, and so is any row where either value is NULL.
/// Returns false if the column is missing or not i64-coercible.
fn fuse_shifted_compare(
    name: &str,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    prev: Option<&RecordBatch>,
    out: &mut [bool],
    differs: impl Fn(i64, i64) -> bool,
) -> bool {
    let Some(&idx) = name_to_idx.get(name) else {
        return false;
    };
    let Some(cur) = coerce_to_i64(batch.column(idx)) else {
        return false;
    };
    let vals = cur.values();

    out[0] = match prev {
        None => true,
        Some(prev) => {
            let Some(prev) = coerce_to_i64(prev.column(idx)) else {
                return false;
            };
            cur.is_null(0) || prev.is_null(0) || differs(vals[0], prev.value(0))
        }
    };

    if let Some(nb) = cur.nulls() {
        for i in 1..out.len() {
            out[i] = !nb.is_valid(i) || !nb.is_valid(i - 1) || differs(vals[i], vals[i - 1]);
        }
    } else {
        for i in 1..out.len() {
            out[i] = differs(vals[i], vals[i - 1]);
        }
    }
    true
}

/// The column name if `right` is `shift(...)` of the same column `left` is.
fn shifted_column<'a>(left: &'a PyExpr, right: &PyExpr) -> Option<&'a str> {
    let PyExpr::Column(name) = left else {
        return None;
    };
    match right {
        PyExpr::Call { func, on, .. } if func == "shift" => match on.as_deref() {
            Some(PyExpr::Column(shifted)) if shifted == name => Some(name.as_str()),
            _ => None,
        },
        _ => None,
    }
}

/// An integer threshold for the fused evaluator: an `Int64`, or a `Float64`
/// that is a finite integer below 2^53 in magnitude (truncating -0.5 to 0
/// would change `diff > -0.5`, NaN/Inf have no integer meaning, and from 2^53
/// on the Float64 reference rounds the Int64 diff, so an exact i64
/// comparison would disagree with it). A string is not a number, so `> "1"`
/// has no fused form; a Decimal never reaches here (`can_linear_scan`).
fn get_literal_i64(expr: &PyExpr) -> Option<i64> {
    match expr {
        PyExpr::Literal(LiteralValue::Int64(v)) => Some(*v),
        PyExpr::Literal(LiteralValue::Float64(f)) => {
            (f.is_finite() && f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0)
                .then_some(*f as i64)
        }
        _ => None,
    }
}

// ============================================================================
// Vectorized boundary evaluator — Arrow compute kernels, intermediate arrays
// ============================================================================

/// Boundary flags for the general path's single concatenated batch: the
/// fused evaluator when `expr` has a fused form, otherwise a multi-pass
/// vectorized evaluation. Row 0 is always a boundary, and so is a NULL
/// predicate result.
fn boundary_flags(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
) -> Result<Vec<bool>, String> {
    match fused_boundaries(expr, batch, name_to_idx, None) {
        Some(fused) => Ok(fused),
        None => vectorized_boundary_flags(expr, batch, name_to_idx),
    }
}

/// The multi-pass half of `boundary_flags`, for predicates with no fused form.
fn vectorized_boundary_flags(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
) -> Result<Vec<bool>, String> {
    let arr = vectorized_eval_expr(expr, batch, name_to_idx)?;
    let bool_arr = arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or("Vectorized evaluation did not produce a BooleanArray")?;
    Ok((0..bool_arr.len())
        .map(|i| i == 0 || bool_arr.is_null(i) || bool_arr.value(i))
        .collect())
}

/// Recursively evaluate a PyExpr into an ArrayRef using Arrow compute.
fn vectorized_eval_expr(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
) -> Result<ArrayRef, String> {
    use datafusion::arrow::array::new_null_array;
    
    match expr {
        PyExpr::Column(name) => {
            let idx = name_to_idx.get(name).ok_or_else(|| format!("Column '{}' not found", name))?;
            Ok(Arc::clone(batch.column(*idx)))
        }
        
        PyExpr::Literal(value) => {
            let n = batch.num_rows();
            let val = literal_to_value(value)
                .ok_or_else(|| format!("Unsupported literal in linear scan: {value}"))?;
            match val {
                Value::Int64(v) => Ok(Arc::new(Int64Array::from(vec![v; n])) as ArrayRef),
                Value::Float64(v) => Ok(Arc::new(Float64Array::from(vec![v; n])) as ArrayRef),
                Value::Bool(v) => Ok(Arc::new(BooleanArray::from(vec![v; n])) as ArrayRef),
                Value::Str(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); n])) as ArrayRef),
                Value::Null => Ok(new_null_array(&DataType::Int64, n)),
            }
        }
        
        PyExpr::Call { func, on, .. } => {
            let on = crate::transpiler::require_on(on.as_deref(), func)?;
            match func.as_str() {
                "shift" => {
                    // shift(1): prepend null, drop last element
                    let source = vectorized_eval_expr(on, batch, name_to_idx)?;
                    shift_array_by_1(&source)
                }
                "is_null" | "is_not_null" => {
                    let source = vectorized_eval_expr(on, batch, name_to_idx)?;
                    let want_null = func == "is_null";
                    let n = source.len();
                    let mut result = Vec::with_capacity(n);
                    for i in 0..n {
                        result.push(source.is_null(i) == want_null);
                    }
                    Ok(Arc::new(BooleanArray::from(result)) as ArrayRef)
                }
                _ => Err(format!("Unsupported function: {}", func)),
            }
        }
        
        PyExpr::BinOp { op, left, right } => {
            let left_arr = vectorized_eval_expr(left, batch, name_to_idx)?;
            let right_arr = vectorized_eval_expr(right, batch, name_to_idx)?;
            vectorized_binop(op, &left_arr, &right_arr)
        }
        
        PyExpr::UnaryOp { op, operand } => {
            if op == "Not" {
                let source = vectorized_eval_expr(operand, batch, name_to_idx)?;
                if let Some(bool_arr) = source.as_any().downcast_ref::<BooleanArray>() {
                    use datafusion::arrow::compute::kernels::boolean;
                    Ok(Arc::new(boolean::not(bool_arr).map_err(|e| e.to_string())?) as ArrayRef)
                } else {
                    Err("NOT requires a boolean array".to_string())
                }
            } else {
                Err(format!("Unsupported unary op: {}", op))
            }
        }
        
        PyExpr::Window { .. } => Err("Window expressions not supported in vectorized eval".to_string()),
        PyExpr::Alias { expr, .. } => vectorized_eval_expr(expr, batch, name_to_idx),
    }
}

/// Shift an array by 1 position (prepend null, drop last).
/// Optimized for Int64/Timestamp types to avoid concat overhead.
fn shift_array_by_1(arr: &ArrayRef) -> Result<ArrayRef, String> {
    let n = arr.len();
    if n == 0 {
        return Ok(Arc::clone(arr));
    }

    // Fast path for Int64 and Timestamp (most common in boundary predicates)
    if let Some(i64_arr) = coerce_to_i64(arr) {
        let src_values = i64_arr.values();
        // Build new values: [0, src[0], src[1], ..., src[n-2]]
        let mut new_values = Vec::with_capacity(n);
        new_values.push(0i64); // placeholder for null slot
        new_values.extend_from_slice(&src_values[..n - 1]);
        // Build null bitmap: row 0 is null, rest inherit from source
        let mut validity = Vec::with_capacity(n);
        validity.push(false); // row 0 is null
        if let Some(src_nulls) = i64_arr.nulls() {
            for i in 0..n - 1 {
                validity.push(src_nulls.is_valid(i));
            }
        } else {
            validity.resize(n, true);
        }
        let null_buffer = datafusion::arrow::buffer::NullBuffer::from(validity);
        let shifted = Int64Array::new(
            datafusion::arrow::buffer::ScalarBuffer::from(new_values),
            Some(null_buffer),
        );
        return Ok(Arc::new(shifted) as ArrayRef);
    }

    // Fallback: use concat for other types
    use datafusion::arrow::array::new_null_array;
    use datafusion::arrow::compute::concat;
    let null_prefix = new_null_array(arr.data_type(), 1);
    let sliced = arr.slice(0, n - 1);
    let result = concat(&[null_prefix.as_ref(), &sliced]).map_err(|e| e.to_string())?;
    Ok(result)
}

/// Coerce an array to Int64Array (for arithmetic operations).
fn coerce_to_i64(arr: &ArrayRef) -> Option<Int64Array> {
    match arr.data_type() {
        DataType::Int64 => arr.as_any().downcast_ref::<Int64Array>().cloned(),
        DataType::UInt64 => {
            let src = arr.as_any().downcast_ref::<UInt64Array>()?;
            Some(Int64Array::from_iter(src.iter().map(|v| v.map(|x| x as i64))))
        }
        DataType::Int32 => {
            let src = arr.as_any().downcast_ref::<Int32Array>()?;
            Some(Int64Array::from_iter(src.iter().map(|v| v.map(|x| x as i64))))
        }
        DataType::UInt32 => {
            let src = arr.as_any().downcast_ref::<UInt32Array>()?;
            Some(Int64Array::from_iter(src.iter().map(|v| v.map(|x| x as i64))))
        }
        DataType::Timestamp(_, _) => {
            // Reinterpret timestamp as i64
            let data = arr.to_data();
            Int64Array::try_new(
                data.buffers()[0].clone().into(),
                data.nulls().cloned(),
            ).ok()
        }
        _ => None,
    }
}

/// Perform a vectorized binary operation on two Arrow arrays.
fn vectorized_binop(op: &str, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    use datafusion::arrow::compute::kernels::cmp;
    use datafusion::arrow::compute::kernels::boolean;
    use datafusion::arrow::compute::kernels::numeric;
    
    match op {
        "Or" => {
            let l = left.as_any().downcast_ref::<BooleanArray>()
                .ok_or("OR requires boolean arrays")?;
            let r = right.as_any().downcast_ref::<BooleanArray>()
                .ok_or("OR requires boolean arrays")?;
            Ok(Arc::new(boolean::or(l, r).map_err(|e| e.to_string())?) as ArrayRef)
        }
        "And" => {
            let l = left.as_any().downcast_ref::<BooleanArray>()
                .ok_or("AND requires boolean arrays")?;
            let r = right.as_any().downcast_ref::<BooleanArray>()
                .ok_or("AND requires boolean arrays")?;
            Ok(Arc::new(boolean::and(l, r).map_err(|e| e.to_string())?) as ArrayRef)
        }
        "Ne" => {
            // Try i64 comparison first (covers Int64, UInt64, Timestamp)
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                // Fused neq + null-as-true in one pass: avoids double allocation.
                // NULL != X → true (boundary), non-null uses direct value comparison.
                let n = l.len();
                let l_values = l.values();
                let r_values = r.values();
                let l_nulls = l.nulls();
                let r_nulls = r.nulls();
                let has_any_nulls = l_nulls.is_some() || r_nulls.is_some();

                let mut out = Vec::with_capacity(n);
                if has_any_nulls {
                    for i in 0..n {
                        let l_null = l_nulls.is_some_and(|nb| !nb.is_valid(i));
                        let r_null = r_nulls.is_some_and(|nb| !nb.is_valid(i));
                        out.push(if l_null || r_null { true } else { l_values[i] != r_values[i] });
                    }
                } else {
                    // Fast path: no nulls at all — pure value comparison
                    for i in 0..n {
                        out.push(l_values[i] != r_values[i]);
                    }
                }
                return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
            }
            // Float64
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                let result = cmp::neq(l, r).map_err(|e| e.to_string())?;
                return Ok(Arc::new(result) as ArrayRef);
            }
            // String types
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<StringArray>(),
                right.as_any().downcast_ref::<StringArray>(),
            ) {
                let result = cmp::neq(l, r).map_err(|e| e.to_string())?;
                return Ok(Arc::new(result) as ArrayRef);
            }
            Err(format!("Ne: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Eq" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                let result = cmp::eq(&l, &r).map_err(|e| e.to_string())?;
                return Ok(Arc::new(result) as ArrayRef);
            }
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                return Ok(Arc::new(cmp::eq(l, r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Eq: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Gt" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                // Check if right is a constant (all same value) — use scalar comparison
                let n = l.len();
                if n > 0 && r.null_count() == 0 {
                    let first_val = r.value(0);
                    let is_scalar = (1..n).all(|i| r.value(i) == first_val);
                    if is_scalar {
                        let l_values = l.values();
                        let l_nulls = l.nulls();
                        let mut out = Vec::with_capacity(n);
                        if let Some(nulls) = l_nulls {
                            for i in 0..n {
                                out.push(if !nulls.is_valid(i) { false } else { l_values[i] > first_val });
                            }
                        } else {
                            for i in 0..n {
                                out.push(l_values[i] > first_val);
                            }
                        }
                        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
                    }
                }
                let result = cmp::gt(&l, &r).map_err(|e| e.to_string())?;
                return Ok(Arc::new(result) as ArrayRef);
            }
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                return Ok(Arc::new(cmp::gt(l, r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Gt: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Lt" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(cmp::lt(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Lt: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Ge" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(cmp::gt_eq(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Ge: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Le" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(cmp::lt_eq(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Le: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Sub" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                // Direct subtraction on raw i64 values — avoids Arrow kernel overhead
                let n = l.len();
                let l_values = l.values();
                let r_values = r.values();
                let l_nulls = l.nulls();
                let r_nulls = r.nulls();
                let has_any_nulls = l_nulls.is_some() || r_nulls.is_some();

                let mut result_values = Vec::with_capacity(n);
                for i in 0..n {
                    result_values.push(l_values[i].wrapping_sub(r_values[i]));
                }

                let null_buffer = if has_any_nulls {
                    let mut validity = Vec::with_capacity(n);
                    for i in 0..n {
                        let l_valid = l_nulls.is_none_or(|nb| nb.is_valid(i));
                        let r_valid = r_nulls.is_none_or(|nb| nb.is_valid(i));
                        validity.push(l_valid && r_valid);
                    }
                    Some(datafusion::arrow::buffer::NullBuffer::from(validity))
                } else {
                    None
                };

                let result = Int64Array::new(
                    datafusion::arrow::buffer::ScalarBuffer::from(result_values),
                    null_buffer,
                );
                return Ok(Arc::new(result) as ArrayRef);
            }
            if let (Some(l), Some(r)) = (
                left.as_any().downcast_ref::<Float64Array>(),
                right.as_any().downcast_ref::<Float64Array>(),
            ) {
                return Ok(Arc::new(numeric::sub(l, r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Sub: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Add" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(numeric::add(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Add: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Mul" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(numeric::mul(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Mul: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        "Div" => {
            if let (Some(l), Some(r)) = (coerce_to_i64(left), coerce_to_i64(right)) {
                return Ok(Arc::new(numeric::div(&l, &r).map_err(|e| e.to_string())?) as ArrayRef);
            }
            Err(format!("Div: unsupported types {:?} and {:?}", left.data_type(), right.data_type()))
        }
        _ => Err(format!("Unsupported binary op: {}", op)),
    }
}

// ============================================================================
// Main entry point
// ============================================================================

/// Build DataFusion sort expressions from the table's `sort_specs`.
pub(crate) fn build_sort_exprs(sort_specs: &[crate::SortSpec]) -> Vec<SortExpr> {
    crate::metadata::sort_specs_to_df_sort_exprs(sort_specs)
}

/// Single-pass group ID assignment with `__group_count__` and `__rn__`.
///
/// 1. Project the predicate's columns plus every declared sort key, in the
///    full declared order (`build_boundary_scan_df`), and collect them.
/// 2. Concatenate and evaluate the boundary flags in one pass
///    (`boundary_flags`).
/// 3. Return a metadata-only table with `__group_id__`, `__group_count__`
///    and `__rn__`.
///
/// This is the count path's fallback when the parallel Parquet count
/// (`parallel_scan::parallel_streaming_group_count`) does not apply, and the
/// whole path for every other input. Runs with the GIL released (reached
/// only from `group_ordered_count_impl` inside `gil::detached`), so it takes
/// only plain Rust values.
pub fn linear_scan_group_id(table: &LTSeqTable, predicate: &PyExpr) -> Result<LTSeqTable, LtseqError> {
    let df = table
        .dataframe
        .as_ref()
        .ok_or(LtseqError::NoData)?;

    let sorted_projected = build_boundary_scan_df(df, &table.sort_specs, predicate)?;

    let proj_batches = RUNTIME
        .block_on(async {
            sorted_projected
                .collect()
                .await
                .map_err(|e| format!("Failed to collect projected data: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    let total_rows: usize = proj_batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return build_metadata_table(Vec::new(), Vec::new(), Vec::new(), table);
    }

    let schema = proj_batches[0].schema();
    let concat_batch = concat_batches(&schema, &proj_batches).map_err(|e| {
        LtseqError::Runtime(format!(
            "Failed to concatenate projected batches: {}",
            e
        ))
    })?;

    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, field) in concat_batch.schema().fields().iter().enumerate() {
        name_to_idx.insert(field.name().clone(), i);
    }

    let boundaries = boundary_flags(predicate, &concat_batch, &name_to_idx)
        .map_err(|e| {
            LtseqError::Runtime(format!(
                "Vectorized boundary evaluation failed: {}",
                e
            ))
        })?;

    build_group_metadata_from_boundaries(&boundaries, table)
}

/// Build the projected + re-sorted DataFrame the general linear-scan path
/// collects for boundary detection: predicate-referenced columns plus every
/// declared sort key, ordered by the FULL sort_specs.
///
/// Boundary detection depends on the physical adjacency of rows, which is
/// defined by the FULL declared sort order — sorting by only the
/// predicate-referenced subset of sort keys would globally re-order the
/// sequence and move the boundaries (issue #141). Carry every sort key in
/// the projection and re-sort by the complete sort_specs; when the plan
/// already satisfies that ordering, DataFusion's enforce_sorting removes
/// the Sort node, so an already-sorted prefix costs nothing.
///
/// Split from `linear_scan_group_id` so tests can assert on the exact plan
/// this path executes (see `tests` module below).
fn build_boundary_scan_df(
    df: &datafusion::dataframe::DataFrame,
    sort_specs: &[crate::metadata::SortSpec],
    predicate: &PyExpr,
) -> Result<datafusion::dataframe::DataFrame, LtseqError> {
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    for spec in sort_specs {
        needed_cols.insert(spec.column.clone());
    }

    let sort_exprs: Vec<SortExpr> = sort_specs
        .iter()
        .map(|spec| spec.to_df_sort_expr())
        .collect();

    let col_exprs: Vec<Expr> = needed_cols
        .iter()
        .map(|name| Expr::Column(Column::new_unqualified(name)))
        .collect();

    let projected_df = if col_exprs.is_empty() {
        df.clone()
    } else {
        df.clone().select(col_exprs).map_err(|e| {
            LtseqError::Runtime(format!(
                "Failed to project columns for linear scan: {}",
                e
            ))
        })?
    };

    if sort_exprs.is_empty() {
        Ok(projected_df)
    } else {
        projected_df.sort(sort_exprs).map_err(|e| {
            LtseqError::Runtime(format!(
                "Failed to sort projected data: {}",
                e
            ))
        })
    }
}

/// Build group metadata arrays from per-row boundary flags.
fn build_group_metadata_from_boundaries(
    boundaries: &[bool],
    table: &LTSeqTable,
) -> Result<LTSeqTable, LtseqError> {
    let total_rows = boundaries.len();

    // Compute group IDs from boundaries via prefix sum
    let mut group_ids: Vec<i64> = Vec::with_capacity(total_rows);
    let mut current_gid: i64 = 0;
    for &is_boundary in boundaries {
        if is_boundary {
            current_gid += 1;
        }
        group_ids.push(current_gid);
    }

    let num_groups = current_gid as usize;
    let mut group_counts: Vec<i64> = vec![0; num_groups + 1];
    for &gid in &group_ids {
        group_counts[gid as usize] += 1;
    }

    let mut rn_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut count_values: Vec<i64> = Vec::with_capacity(total_rows);
    let mut rn_counters: Vec<i64> = vec![0; num_groups + 1];

    for &gid in &group_ids {
        rn_counters[gid as usize] += 1;
        rn_values.push(rn_counters[gid as usize]);
        count_values.push(group_counts[gid as usize]);
    }

    build_metadata_table(group_ids, count_values, rn_values, table)
}

/// Build the metadata LTSeqTable from group_id, count, and rn arrays.
fn build_metadata_table(
    group_ids: Vec<i64>,
    count_values: Vec<i64>,
    rn_values: Vec<i64>,
    table: &LTSeqTable,
) -> Result<LTSeqTable, LtseqError> {
    // ── Phase B: Return metadata as MemTable ────────────────────────────
    //
    // Instead of JOINing with the original data (expensive sort + ROW_NUMBER),
    // return ONLY the group metadata columns. This makes downstream operations
    // like first().count() extremely fast since they only need __rn__ filtering.
    //
    // For operations that need original columns (first().to_pandas()), the
    // non-linear-scan path through group_id_impl handles those.

    let group_id_array: ArrayRef = Arc::new(Int64Array::from(group_ids));
    let group_count_array: ArrayRef = Arc::new(Int64Array::from(count_values));
    let rn_array: ArrayRef = Arc::new(Int64Array::from(rn_values));

    let meta_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("__group_id__", DataType::Int64, true),
        Field::new("__group_count__", DataType::Int64, true),
        Field::new("__rn__", DataType::Int64, true),
    ]));

    let meta_batch = RecordBatch::try_new(
        Arc::clone(&meta_schema),
        vec![group_id_array, group_count_array, rn_array],
    )
    .map_err(|e| {
        LtseqError::Runtime(format!(
            "Failed to create group metadata batch: {}",
            e
        ))
    })?;

    // Return as LTSeqTable from the metadata batch
    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        vec![meta_batch],
        meta_schema,
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{sort_specs_to_file_sort_order, SortSpec};
    use datafusion::arrow::array::Int64Array;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    /// Every operator `can_linear_scan` admits evaluates (#147: FloorDiv and
    /// Mod were admitted with no arm in `vectorized_binop`).
    #[test]
    fn every_supported_binary_op_evaluates() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![-7, 7, 6]));
        let divisors: ArrayRef = Arc::new(Int64Array::from(vec![2, -2, 3]));
        let bools: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        for op in SUPPORTED_BINARY_OPS {
            let (left, right) = match op {
                "And" | "Or" => (&bools, &bools),
                _ => (&ints, &divisors),
            };
            let result = vectorized_binop(op, left, right);
            assert!(result.is_ok(), "{op}: {:?}", result.err());
        }
    }

    /// The issue #141 trigger predicate shape: references only the SECONDARY
    /// sort key — `(eventtime - eventtime.shift(1)) > 10`.
    fn secondary_key_predicate() -> PyExpr {
        PyExpr::BinOp {
            op: "Gt".to_string(),
            left: Box::new(PyExpr::BinOp {
                op: "Sub".to_string(),
                left: Box::new(PyExpr::Column("eventtime".to_string())),
                right: Box::new(PyExpr::Call {
                    func: "shift".to_string(),
                    args: vec![PyExpr::Literal(LiteralValue::Int64(1))],
                    kwargs: HashMap::new(),
                    on: Some(Box::new(PyExpr::Column("eventtime".to_string()))),
                }),
            }),
            right: Box::new(PyExpr::Literal(LiteralValue::Int64(10))),
        }
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("userid", DataType::Int64, false),
            Field::new("eventtime", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![100, 101, 1, 2])),
            ],
        )
        .expect("valid test batch")
    }

    /// Render the optimized physical plan of the exact DataFrame the general
    /// linear-scan path collects.
    fn physical_plan_string(declare_order: bool) -> String {
        let specs = vec![
            SortSpec::new("userid".to_string(), false),
            SortSpec::new("eventtime".to_string(), false),
        ];

        let batch = sample_batch();
        let mut mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .expect("valid MemTable");
        if declare_order {
            mem_table = mem_table.with_sort_order(sort_specs_to_file_sort_order(&specs));
        }

        let ctx = SessionContext::new();
        let df = ctx
            .read_table(Arc::new(mem_table))
            .expect("read MemTable");

        let scan_df = build_boundary_scan_df(&df, &specs, &secondary_key_predicate())
            .expect("build boundary scan plan");

        let plan = RUNTIME
            .block_on(scan_df.create_physical_plan())
            .expect("create physical plan");
        let rendered = displayable(plan.as_ref()).indent(false).to_string();
        rendered
    }

    /// Issue #141 acceptance: when the input already carries the full
    /// declared ordering, the general path must not introduce an extra
    /// global Sort — enforce_sorting elides the redundant full-key sort.
    #[test]
    fn boundary_scan_plan_has_no_sort_when_order_declared() {
        let plan = physical_plan_string(true);
        assert!(
            !plan.contains("SortExec"),
            "general linear-scan plan re-sorts declared-order input:\n{}",
            plan
        );
    }

    /// Control: without a declared ordering the full-key Sort must survive.
    /// Also proves "SortExec" is the token DataFusion prints, so the
    /// assertion above cannot pass vacuously.
    #[test]
    fn boundary_scan_plan_sorts_when_order_unknown() {
        let plan = physical_plan_string(false);
        assert!(
            plan.contains("SortExec"),
            "expected a Sort for undeclared-order input:\n{}",
            plan
        );
    }

    fn declared_specs() -> Vec<SortSpec> {
        vec![
            SortSpec::new("userid".to_string(), false),
            SortSpec::new("eventtime".to_string(), false),
        ]
    }

    /// An empty input must yield the zero-row METADATA table, whose columns
    /// group_ordered_count filters on, not a stub carrying the input schema
    /// (issue #161).
    fn assert_empty_metadata_table(result: &LTSeqTable) {
        let names: Vec<&str> = result
            .require_schema()
            .expect("metadata schema")
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["__group_id__", "__group_count__", "__rn__"]);
        let df = result.require_df().expect("zero-row table has a plan");
        let rows = RUNTIME.block_on((**df).clone().count()).expect("count");
        assert_eq!(rows, 0);
    }

    /// The general path (in-memory input).
    #[test]
    fn group_id_on_empty_input_is_empty_metadata_table() {
        let table = LTSeqTable::from_batches(
            crate::engine::create_session_context(),
            Vec::new(),
            sample_batch().schema(),
            declared_specs(),
        )
        .expect("zero-row table");

        let result = linear_scan_group_id(&table, &secondary_key_predicate())
            .expect("group ids of an empty table");
        assert_empty_metadata_table(&result);
    }

    /// A lazy Parquet scan (pre-sorted file with no rows).
    #[test]
    fn group_id_on_empty_parquet_is_empty_metadata_table() {
        use datafusion::datasource::file_format::options::ParquetReadOptions;

        let path = std::env::temp_dir().join(format!(
            "ltseq_issue161_empty_{}.parquet",
            std::process::id()
        ));
        let schema = sample_batch().schema();
        let file = std::fs::File::create(&path).expect("create parquet file");
        let writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("parquet writer");
        writer.close().expect("write empty parquet file");

        let path_str = path.to_str().expect("utf-8 temp path").to_string();
        let session = crate::engine::create_session_context();
        let df = RUNTIME
            .block_on(session.read_parquet(&path_str, ParquetReadOptions::default()))
            .expect("read parquet");
        let table = LTSeqTable::from_df_with_schema(
            Arc::clone(&session),
            df,
            schema,
            declared_specs(),
            Some(path_str),
        );

        let result = linear_scan_group_id(&table, &secondary_key_predicate());
        let _ = std::fs::remove_file(&path);
        assert_empty_metadata_table(&result.expect("group ids of an empty file"));
    }

    // ── Fused evaluator: batch-split consistency (issue #157) ──────────────

    fn col(name: &str) -> PyExpr {
        PyExpr::Column(name.to_string())
    }

    fn lit(value: LiteralValue) -> PyExpr {
        PyExpr::Literal(value)
    }

    fn binop(op: &str, left: PyExpr, right: PyExpr) -> PyExpr {
        PyExpr::BinOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn shift1(name: &str) -> PyExpr {
        PyExpr::Call {
            func: "shift".to_string(),
            args: vec![lit(LiteralValue::Int64(1))],
            kwargs: HashMap::new(),
            on: Some(Box::new(col(name))),
        }
    }

    /// `c.shift(1, key=value)`
    fn shift_with(name: &str, key: &str, value: PyExpr) -> PyExpr {
        PyExpr::Call {
            func: "shift".to_string(),
            args: vec![lit(LiteralValue::Int64(1))],
            kwargs: HashMap::from([(key.to_string(), value)]),
            on: Some(Box::new(col(name))),
        }
    }

    /// `c != c.shift(1)`
    fn changes(name: &str) -> PyExpr {
        binop("Ne", col(name), shift1(name))
    }

    /// `(c - c.shift(1)) > threshold`
    fn gap_over(name: &str, threshold: i64) -> PyExpr {
        binop(
            "Gt",
            binop("Sub", col(name), shift1(name)),
            lit(LiteralValue::Int64(threshold)),
        )
    }

    /// Every fused shape and combination the R2 kernel accepts.
    fn fusable_predicates() -> Vec<PyExpr> {
        vec![
            changes("u"),
            gap_over("t", 4),
            gap_over("ts", 50),
            binop("Or", changes("u"), gap_over("t", 4)),
            binop("And", changes("u"), gap_over("ts", 50)),
            binop(
                "Or",
                binop("And", changes("u"), gap_over("t", 4)),
                changes("t"),
            ),
        ]
    }

    /// Int64 `u`, Int32 `t` and microsecond-timestamp `ts`, so every
    /// i64 coercion in `coerce_to_i64` meets a batch cut. With `nulls`, NULLs
    /// sit next to each other, at the first and last row, and on both sides
    /// of every cut position the tests try. A NULL slot keeps a value that
    /// would NOT start a group, so an evaluator that ignored the validity
    /// bitmap (here or in the previous row) gets a different answer.
    fn boundary_batch(nulls: bool) -> RecordBatch {
        use datafusion::arrow::array::{Int32Array, TimestampMicrosecondArray};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::TimeUnit;

        let validity = |null_rows: &[usize]| {
            nulls.then(|| NullBuffer::from_iter((0..12).map(|i| !null_rows.contains(&i))))
        };
        let u = Int64Array::new(
            vec![1, 1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4].into(),
            validity(&[2, 5, 6]),
        );
        let t = Int32Array::new(
            vec![0, 5, 6, 7, 8, 21, 30, 31, 32, 33, 41, 50].into(),
            validity(&[0, 3, 8]),
        );
        let ts = TimestampMicrosecondArray::new(
            vec![0, 100, 120, 200, 210, 230, 310, 320, 400, 401, 402, 900].into(),
            validity(&[4, 11]),
        );

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("u", DataType::Int64, true),
            Field::new("t", DataType::Int32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(u), Arc::new(t), Arc::new(ts)])
            .expect("valid boundary batch")
    }

    fn name_index(batch: &RecordBatch) -> HashMap<String, usize> {
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), i))
            .collect()
    }

    /// Flags for `batch` evaluated as consecutive pieces starting at each of
    /// `cuts`, each piece seeing the previous piece's last row as `prev`.
    fn flags_in_pieces(expr: &PyExpr, batch: &RecordBatch, cuts: &[usize]) -> Vec<bool> {
        let idx = name_index(batch);
        let mut starts = vec![0];
        starts.extend_from_slice(cuts);
        starts.push(batch.num_rows());
        let mut flags = Vec::new();
        let mut prev: Option<RecordBatch> = None;
        for w in starts.windows(2) {
            let piece = batch.slice(w[0], w[1] - w[0]);
            flags.extend(
                fused_boundaries(expr, &piece, &idx, prev.as_ref())
                    .expect("fusable predicate"),
            );
            prev = Some(piece.slice(piece.num_rows() - 1, 1));
        }
        flags
    }

    /// Pins the NULL semantics the split tests below rely on, so they cannot
    /// pass by agreeing on a wrong answer: a NULL on either side of the
    /// comparison starts a group, as on the DataFusion path.
    #[test]
    fn fused_flags_mark_nulls_as_boundaries() {
        let batch = boundary_batch(true);
        let flags = fused_boundaries(&changes("u"), &batch, &name_index(&batch), None)
            .expect("fusable predicate");
        // u = [1, 1, ∅, 1, 2, ∅, ∅, 3, 3, 3, 4, 4]; the NULL-free variant
        // [1, 1, 1, 1, 2, 2, 3, 3, ...] would flag only rows 0, 4, 6 and 10.
        let expected = [
            true, false, true, true, true, true, true, true, false, false, true, false,
        ];
        assert_eq!(flags, expected);
    }

    /// Splitting a batch anywhere and carrying the previous row across the
    /// cut gives the same flags as evaluating it whole. This is the
    /// streaming-vs-batch consistency the parallel Parquet count relies on.
    #[test]
    fn fused_flags_do_not_depend_on_batch_cuts() {
        for nulls in [false, true] {
            let batch = boundary_batch(nulls);
            let n = batch.num_rows();
            for expr in fusable_predicates() {
                let whole = flags_in_pieces(&expr, &batch, &[]);
                for cut in 1..n {
                    assert_eq!(
                        flags_in_pieces(&expr, &batch, &[cut]),
                        whole,
                        "cut at {cut}, nulls={nulls}, expr={expr:?}"
                    );
                }
                let every_row: Vec<usize> = (1..n).collect();
                assert_eq!(
                    flags_in_pieces(&expr, &batch, &every_row),
                    whole,
                    "one-row pieces, nulls={nulls}, expr={expr:?}"
                );
            }
        }
    }

    /// On NULL-free data the fused evaluator agrees with the multi-pass one
    /// (issue #189 tracks where the multi-pass one gets NULLs wrong).
    #[test]
    fn fused_flags_match_vectorized_flags_without_nulls() {
        let batch = boundary_batch(false);
        let idx = name_index(&batch);
        for expr in fusable_predicates() {
            assert_eq!(
                fused_boundaries(&expr, &batch, &idx, None),
                Some(vectorized_boundary_flags(&expr, &batch, &idx).expect("vectorized flags")),
                "expr={expr:?}"
            );
        }
    }

    /// Shapes outside the fused set are declined, with or without a previous
    /// row, so the caller falls back instead of getting a guessed answer.
    #[test]
    fn fused_evaluator_declines_unsupported_shapes() {
        let mut batch = boundary_batch(false);
        let idx_cols = batch.num_columns();
        let strings = datafusion::arrow::array::StringArray::from(vec!["a"; batch.num_rows()]);
        let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| (**f).clone()).collect();
        fields.push(Field::new("s", DataType::Utf8, false));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(strings));
        batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .expect("batch with a string column");
        assert_eq!(batch.num_columns(), idx_cols + 1);
        let idx = name_index(&batch);
        let prev = batch.slice(0, 1);

        let unsupported = [
            binop("Ne", shift1("u"), col("u")),                  // swapped operands
            binop("Ne", col("u"), shift1("t")),                  // different columns
            changes("s"),                                        // not i64-coercible
            binop("Gt", binop("Sub", col("t"), shift1("t")), lit(LiteralValue::Float64(1.5))),
            binop("Ge", binop("Sub", col("t"), shift1("t")), lit(LiteralValue::Int64(4))),
            binop("Or", changes("u"), changes("s")),             // one leaf unsupported
        ];
        for expr in unsupported {
            assert_eq!(fused_boundaries(&expr, &batch, &idx, None), None, "expr={expr:?}");
            assert_eq!(
                fused_boundaries(&expr, &batch, &idx, Some(&prev)),
                None,
                "expr={expr:?}"
            );
        }
    }

    /// A Decimal literal anywhere in a predicate sends it to the DataFusion
    /// path, integral or not: as an `Int64`, `x / Decimal("2")` would divide
    /// as integers here.
    #[test]
    fn decimal_literals_are_not_linear_scan_values() {
        let two = |scale: i8| {
            lit(LiteralValue::Decimal128 { value: 2 * 10i128.pow(scale as u32), precision: 2, scale })
        };
        let schema = ArrowSchema::new(vec![Field::new("x", DataType::Int64, true)]);
        for scale in [0, 1] {
            let halves = binop("Gt", binop("Div", col("x"), two(scale)), binop("Div", shift1("x"), two(scale)));
            assert!(!can_linear_scan(&halves, &schema), "scale {scale}");
            let gap = binop("Gt", binop("Sub", col("x"), shift1("x")), two(scale));
            assert!(!can_linear_scan(&gap, &schema), "scale {scale}");
        }
        assert!(can_linear_scan(
            &binop("Gt", binop("Div", col("x"), lit(LiteralValue::Int64(2))), shift1("x")),
            &schema
        ));
    }

    /// The kernel takes a predicate only where it computes what DataFusion
    /// computes on the column types at hand.
    #[test]
    fn eligibility_follows_the_column_types() {
        use datafusion::arrow::datatypes::TimeUnit;
        let int = |v| lit(LiteralValue::Int64(v));
        let float = |v| lit(LiteralValue::Float64(v));
        let gap = |c: &str, n| binop("Gt", binop("Sub", col(c), shift1(c)), int(n));
        let types = [
            ("i64", DataType::Int64),
            ("i32", DataType::Int32),
            ("u32", DataType::UInt32),
            ("u64", DataType::UInt64),
            ("f64", DataType::Float64),
            ("ts", DataType::Timestamp(TimeUnit::Second, None)),
            ("ts_us", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("s", DataType::Utf8),
            ("b", DataType::Boolean),
            ("d", DataType::Date32),
        ];
        let schema = ArrowSchema::new(
            types.iter().map(|(name, t)| Field::new(*name, t.clone(), true)).collect::<Vec<_>>(),
        );
        let table: Vec<(PyExpr, bool)> = vec![
            // Comparisons of a column with its previous value.
            (changes("i64"), true),
            (changes("i32"), true),
            (changes("u32"), true),
            (changes("u64"), true),
            (changes("ts"), true),
            (changes("f64"), true),
            (changes("s"), true),
            (changes("b"), false),
            (changes("d"), false),
            (binop("Gt", col("u64"), shift1("u64")), false),
            (binop("Lt", col("f64"), shift1("f64")), false),
            (binop("Gt", col("ts"), shift1("ts_us")), false),
            // Arithmetic: only what DataFusion computes in Int64 or Float64.
            (gap("i64", 4), true),
            (gap("i32", 4), false),
            (gap("u32", 4), false),
            (gap("u64", 4), false),
            (gap("ts", 1800), false),
            (binop("Gt", binop("Sub", col("i32"), int(1)), shift1("i32")), true),
            (binop("Gt", binop("Sub", col("f64"), shift1("f64")), float(0.5)), true),
            (binop("Gt", binop("Add", col("f64"), shift1("f64")), float(0.5)), false),
            // Float thresholds against integers: integral and below 2^53 only.
            (binop("Gt", binop("Sub", col("i64"), shift1("i64")), float(2.0)), true),
            (binop("Gt", binop("Sub", col("i64"), shift1("i64")), float(1.5)), false),
            (binop("Ge", binop("Sub", col("i64"), shift1("i64")), float(2.0)), false),
            (
                binop("Gt", binop("Sub", col("i64"), shift1("i64")), lit(LiteralValue::String("1".into()))),
                false,
            ),
            (binop("Or", changes("i64"), gap("ts", 1800)), false),
            (binop("Gt", binop("Sub", col("i64"), shift1("i64")), lit(LiteralValue::Null)), true),
            // A shift with keyword arguments is the reference's to evaluate.
            (binop("Ne", col("i64"), shift_with("i64", "default", int(7))), false),
            (
                binop("Ne", col("i64"), shift_with("i64", "partition_by", lit(LiteralValue::String("s".into())))),
                false,
            ),
        ];
        for (expr, eligible) in table {
            assert_eq!(can_linear_scan(&expr, &schema), eligible, "{expr:?}");
        }
    }
}
