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
//! | `Call { func: "shift", on: Column("x"), args: [1] }` | Read column value at previous row |
//! | `Call { func: "is_null", on: expr }` | Check if evaluated value is null |
//! | `BinOp { op: Ne/Eq/Gt/Lt/Ge/Le }` | Compare two values |
//! | `BinOp { op: Or/And }` | Logical combination |
//! | `BinOp { op: Add/Sub/Mul/Div }` | Arithmetic |
//! | `Literal { value, dtype }` | Constant value |
//! | `UnaryOp { op: "Not" }` | Logical negation |

use crate::engine::{create_sequential_session, RUNTIME};
use crate::types::PyExpr;
use crate::LTSeqTable;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::logical_expr::{Expr, SortExpr};
use futures_util::StreamExt;
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ============================================================================
// Expression eligibility check
// ============================================================================

/// Check if a PyExpr can be evaluated by the linear scan engine.
///
/// Returns true if the expression tree:
/// 1. Contains at least one shift(1) call (otherwise, it's a simple column/expression
///    and should use the standard DataFusion IS DISTINCT FROM LAG path)
/// 2. Only contains operations we support: Column, shift(1), is_null, BinOp, UnaryOp, Literal
pub fn can_linear_scan(expr: &PyExpr) -> bool {
    is_supported_expr(expr) && contains_shift(expr)
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
                contains_shift(on)
            }
        }
        PyExpr::Window { .. } => false,
    }
}

/// Check if all nodes in the expression tree are supported by the linear scan evaluator.
fn is_supported_expr(expr: &PyExpr) -> bool {
    match expr {
        PyExpr::Column(_) => true,
        PyExpr::Literal { .. } => true,
        PyExpr::BinOp { op, left, right } => {
            let valid_op = matches!(
                op.as_str(),
                "Ne" | "Eq"
                    | "Gt"
                    | "Lt"
                    | "Ge"
                    | "Le"
                    | "Or"
                    | "And"
                    | "Add"
                    | "Sub"
                    | "Mul"
                    | "Div"
                    | "FloorDiv"
                    | "Mod"
            );
            valid_op && is_supported_expr(left) && is_supported_expr(right)
        }
        PyExpr::UnaryOp { op, operand } => op == "Not" && is_supported_expr(operand),
        PyExpr::Call {
            func,
            args,
            kwargs: _,
            on,
        } => {
            match func.as_str() {
                "shift" => {
                    // Only support shift(1) on a column reference
                    if !matches!(on.as_ref(), PyExpr::Column(_)) {
                        return false;
                    }
                    // First arg must be literal integer 1
                    if args.len() != 1 {
                        return false;
                    }
                    match &args[0] {
                        PyExpr::Literal { value, dtype } => {
                            // Accept shift(1) only
                            if value != "1" {
                                return false;
                            }
                            // Must be integer type
                            matches!(
                                dtype.as_str(),
                                "Int64" | "Int32" | "int" | "UInt64" | "UInt32"
                            )
                        }
                        _ => false,
                    }
                }
                "is_null" => {
                    // is_null() on a supported sub-expression
                    is_supported_expr(on)
                }
                _ => false,
            }
        }
        PyExpr::Window { .. } => false,
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

impl Value {
    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Coerce to f64 for arithmetic operations
    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int64(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as i64
    fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            (Value::Str(a), Value::Str(b)) => a == b,
            // Cross-type numeric comparison
            (Value::Int64(a), Value::Float64(b)) => (*a as f64) == *b,
            (Value::Float64(a), Value::Int64(b)) => *a == (*b as f64),
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

// ============================================================================
// Column accessor — typed reader for Arrow arrays
// ============================================================================

/// Read a value from an Arrow array at a given index.
fn read_array_value(array: &ArrayRef, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Int64(arr.value(row))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Int64(arr.value(row) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Int64(arr.value(row) as i64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Int64(arr.value(row) as i64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::Float64(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::Str(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                .unwrap();
            Value::Str(arr.value(row).to_string())
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringViewArray>()
                .unwrap();
            Value::Str(arr.value(row).to_string())
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(row))
        }
        DataType::Timestamp(_, _) => {
            // Timestamps are stored as i64 internally (microseconds, nanoseconds, etc.)
            // For comparison and arithmetic, treat as i64
            use datafusion::arrow::array::TimestampMicrosecondArray;
            use datafusion::arrow::array::TimestampMillisecondArray;
            use datafusion::arrow::array::TimestampNanosecondArray;
            use datafusion::arrow::array::TimestampSecondArray;
            if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                Value::Int64(arr.value(row))
            } else if let Some(arr) =
                array.as_any().downcast_ref::<TimestampMillisecondArray>()
            {
                Value::Int64(arr.value(row))
            } else if let Some(arr) =
                array.as_any().downcast_ref::<TimestampMicrosecondArray>()
            {
                Value::Int64(arr.value(row))
            } else if let Some(arr) =
                array.as_any().downcast_ref::<TimestampNanosecondArray>()
            {
                Value::Int64(arr.value(row))
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

// ============================================================================
// Expression evaluator
// ============================================================================

// ============================================================================
// Compiled evaluator — pre-resolves columns for fast per-row evaluation
// ============================================================================

/// Pre-downcasted column accessor for zero-overhead typed reads.
#[derive(Clone)]
enum TypedColumn {
    Int64(Arc<Int64Array>),
    UInt64(Arc<UInt64Array>),
    Int32(Arc<Int32Array>),
    UInt32(Arc<UInt32Array>),
    Float64(Arc<Float64Array>),
    Bool(Arc<BooleanArray>),
    /// Fallback: use dynamic dispatch via read_array_value
    Generic(ArrayRef),
}

impl TypedColumn {
    fn from_array(array: &ArrayRef) -> Self {
        match array.data_type() {
            DataType::Int64 => {
                TypedColumn::Int64(Arc::new(array.as_any().downcast_ref::<Int64Array>().unwrap().clone()))
            }
            DataType::UInt64 => {
                TypedColumn::UInt64(Arc::new(array.as_any().downcast_ref::<UInt64Array>().unwrap().clone()))
            }
            DataType::Int32 => {
                TypedColumn::Int32(Arc::new(array.as_any().downcast_ref::<Int32Array>().unwrap().clone()))
            }
            DataType::UInt32 => {
                TypedColumn::UInt32(Arc::new(array.as_any().downcast_ref::<UInt32Array>().unwrap().clone()))
            }
            DataType::Float64 => {
                TypedColumn::Float64(Arc::new(array.as_any().downcast_ref::<Float64Array>().unwrap().clone()))
            }
            DataType::Boolean => {
                TypedColumn::Bool(Arc::new(array.as_any().downcast_ref::<BooleanArray>().unwrap().clone()))
            }
            DataType::Timestamp(_, _) => {
                // Timestamps are i64 internally — reinterpret the underlying data as Int64
                let data = array.to_data();
                match Int64Array::try_new(
                    data.buffers()[0].clone().into(),
                    data.nulls().cloned(),
                ) {
                    Ok(i64_arr) => TypedColumn::Int64(Arc::new(i64_arr)),
                    Err(_) => TypedColumn::Generic(Arc::clone(array)),
                }
            }
            _ => TypedColumn::Generic(Arc::clone(array)),
        }
    }

    #[inline(always)]
    fn read(&self, row: usize) -> Value {
        match self {
            TypedColumn::Int64(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Int64(arr.value(row)) }
            }
            TypedColumn::UInt64(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Int64(arr.value(row) as i64) }
            }
            TypedColumn::Int32(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Int64(arr.value(row) as i64) }
            }
            TypedColumn::UInt32(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Int64(arr.value(row) as i64) }
            }
            TypedColumn::Float64(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Float64(arr.value(row)) }
            }
            TypedColumn::Bool(arr) => {
                if arr.is_null(row) { Value::Null } else { Value::Bool(arr.value(row)) }
            }
            TypedColumn::Generic(arr) => read_array_value(arr, row),
        }
    }
}

/// A compiled instruction for the stack-based evaluator.
/// Pre-resolves column indices and types to avoid per-row lookups.
#[derive(Debug, Clone)]
enum Instruction {
    /// Push column value at current row
    PushColumn(usize), // column index in batch
    /// Push column value at previous row (shift(1))
    PushShiftedColumn(usize), // column index in batch
    /// Push literal value
    PushLiteral(Value),
    /// Binary operation on top two stack values
    BinOp(BinOpKind),
    /// Unary NOT on top stack value
    Not,
    /// is_null check on top stack value
    IsNull,
}

#[derive(Debug, Clone, Copy)]
enum BinOpKind {
    Ne,
    Eq,
    Gt,
    Lt,
    Ge,
    Le,
    Or,
    And,
    Add,
    Sub,
    Mul,
    Div,
    FloorDiv,
    Mod,
}

/// Compiled evaluator with pre-resolved column references
#[allow(dead_code)]
struct CompiledEvaluator {
    instructions: Vec<Instruction>,
    /// Pre-downcasted column arrays for typed access
    columns: Vec<TypedColumn>,
}

#[allow(dead_code)]
impl CompiledEvaluator {
    /// Compile a PyExpr into instructions
    fn compile(expr: &PyExpr, batch: &RecordBatch) -> Self {
        let mut name_to_idx = std::collections::HashMap::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            name_to_idx.insert(field.name().clone(), i);
        }

        let columns: Vec<TypedColumn> = batch
            .columns()
            .iter()
            .map(|arr| TypedColumn::from_array(arr))
            .collect();
        let mut instructions = Vec::new();
        Self::compile_expr(expr, &name_to_idx, &mut instructions);

        CompiledEvaluator {
            instructions,
            columns,
        }
    }

    fn compile_expr(
        expr: &PyExpr,
        name_to_idx: &std::collections::HashMap<String, usize>,
        out: &mut Vec<Instruction>,
    ) {
        match expr {
            PyExpr::Column(name) => {
                let idx = name_to_idx.get(name).copied().unwrap_or(usize::MAX);
                out.push(Instruction::PushColumn(idx));
            }
            PyExpr::Literal { value, dtype } => {
                out.push(Instruction::PushLiteral(literal_to_value(value, dtype)));
            }
            PyExpr::Call {
                func, on, args: _, ..
            } => match func.as_str() {
                "shift" => {
                    if let PyExpr::Column(col_name) = on.as_ref() {
                        let idx = name_to_idx.get(col_name).copied().unwrap_or(usize::MAX);
                        out.push(Instruction::PushShiftedColumn(idx));
                    } else {
                        out.push(Instruction::PushLiteral(Value::Null));
                    }
                }
                "is_null" => {
                    Self::compile_expr(on, name_to_idx, out);
                    out.push(Instruction::IsNull);
                }
                _ => {
                    out.push(Instruction::PushLiteral(Value::Null));
                }
            },
            PyExpr::BinOp { op, left, right } => {
                // For short-circuit ops (Or/And), we still evaluate both sides
                // on the stack. True short-circuiting would require jumps.
                Self::compile_expr(left, name_to_idx, out);
                Self::compile_expr(right, name_to_idx, out);
                let kind = match op.as_str() {
                    "Ne" => BinOpKind::Ne,
                    "Eq" => BinOpKind::Eq,
                    "Gt" => BinOpKind::Gt,
                    "Lt" => BinOpKind::Lt,
                    "Ge" => BinOpKind::Ge,
                    "Le" => BinOpKind::Le,
                    "Or" => BinOpKind::Or,
                    "And" => BinOpKind::And,
                    "Add" => BinOpKind::Add,
                    "Sub" => BinOpKind::Sub,
                    "Mul" => BinOpKind::Mul,
                    "Div" => BinOpKind::Div,
                    "FloorDiv" => BinOpKind::FloorDiv,
                    "Mod" => BinOpKind::Mod,
                    _ => BinOpKind::Eq, // fallback
                };
                out.push(Instruction::BinOp(kind));
            }
            PyExpr::UnaryOp { op, operand } => {
                Self::compile_expr(operand, name_to_idx, out);
                if op == "Not" {
                    out.push(Instruction::Not);
                }
            }
            PyExpr::Window { .. } => {
                out.push(Instruction::PushLiteral(Value::Null));
            }
        }
    }

    /// Evaluate the compiled expression for a given row
    #[inline]
    fn evaluate(&self, row: usize, prev_row: Option<usize>) -> bool {
        let mut stack: Vec<Value> = Vec::with_capacity(16);

        for instr in &self.instructions {
            match instr {
                Instruction::PushColumn(idx) => {
                    if *idx < self.columns.len() {
                        stack.push(self.columns[*idx].read(row));
                    } else {
                        stack.push(Value::Null);
                    }
                }
                Instruction::PushShiftedColumn(idx) => {
                    match prev_row {
                        Some(prev) if *idx < self.columns.len() => {
                            stack.push(self.columns[*idx].read(prev));
                        }
                        _ => stack.push(Value::Null),
                    }
                }
                Instruction::PushLiteral(val) => {
                    stack.push(val.clone());
                }
                Instruction::BinOp(kind) => {
                    let rv = stack.pop().unwrap_or(Value::Null);
                    let lv = stack.pop().unwrap_or(Value::Null);
                    stack.push(eval_binop(*kind, &lv, &rv));
                }
                Instruction::Not => {
                    let val = stack.pop().unwrap_or(Value::Null);
                    stack.push(match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    });
                }
                Instruction::IsNull => {
                    let val = stack.pop().unwrap_or(Value::Null);
                    stack.push(Value::Bool(val.is_null()));
                }
            }
        }

        // Convert final result to boolean
        match stack.pop() {
            Some(Value::Bool(b)) => b,
            Some(Value::Null) => true, // NULL → boundary (new group)
            _ => false,
        }
    }

    /// Create a cross-boundary evaluator from the last row of the previous batch
    /// and the first row of the current batch. Used for detecting boundaries at
    /// batch transitions.
    #[allow(dead_code)]
    fn compile_cross_boundary(
        expr: &PyExpr,
        prev_last_row: &RecordBatch,
        curr_batch: &RecordBatch,
    ) -> CrossBoundaryEvaluator {
        // Build name->idx mapping (same schema for both)
        let mut name_to_idx = std::collections::HashMap::new();
        for (i, field) in prev_last_row.schema().fields().iter().enumerate() {
            name_to_idx.insert(field.name().clone(), i);
        }

        let prev_columns: Vec<TypedColumn> = prev_last_row
            .columns()
            .iter()
            .map(|arr| TypedColumn::from_array(arr))
            .collect();

        let curr_columns: Vec<TypedColumn> = curr_batch
            .columns()
            .iter()
            .map(|arr| TypedColumn::from_array(arr))
            .collect();

        let mut instructions = Vec::new();
        Self::compile_expr(expr, &name_to_idx, &mut instructions);

        CrossBoundaryEvaluator {
            instructions,
            prev_columns,
            curr_columns,
        }
    }
}

/// Evaluator for cross-batch boundary detection.
/// Uses the previous batch's last row for shift(1) and the current batch's first row
/// for the current row value.
#[allow(dead_code)]
struct CrossBoundaryEvaluator {
    instructions: Vec<Instruction>,
    /// Columns from the previous batch's last row (row index 0 in a 1-row batch)
    prev_columns: Vec<TypedColumn>,
    /// Columns from the current batch (row index 0)
    curr_columns: Vec<TypedColumn>,
}

#[allow(dead_code)]
impl CrossBoundaryEvaluator {
    /// Evaluate the boundary predicate: current row = curr_batch[0], prev row = prev_batch[0]
    #[inline]
    fn evaluate_boundary(&self) -> bool {
        let mut stack: Vec<Value> = Vec::with_capacity(16);

        for instr in &self.instructions {
            match instr {
                Instruction::PushColumn(idx) => {
                    // Current row → curr_columns, row 0
                    if *idx < self.curr_columns.len() {
                        stack.push(self.curr_columns[*idx].read(0));
                    } else {
                        stack.push(Value::Null);
                    }
                }
                Instruction::PushShiftedColumn(idx) => {
                    // Previous row → prev_columns, row 0
                    if *idx < self.prev_columns.len() {
                        stack.push(self.prev_columns[*idx].read(0));
                    } else {
                        stack.push(Value::Null);
                    }
                }
                Instruction::PushLiteral(val) => {
                    stack.push(val.clone());
                }
                Instruction::BinOp(kind) => {
                    let rv = stack.pop().unwrap_or(Value::Null);
                    let lv = stack.pop().unwrap_or(Value::Null);
                    stack.push(eval_binop(*kind, &lv, &rv));
                }
                Instruction::Not => {
                    let val = stack.pop().unwrap_or(Value::Null);
                    stack.push(match val {
                        Value::Bool(b) => Value::Bool(!b),
                        _ => Value::Null,
                    });
                }
                Instruction::IsNull => {
                    let val = stack.pop().unwrap_or(Value::Null);
                    stack.push(Value::Bool(val.is_null()));
                }
            }
        }

        match stack.pop() {
            Some(Value::Bool(b)) => b,
            Some(Value::Null) => true,
            _ => false,
        }
    }
}

/// Evaluate a binary operation
#[inline]
fn eval_binop(kind: BinOpKind, lv: &Value, rv: &Value) -> Value {
    match kind {
        BinOpKind::Or => {
            let lb = matches!(lv, Value::Bool(true));
            if lb {
                return Value::Bool(true);
            }
            Value::Bool(matches!(rv, Value::Bool(true)))
        }
        BinOpKind::And => {
            let lb = matches!(lv, Value::Bool(true));
            if !lb {
                return Value::Bool(false);
            }
            Value::Bool(matches!(rv, Value::Bool(true)))
        }
        BinOpKind::Ne => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(true)
            } else {
                Value::Bool(lv != rv)
            }
        }
        BinOpKind::Eq => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(false)
            } else {
                Value::Bool(lv == rv)
            }
        }
        BinOpKind::Gt => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(false)
            } else {
                Value::Bool(lv > rv)
            }
        }
        BinOpKind::Lt => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(false)
            } else {
                Value::Bool(lv < rv)
            }
        }
        BinOpKind::Ge => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(false)
            } else {
                Value::Bool(lv >= rv)
            }
        }
        BinOpKind::Le => {
            if lv.is_null() || rv.is_null() {
                Value::Bool(false)
            } else {
                Value::Bool(lv <= rv)
            }
        }
        BinOpKind::Sub => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_i64(), rv.as_i64()) {
                    (Some(a), Some(b)) => Value::Int64(a - b),
                    _ => match (lv.as_f64(), rv.as_f64()) {
                        (Some(a), Some(b)) => Value::Float64(a - b),
                        _ => Value::Null,
                    },
                }
            }
        }
        BinOpKind::Add => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_i64(), rv.as_i64()) {
                    (Some(a), Some(b)) => Value::Int64(a + b),
                    _ => match (lv.as_f64(), rv.as_f64()) {
                        (Some(a), Some(b)) => Value::Float64(a + b),
                        _ => Value::Null,
                    },
                }
            }
        }
        BinOpKind::Mul => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_i64(), rv.as_i64()) {
                    (Some(a), Some(b)) => Value::Int64(a * b),
                    _ => match (lv.as_f64(), rv.as_f64()) {
                        (Some(a), Some(b)) => Value::Float64(a * b),
                        _ => Value::Null,
                    },
                }
            }
        }
        BinOpKind::Div => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_f64(), rv.as_f64()) {
                    (Some(a), Some(b)) if b != 0.0 => Value::Float64(a / b),
                    _ => Value::Null,
                }
            }
        }
        BinOpKind::FloorDiv => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_f64(), rv.as_f64()) {
                    (Some(a), Some(b)) if b != 0.0 => Value::Int64((a / b).floor() as i64),
                    _ => Value::Null,
                }
            }
        }
        BinOpKind::Mod => {
            if lv.is_null() || rv.is_null() {
                Value::Null
            } else {
                match (lv.as_i64(), rv.as_i64()) {
                    (Some(a), Some(b)) if b != 0 => Value::Int64(a % b),
                    _ => Value::Null,
                }
            }
        }
    }
}

/// Column lookup table: maps column name → index in the concatenated batch.
/// Kept for potential fallback use but no longer used in the hot path.
#[allow(dead_code)]
struct ColumnLookup {
    /// The single concatenated RecordBatch containing all rows
    batch: RecordBatch,
    /// Map from column name to column index in the batch
    name_to_idx: std::collections::HashMap<String, usize>,
}

#[allow(dead_code)]
impl ColumnLookup {
    fn new(batch: RecordBatch) -> Self {
        let mut name_to_idx = std::collections::HashMap::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            name_to_idx.insert(field.name().clone(), i);
        }
        ColumnLookup { batch, name_to_idx }
    }

    /// Read a column value at a given row
    fn read(&self, col_name: &str, row: usize) -> Value {
        match self.name_to_idx.get(col_name) {
            Some(&idx) => read_array_value(self.batch.column(idx), row),
            None => Value::Null,
        }
    }
}

/// Parse a literal PyExpr into a Value
fn literal_to_value(value: &str, dtype: &str) -> Value {
    match dtype {
        "Int64" | "Int32" | "int" | "UInt64" | "UInt32" => {
            value.parse::<i64>().map(Value::Int64).unwrap_or(Value::Null)
        }
        "Float64" | "Float32" | "float" => value
            .parse::<f64>()
            .map(Value::Float64)
            .unwrap_or(Value::Null),
        "Utf8" | "str" | "String" => Value::Str(value.to_string()),
        "Boolean" | "bool" => match value {
            "True" | "true" | "1" => Value::Bool(true),
            "False" | "false" | "0" => Value::Bool(false),
            _ => Value::Null,
        },
        "NoneType" | "null" | "None" => Value::Null,
        _ => {
            // Try numeric parsing as fallback
            if let Ok(v) = value.parse::<i64>() {
                Value::Int64(v)
            } else if let Ok(v) = value.parse::<f64>() {
                Value::Float64(v)
            } else {
                Value::Str(value.to_string())
            }
        }
    }
}

/// Evaluate a PyExpr for a given row, using `prev_row` for shift(1) lookups.
/// Kept for potential fallback use but no longer used in the hot path.
///
/// - `row`: current row index
/// - `prev_row`: previous row index (None for row 0)
#[allow(dead_code)]
fn evaluate(
    expr: &PyExpr,
    lookup: &ColumnLookup,
    row: usize,
    prev_row: Option<usize>,
) -> Value {
    match expr {
        PyExpr::Column(name) => lookup.read(name, row),

        PyExpr::Literal { value, dtype } => literal_to_value(value, dtype),

        PyExpr::Call {
            func, on, args: _, ..
        } => match func.as_str() {
            "shift" => {
                // shift(1) on a column: read the previous row's value
                if let PyExpr::Column(col_name) = on.as_ref() {
                    match prev_row {
                        Some(prev) => lookup.read(col_name, prev),
                        None => Value::Null, // First row: shift(1) is NULL
                    }
                } else {
                    Value::Null
                }
            }
            "is_null" => {
                let val = evaluate(on, lookup, row, prev_row);
                Value::Bool(val.is_null())
            }
            _ => Value::Null,
        },

        PyExpr::BinOp { op, left, right } => {
            let lv = evaluate(left, lookup, row, prev_row);
            let rv = evaluate(right, lookup, row, prev_row);

            match op.as_str() {
                // Short-circuit logical ops
                "Or" => {
                    let lb = match &lv {
                        Value::Bool(b) => *b,
                        _ => false,
                    };
                    if lb {
                        return Value::Bool(true);
                    }
                    let rb = match &rv {
                        Value::Bool(b) => *b,
                        _ => false,
                    };
                    Value::Bool(rb)
                }
                "And" => {
                    let lb = match &lv {
                        Value::Bool(b) => *b,
                        _ => false,
                    };
                    if !lb {
                        return Value::Bool(false);
                    }
                    let rb = match &rv {
                        Value::Bool(b) => *b,
                        _ => false,
                    };
                    Value::Bool(rb)
                }

                // Comparison ops — handle NULL propagation (NULL != X → NULL → false for boundary)
                "Ne" => {
                    if lv.is_null() || rv.is_null() {
                        // NULL != anything is NULL, but for boundary detection
                        // we want NULL to mean "different" (new group)
                        Value::Bool(true)
                    } else {
                        Value::Bool(lv != rv)
                    }
                }
                "Eq" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Bool(false)
                    } else {
                        Value::Bool(lv == rv)
                    }
                }
                "Gt" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Bool(false)
                    } else {
                        Value::Bool(lv > rv)
                    }
                }
                "Lt" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Bool(false)
                    } else {
                        Value::Bool(lv < rv)
                    }
                }
                "Ge" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Bool(false)
                    } else {
                        Value::Bool(lv >= rv)
                    }
                }
                "Le" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Bool(false)
                    } else {
                        Value::Bool(lv <= rv)
                    }
                }

                // Arithmetic ops
                "Sub" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Null
                    } else {
                        // Try integer arithmetic first (preserves precision)
                        match (lv.as_i64(), rv.as_i64()) {
                            (Some(a), Some(b)) => Value::Int64(a - b),
                            _ => match (lv.as_f64(), rv.as_f64()) {
                                (Some(a), Some(b)) => Value::Float64(a - b),
                                _ => Value::Null,
                            },
                        }
                    }
                }
                "Add" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Null
                    } else {
                        match (lv.as_i64(), rv.as_i64()) {
                            (Some(a), Some(b)) => Value::Int64(a + b),
                            _ => match (lv.as_f64(), rv.as_f64()) {
                                (Some(a), Some(b)) => Value::Float64(a + b),
                                _ => Value::Null,
                            },
                        }
                    }
                }
                "Mul" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Null
                    } else {
                        match (lv.as_i64(), rv.as_i64()) {
                            (Some(a), Some(b)) => Value::Int64(a * b),
                            _ => match (lv.as_f64(), rv.as_f64()) {
                                (Some(a), Some(b)) => Value::Float64(a * b),
                                _ => Value::Null,
                            },
                        }
                    }
                }
                "Div" | "FloorDiv" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Null
                    } else {
                        match (lv.as_f64(), rv.as_f64()) {
                            (Some(a), Some(b)) if b != 0.0 => {
                                if op == "FloorDiv" {
                                    Value::Int64((a / b).floor() as i64)
                                } else {
                                    Value::Float64(a / b)
                                }
                            }
                            _ => Value::Null,
                        }
                    }
                }
                "Mod" => {
                    if lv.is_null() || rv.is_null() {
                        Value::Null
                    } else {
                        match (lv.as_i64(), rv.as_i64()) {
                            (Some(a), Some(b)) if b != 0 => Value::Int64(a % b),
                            _ => Value::Null,
                        }
                    }
                }

                _ => Value::Null,
            }
        }

        PyExpr::UnaryOp { op, operand } => {
            if op == "Not" {
                let val = evaluate(operand, lookup, row, prev_row);
                match val {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }

        PyExpr::Window { .. } => Value::Null,
    }
}

/// Convert a Value to boolean for boundary detection.
/// NULL → true (treat as boundary / new group for first row)
#[allow(dead_code)]
fn value_to_bool(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Null => true,
        _ => false,
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
            extract_referenced_columns(on, cols);
            for arg in args {
                extract_referenced_columns(arg, cols);
            }
        }
        PyExpr::Window { expr, .. } => {
            extract_referenced_columns(expr, cols);
        }
    }
}

// ============================================================================
// Vectorized boundary evaluator — uses Arrow compute kernels
// ============================================================================

/// Evaluate a PyExpr over the entire batch at once using Arrow compute kernels.
/// Returns a BooleanArray where true = boundary (new group starts).
///
/// For each node type:
/// - Column("x") → the array from the batch
/// - shift(1) on Column("x") → array shifted right by 1 (null prepended)
/// - BinOp → Arrow compute kernel (neq, eq, gt, lt, add, sub, etc.)
/// - Literal → scalar array filled with the literal value
///
/// This is ~10x faster than row-by-row evaluation for large batches
/// because it leverages SIMD and avoids per-row Value boxing.
/// Fused boundary evaluation: detect common shifted-comparison patterns and
/// evaluate them in a single pass without creating intermediate arrays.
///
/// Common patterns and their fused evaluation:
/// - `Column != Column.shift(1)` → `arr[i] != arr[i-1]`
/// - `(Column - Column.shift(1)) > Literal` → `arr[i] - arr[i-1] > threshold`
/// - `expr1 | expr2` → combine two sub-evaluations
///
/// Returns Some(BooleanArray) if the expression can be fused, None otherwise.
fn try_fused_boundary_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
) -> Option<BooleanArray> {
    let n = batch.num_rows();
    if n == 0 {
        return Some(BooleanArray::from(Vec::<bool>::new()));
    }

    // Try to fuse the entire expression into a single pass
    let mut result = vec![true; n]; // row 0 always boundary
    if fuse_eval(expr, batch, name_to_idx, &mut result) {
        Some(BooleanArray::from(result))
    } else {
        None
    }
}

/// Recursively fuse-evaluate an expression, writing boolean results into `out`.
/// `out` is pre-filled with `true` for row 0.
/// Returns true if fusion was successful, false if the expression can't be fused.
fn fuse_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    out: &mut Vec<bool>,
) -> bool {
    let n = out.len();
    match expr {
        // Pattern: expr1 | expr2
        PyExpr::BinOp { op, left, right } if op == "Or" => {
            // Evaluate left into temp, right into out, then OR them
            let mut left_out = vec![true; n];
            let mut right_out = vec![true; n];
            if !fuse_eval(left, batch, name_to_idx, &mut left_out) {
                return false;
            }
            if !fuse_eval(right, batch, name_to_idx, &mut right_out) {
                return false;
            }
            out[0] = true; // row 0 always boundary
            for i in 1..n {
                out[i] = left_out[i] || right_out[i];
            }
            true
        }
        // Pattern: expr1 & expr2
        PyExpr::BinOp { op, left, right } if op == "And" => {
            let mut left_out = vec![true; n];
            let mut right_out = vec![true; n];
            if !fuse_eval(left, batch, name_to_idx, &mut left_out) {
                return false;
            }
            if !fuse_eval(right, batch, name_to_idx, &mut right_out) {
                return false;
            }
            out[0] = true;
            for i in 1..n {
                out[i] = left_out[i] && right_out[i];
            }
            true
        }
        // Pattern: Column != Column.shift(1)
        PyExpr::BinOp { op, left, right } if op == "Ne" => {
            if let (Some(col_name), true) = (get_column_name(left), is_shift_of_same_column(left, right)) {
                let idx = match name_to_idx.get(col_name) {
                    Some(i) => *i,
                    None => return false,
                };
                let col = batch.column(idx);
                if let Some(i64_arr) = coerce_to_i64(col) {
                    let vals = i64_arr.values();
                    let nulls = i64_arr.nulls();
                    out[0] = true;
                    if let Some(nb) = nulls {
                        for i in 1..n {
                            out[i] = !nb.is_valid(i) || !nb.is_valid(i - 1) || vals[i] != vals[i - 1];
                        }
                    } else {
                        for i in 1..n {
                            out[i] = vals[i] != vals[i - 1];
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        // Pattern: (Column - Column.shift(1)) > Literal
        PyExpr::BinOp { op, left, right } if op == "Gt" => {
            // Check if left is Sub(Column, Column.shift(1)) and right is Literal
            if let PyExpr::BinOp { op: sub_op, left: sub_left, right: sub_right } = left.as_ref() {
                if sub_op == "Sub" {
                    if let (Some(col_name), true) = (get_column_name(sub_left), is_shift_of_same_column(sub_left, sub_right)) {
                        if let Some(threshold) = get_literal_i64(right) {
                            let idx = match name_to_idx.get(col_name) {
                                Some(i) => *i,
                                None => return false,
                            };
                            let col = batch.column(idx);
                            if let Some(i64_arr) = coerce_to_i64(col) {
                                let vals = i64_arr.values();
                                let nulls = i64_arr.nulls();
                                out[0] = true;
                                if let Some(nb) = nulls {
                                    for i in 1..n {
                                        out[i] = !nb.is_valid(i) || !nb.is_valid(i - 1)
                                            || vals[i].wrapping_sub(vals[i - 1]) > threshold;
                                    }
                                } else {
                                    for i in 1..n {
                                        out[i] = vals[i].wrapping_sub(vals[i - 1]) > threshold;
                                    }
                                }
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Extract column name from a PyExpr::Column
fn get_column_name(expr: &PyExpr) -> Option<&str> {
    match expr {
        PyExpr::Column(name) => Some(name.as_str()),
        _ => None,
    }
}

/// Check if `right` is `shift(1)` of the same column as `left`
fn is_shift_of_same_column(left: &PyExpr, right: &PyExpr) -> bool {
    if let PyExpr::Column(left_name) = left {
        if let PyExpr::Call { func, on, .. } = right {
            if func == "shift" {
                if let PyExpr::Column(right_name) = on.as_ref() {
                    return left_name == right_name;
                }
            }
        }
    }
    false
}

/// Extract an i64 literal value from a PyExpr::Literal
fn get_literal_i64(expr: &PyExpr) -> Option<i64> {
    match expr {
        PyExpr::Literal { value, dtype } => {
            // value is a String representation; parse based on dtype
            match dtype.as_str() {
                "Int64" | "Int32" | "Int16" | "Int8" => value.parse::<i64>().ok(),
                "Float64" | "Float32" => value.parse::<f64>().ok().map(|f| f as i64),
                _ => value.parse::<i64>().ok(),
            }
        }
        _ => None,
    }
}

// ============================================================================
// Streaming fuse_eval — cross-batch boundary detection for sequential execution
// ============================================================================

/// State carried across batches during streaming boundary detection.
///
/// Enables `fuse_eval` to correctly detect boundaries at batch boundaries
/// by remembering the last row's column values from the previous batch.
struct StreamState {
    /// Last row's column values from the previous batch (keyed by column name).
    /// `None` means the column value was NULL or no previous batch exists.
    prev_values: HashMap<String, Option<i64>>,
    /// Current group ID (incremented on each boundary).
    current_gid: i64,
}

impl StreamState {
    fn new() -> Self {
        StreamState {
            prev_values: HashMap::new(),
            current_gid: 0,
        }
    }

    /// Save the last row's column values from the current batch.
    fn save_last_row(&mut self, batch: &RecordBatch, name_to_idx: &HashMap<String, usize>) {
        let last = batch.num_rows() - 1;
        for (name, idx) in name_to_idx {
            let col = batch.column(*idx);
            if col.is_null(last) {
                self.prev_values.insert(name.clone(), None);
            } else if let Some(i64_arr) = coerce_to_i64(col) {
                self.prev_values.insert(name.clone(), Some(i64_arr.value(last)));
            }
        }
    }
}

/// Streaming version of `fuse_eval` that handles cross-batch boundaries.
///
/// Like `fuse_eval`, but row 0 of each batch compares against `state.prev_values`
/// instead of unconditionally being `true`. First batch (empty prev_values) still
/// marks row 0 as boundary.
fn streaming_fuse_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    state: &StreamState,
    out: &mut Vec<bool>,
) -> bool {
    let n = out.len();
    match expr {
        // Pattern: expr1 | expr2
        PyExpr::BinOp { op, left, right } if op == "Or" => {
            let mut left_out = vec![false; n];
            let mut right_out = vec![false; n];
            if !streaming_fuse_eval(left, batch, name_to_idx, state, &mut left_out) {
                return false;
            }
            if !streaming_fuse_eval(right, batch, name_to_idx, state, &mut right_out) {
                return false;
            }
            for i in 0..n {
                out[i] = left_out[i] || right_out[i];
            }
            // First batch, first row is always a boundary
            if state.prev_values.is_empty() {
                out[0] = true;
            }
            true
        }
        // Pattern: expr1 & expr2
        PyExpr::BinOp { op, left, right } if op == "And" => {
            let mut left_out = vec![false; n];
            let mut right_out = vec![false; n];
            if !streaming_fuse_eval(left, batch, name_to_idx, state, &mut left_out) {
                return false;
            }
            if !streaming_fuse_eval(right, batch, name_to_idx, state, &mut right_out) {
                return false;
            }
            for i in 0..n {
                out[i] = left_out[i] && right_out[i];
            }
            // First batch, first row is always a boundary
            if state.prev_values.is_empty() {
                out[0] = true;
            }
            true
        }
        // Pattern: Column != Column.shift(1)
        PyExpr::BinOp { op, left, right } if op == "Ne" => {
            if let (Some(col_name), true) = (get_column_name(left), is_shift_of_same_column(left, right)) {
                let idx = match name_to_idx.get(col_name) {
                    Some(i) => *i,
                    None => return false,
                };
                let col = batch.column(idx);
                if let Some(i64_arr) = coerce_to_i64(col) {
                    let vals = i64_arr.values();
                    let nulls = i64_arr.nulls();

                    // Row 0: compare against previous batch's last value
                    if state.prev_values.is_empty() {
                        out[0] = true; // First batch: always boundary
                    } else {
                        let cur_null = nulls.map_or(false, |nb| !nb.is_valid(0));
                        match state.prev_values.get(col_name) {
                            Some(Some(prev_val)) => {
                                out[0] = cur_null || vals[0] != *prev_val;
                            }
                            _ => {
                                out[0] = true; // prev was null → boundary
                            }
                        }
                    }

                    // Rows 1..n: same as non-streaming fuse_eval
                    if let Some(nb) = nulls {
                        for i in 1..n {
                            out[i] = !nb.is_valid(i) || !nb.is_valid(i - 1) || vals[i] != vals[i - 1];
                        }
                    } else {
                        for i in 1..n {
                            out[i] = vals[i] != vals[i - 1];
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        // Pattern: (Column - Column.shift(1)) > Literal
        PyExpr::BinOp { op, left, right } if op == "Gt" => {
            if let PyExpr::BinOp { op: sub_op, left: sub_left, right: sub_right } = left.as_ref() {
                if sub_op == "Sub" {
                    if let (Some(col_name), true) = (get_column_name(sub_left), is_shift_of_same_column(sub_left, sub_right)) {
                        if let Some(threshold) = get_literal_i64(right) {
                            let idx = match name_to_idx.get(col_name) {
                                Some(i) => *i,
                                None => return false,
                            };
                            let col = batch.column(idx);
                            if let Some(i64_arr) = coerce_to_i64(col) {
                                let vals = i64_arr.values();
                                let nulls = i64_arr.nulls();

                                // Row 0: compare against previous batch's last value
                                if state.prev_values.is_empty() {
                                    out[0] = true;
                                } else {
                                    let cur_null = nulls.map_or(false, |nb| !nb.is_valid(0));
                                    match state.prev_values.get(col_name) {
                                        Some(Some(prev_val)) => {
                                            out[0] = cur_null || vals[0].wrapping_sub(*prev_val) > threshold;
                                        }
                                        _ => {
                                            out[0] = true;
                                        }
                                    }
                                }

                                // Rows 1..n
                                if let Some(nb) = nulls {
                                    for i in 1..n {
                                        out[i] = !nb.is_valid(i) || !nb.is_valid(i - 1)
                                            || vals[i].wrapping_sub(vals[i - 1]) > threshold;
                                    }
                                } else {
                                    for i in 1..n {
                                        out[i] = vals[i].wrapping_sub(vals[i - 1]) > threshold;
                                    }
                                }
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Try streaming fused boundary evaluation on a batch.
/// Returns Some(BooleanArray) if fusion succeeds, None otherwise.
fn try_streaming_fused_boundary_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
    state: &StreamState,
) -> Option<BooleanArray> {
    let n = batch.num_rows();
    if n == 0 {
        return Some(BooleanArray::from(Vec::<bool>::new()));
    }

    let mut result = vec![false; n]; // streaming: row 0 NOT unconditionally true
    if streaming_fuse_eval(expr, batch, name_to_idx, state, &mut result) {
        Some(BooleanArray::from(result))
    } else {
        None
    }
}

fn vectorized_boundary_eval(
    expr: &PyExpr,
    batch: &RecordBatch,
    name_to_idx: &HashMap<String, usize>,
) -> Result<BooleanArray, String> {
    // Try fused single-pass evaluation first (avoids intermediate arrays)
    if let Some(fused) = try_fused_boundary_eval(expr, batch, name_to_idx) {
        return Ok(fused);
    }

    // Fallback: multi-pass vectorized evaluation with intermediate arrays
    let arr = vectorized_eval_expr(expr, batch, name_to_idx)?;
    
    // Convert result to BooleanArray
    let n = batch.num_rows();
    if let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() {
        // Set row 0 to true (always a boundary)
        let mut result = Vec::with_capacity(n);
        result.push(true);
        for i in 1..n {
            if bool_arr.is_null(i) {
                result.push(true); // NULL → boundary
            } else {
                result.push(bool_arr.value(i));
            }
        }
        Ok(BooleanArray::from(result))
    } else {
        Err("Vectorized evaluation did not produce a BooleanArray".to_string())
    }
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
        
        PyExpr::Literal { value, dtype } => {
            let n = batch.num_rows();
            let val = literal_to_value(value, dtype);
            match val {
                Value::Int64(v) => Ok(Arc::new(Int64Array::from(vec![v; n])) as ArrayRef),
                Value::Float64(v) => Ok(Arc::new(Float64Array::from(vec![v; n])) as ArrayRef),
                Value::Bool(v) => Ok(Arc::new(BooleanArray::from(vec![v; n])) as ArrayRef),
                Value::Str(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); n])) as ArrayRef),
                Value::Null => Ok(new_null_array(&DataType::Int64, n)),
            }
        }
        
        PyExpr::Call { func, on, args: _, .. } => {
            match func.as_str() {
                "shift" => {
                    // shift(1): prepend null, drop last element
                    let source = vectorized_eval_expr(on, batch, name_to_idx)?;
                    shift_array_by_1(&source)
                }
                "is_null" => {
                    let source = vectorized_eval_expr(on, batch, name_to_idx)?;
                    let n = source.len();
                    let mut result = Vec::with_capacity(n);
                    for i in 0..n {
                        result.push(source.is_null(i));
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
                        let l_null = l_nulls.map_or(false, |nb| !nb.is_valid(i));
                        let r_null = r_nulls.map_or(false, |nb| !nb.is_valid(i));
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
                        let l_valid = l_nulls.map_or(true, |nb| nb.is_valid(i));
                        let r_valid = r_nulls.map_or(true, |nb| nb.is_valid(i));
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

/// Build DataFusion sort expressions from the table's `sort_exprs` column names.
pub(crate) fn build_sort_exprs(sort_keys: &[String]) -> Vec<SortExpr> {
    sort_keys
        .iter()
        .map(|col_name| SortExpr {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect()
}

/// Single-pass group ID assignment with `__group_count__` and `__rn__`.
///
/// Optimized two-phase approach:
///
/// **Phase A** (boundary detection — lightweight):
///   1. Extract only the columns referenced in the predicate + sort keys
///   2. Project + Sort (DataFusion uses SortPreservingMerge for pre-sorted Parquet)
///   3. Collect the small projection → process batches sequentially (no concat)
///   4. Compute group_id, group_count, rn arrays
///
/// **Phase B** (result assembly):
///   5. Return metadata-only table with __group_id__, __group_count__, __rn__
///
/// **Streaming fast path**: When data comes from a pre-sorted Parquet file
/// (`source_parquet_path` + `sort_exprs`), uses `target_partitions=1` +
/// `execute_stream()` to avoid the 32-partition SortPreservingMerge overhead
/// and process batches individually without `concat_batches`.
pub fn linear_scan_group_id(table: &LTSeqTable, predicate: &PyExpr) -> PyResult<LTSeqTable> {
    // ── Streaming fast path for pre-sorted Parquet ───────────────────────
    //
    // Conditions:
    //   1. Data comes from a Parquet file (source_parquet_path is set)
    //   2. Sort order is declared (sort_exprs is non-empty)
    //   3. The predicate can be fuse-evaluated (common boundary patterns)
    //
    // Benefits over the general path:
    //   - target_partitions=1: no SortPreservingMerge (single partition preserves order)
    //   - execute_stream(): batch-by-batch processing, no concat_batches
    //   - Skip .sort() node: single partition + file_sort_order is sufficient
    if let Some(ref parquet_path) = table.source_parquet_path {
        if !table.sort_exprs.is_empty() {
            return streaming_linear_scan_group_id(
                table,
                predicate,
                parquet_path,
            );
        }
    }

    // ── General path (non-Parquet or unsorted data) ─────────────────────
    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

    // ── Phase A: Lightweight boundary detection ──────────────────────────

    // Step 1: Extract columns needed by the predicate
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    // Only include sort keys that are referenced by the predicate.
    // Non-referenced sort keys don't affect boundary detection and reading
    // them from Parquet is wasteful (e.g., watchid adds ~30% I/O overhead).
    let relevant_sort_exprs: Vec<SortExpr> = table
        .sort_exprs
        .iter()
        .filter(|key| needed_cols.contains(key.as_str()))
        .map(|col_name| SortExpr {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect();

    // Step 2: Project to only needed columns + sort
    let col_exprs: Vec<Expr> = needed_cols
        .iter()
        .map(|name| Expr::Column(Column::new_unqualified(name)))
        .collect();

    let projected_df = if col_exprs.is_empty() {
        (**df).clone()
    } else {
        (**df).clone().select(col_exprs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to project columns for linear scan: {}",
                e
            ))
        })?
    };

    let sorted_projected = if relevant_sort_exprs.is_empty() {
        projected_df
    } else {
        projected_df.sort(relevant_sort_exprs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to sort projected data: {}",
                e
            ))
        })?
    };

    // Step 3: Collect the small projection
    let proj_batches = RUNTIME
        .block_on(async {
            sorted_projected
                .collect()
                .await
                .map_err(|e| format!("Failed to collect projected data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if proj_batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let total_rows: usize = proj_batches.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    // Step 4: Boundary detection — concat batches then use vectorized SIMD evaluation.
    // concat_batches is cheap for small projections (2-3 columns) and the vectorized
    // path using Arrow compute kernels is ~2x faster than per-row CompiledEvaluator.
    let schema = proj_batches[0].schema();
    let concat_batch = concat_batches(&schema, &proj_batches).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to concatenate projected batches: {}",
            e
        ))
    })?;

    // Build column name → index mapping
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, field) in concat_batch.schema().fields().iter().enumerate() {
        name_to_idx.insert(field.name().clone(), i);
    }

    // Vectorized boundary detection using Arrow compute kernels (SIMD-accelerated)
    let boundaries = vectorized_boundary_eval(predicate, &concat_batch, &name_to_idx)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Vectorized boundary evaluation failed: {}",
                e
            ))
        })?;

    build_group_metadata_from_boundaries(&boundaries, total_rows, table)
}

/// Streaming fast path: read pre-sorted Parquet with single partition,
/// process batches via execute_stream() with cross-batch boundary state.
fn streaming_linear_scan_group_id(
    table: &LTSeqTable,
    predicate: &PyExpr,
    parquet_path: &str,
) -> PyResult<LTSeqTable> {
    // Step 1: Extract columns needed by the predicate
    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    // Step 2: Create single-partition session and read Parquet with declared sort order
    let seq_session = create_sequential_session();

    // Build file_sort_order from sort_exprs (same as assume_sorted_impl)
    let sort_order: Vec<Vec<SortExpr>> = vec![table
        .sort_exprs
        .iter()
        .map(|col_name| SortExpr {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect()];

    let col_names: Vec<String> = needed_cols.into_iter().collect();

    let result = RUNTIME.block_on(async {
        // Read Parquet with sort order metadata (single partition)
        let options = ParquetReadOptions::default().file_sort_order(sort_order);
        let df = seq_session
            .read_parquet(parquet_path, options)
            .await
            .map_err(|e| format!("Failed to read Parquet: {}", e))?;

        // Project to only needed columns (DataFusion handles column pruning at Parquet level)
        let col_exprs: Vec<Expr> = col_names
            .iter()
            .map(|name| Expr::Column(Column::new_unqualified(name)))
            .collect();

        let projected_df = if col_exprs.is_empty() {
            df
        } else {
            df.select(col_exprs)
                .map_err(|e| format!("Failed to project: {}", e))?
        };

        // No .sort() needed: single partition + file_sort_order preserves order natively

        // Step 3: Execute as stream — batch-by-batch processing
        let mut stream = projected_df
            .execute_stream()
            .await
            .map_err(|e| format!("Failed to create stream: {}", e))?;

        // Step 4: Streaming boundary detection with cross-batch state
        let mut state = StreamState::new();
        let mut group_ids: Vec<i64> = Vec::new();
        let mut name_to_idx: HashMap<String, usize> = HashMap::new();
        let mut idx_built = false;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| format!("Stream error: {}", e))?;
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }

            // Build name_to_idx from first batch
            if !idx_built {
                for (i, field) in batch.schema().fields().iter().enumerate() {
                    name_to_idx.insert(field.name().clone(), i);
                }
                idx_built = true;
            }

            // Try streaming fused boundary evaluation
            let boundaries = match try_streaming_fused_boundary_eval(
                predicate, &batch, &name_to_idx, &state,
            ) {
                Some(b) => b,
                None => {
                    // Fallback: can't fuse this expression in streaming mode.
                    // Return error to trigger general path (should not happen for
                    // R2 sessionization predicates).
                    return Err(
                        "STREAMING_FALLBACK".to_string()
                    );
                }
            };

            // Accumulate group IDs from boundaries
            for i in 0..n {
                if boundaries.value(i) {
                    state.current_gid += 1;
                }
                group_ids.push(state.current_gid);
            }

            // Save last row's column values for cross-batch boundary detection
            state.save_last_row(&batch, &name_to_idx);
        }

        Ok(group_ids)
    });

    // Handle fallback: if streaming fuse_eval couldn't handle the expression,
    // fall back to the general path
    let group_ids = match result {
        Ok(ids) => ids,
        Err(e) if e == "STREAMING_FALLBACK" => {
            return general_linear_scan_group_id(table, predicate);
        }
        Err(e) => {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e));
        }
    };

    let total_rows = group_ids.len();
    if total_rows == 0 {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    // Step 5: Build metadata from accumulated group IDs
    let current_gid = *group_ids.last().unwrap();
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

/// Fallback: general path for when streaming can't handle the expression.
/// This is identical to the non-streaming path in linear_scan_group_id.
fn general_linear_scan_group_id(
    table: &LTSeqTable,
    predicate: &PyExpr,
) -> PyResult<LTSeqTable> {
    let df = table
        .dataframe
        .as_ref()
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No data loaded"))?;

    let mut needed_cols: HashSet<String> = HashSet::new();
    extract_referenced_columns(predicate, &mut needed_cols);

    let relevant_sort_exprs: Vec<SortExpr> = table
        .sort_exprs
        .iter()
        .filter(|key| needed_cols.contains(key.as_str()))
        .map(|col_name| SortExpr {
            expr: Expr::Column(Column::new_unqualified(col_name)),
            asc: true,
            nulls_first: true,
        })
        .collect();

    let col_exprs: Vec<Expr> = needed_cols
        .iter()
        .map(|name| Expr::Column(Column::new_unqualified(name)))
        .collect();

    let projected_df = if col_exprs.is_empty() {
        (**df).clone()
    } else {
        (**df).clone().select(col_exprs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to project columns for linear scan: {}",
                e
            ))
        })?
    };

    let sorted_projected = if relevant_sort_exprs.is_empty() {
        projected_df
    } else {
        projected_df.sort(relevant_sort_exprs).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to sort projected data: {}",
                e
            ))
        })?
    };

    let proj_batches = RUNTIME
        .block_on(async {
            sorted_projected
                .collect()
                .await
                .map_err(|e| format!("Failed to collect projected data: {}", e))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if proj_batches.is_empty() {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let total_rows: usize = proj_batches.iter().map(|b| b.num_rows()).sum();

    if total_rows == 0 {
        return Ok(LTSeqTable::empty(
            Arc::clone(&table.session),
            table.schema.as_ref().map(Arc::clone),
            Vec::new(),
            table.source_parquet_path.clone(),
        ));
    }

    let schema = proj_batches[0].schema();
    let concat_batch = concat_batches(&schema, &proj_batches).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to concatenate projected batches: {}",
            e
        ))
    })?;

    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, field) in concat_batch.schema().fields().iter().enumerate() {
        name_to_idx.insert(field.name().clone(), i);
    }

    let boundaries = vectorized_boundary_eval(predicate, &concat_batch, &name_to_idx)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Vectorized boundary evaluation failed: {}",
                e
            ))
        })?;

    build_group_metadata_from_boundaries(&boundaries, total_rows, table)
}

/// Build group metadata arrays from a BooleanArray of boundaries.
fn build_group_metadata_from_boundaries(
    boundaries: &BooleanArray,
    total_rows: usize,
    table: &LTSeqTable,
) -> PyResult<LTSeqTable> {
    // Compute group IDs from boundaries via prefix sum
    let mut group_ids: Vec<i64> = Vec::with_capacity(total_rows);
    let mut current_gid: i64 = 0;
    for i in 0..total_rows {
        if boundaries.value(i) {
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
) -> PyResult<LTSeqTable> {
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
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create group metadata batch: {}",
            e
        ))
    })?;

    // Return as LTSeqTable from the metadata batch
    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        vec![meta_batch],
        Vec::new(),
        table.source_parquet_path.clone(),
    )
}
