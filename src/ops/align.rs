//! Align operation for LTSeqTable
//!
//! Aligns table rows to a reference sequence using a native DataFusion
//! LEFT JOIN (issue #91 PR 5): the reference sequence becomes an in-memory
//! DataFrame via `SessionContext::read_batch` (no table registration, no SQL
//! text), joined against the lazy table plan.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, JoinType, SortExpr};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use std::sync::Arc;

/// Build the `__ref_key__` array with the KEY COLUMN's exact Arrow type, so
/// the join compares values natively instead of the legacy CAST-to-VARCHAR
/// string equality.
///
/// Values are first collected into the widest array of the key's type family
/// (i64 / u64 / f64 / utf8), then cast to the exact key type with
/// `safe: false` so out-of-range values fail loudly instead of becoming
/// NULLs. Reference values that don't convert are a validation error (the
/// legacy behavior silently string-compared them); key column types outside
/// the integer/float/string families are explicitly unsupported.
fn build_ref_key_array(
    py: Python<'_>,
    ref_sequence: &[Py<PyAny>],
    key_type: &DataType,
    key_col: &str,
) -> PyResult<ArrayRef> {
    let type_err = |pos: usize, val: &str| {
        PyErr::from(LtseqError::Validation(format!(
            "align(): ref_sequence[{}] = {} cannot be converted to key column \
             '{}' type {:?}",
            pos, val, key_col, key_type
        )))
    };

    let wide: ArrayRef = match key_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut vals = Vec::with_capacity(ref_sequence.len());
            for (pos, obj) in ref_sequence.iter().enumerate() {
                let bound = obj.bind(py);
                let v: i64 = bound
                    .extract()
                    .map_err(|_| type_err(pos, &bound.to_string()))?;
                vals.push(v);
            }
            Arc::new(Int64Array::from(vals))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let mut vals = Vec::with_capacity(ref_sequence.len());
            for (pos, obj) in ref_sequence.iter().enumerate() {
                let bound = obj.bind(py);
                let v: u64 = bound
                    .extract()
                    .map_err(|_| type_err(pos, &bound.to_string()))?;
                vals.push(v);
            }
            Arc::new(UInt64Array::from(vals))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let mut vals = Vec::with_capacity(ref_sequence.len());
            for (pos, obj) in ref_sequence.iter().enumerate() {
                let bound = obj.bind(py);
                let v: f64 = bound
                    .extract()
                    .map_err(|_| type_err(pos, &bound.to_string()))?;
                vals.push(v);
            }
            Arc::new(Float64Array::from(vals))
        }
        // Strings — and temporal types, whose reference values arrive as
        // strings (or stringifiable objects like datetime.date) and are
        // parsed into the key type by the arrow cast below. This preserves
        // the documented usage align(["2024-01-01"], key=lambda r: r.date)
        // against a Date32 column, with real typed comparison instead of the
        // legacy CAST-to-VARCHAR string equality.
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_) => {
            let mut vals = Vec::with_capacity(ref_sequence.len());
            for obj in ref_sequence.iter() {
                let bound = obj.bind(py);
                let v: String = match bound.extract::<String>() {
                    Ok(s) => s,
                    Err(_) => bound.str()?.to_string(),
                };
                vals.push(v);
            }
            Arc::new(StringArray::from(vals))
        }
        other => {
            return Err(LtseqError::Validation(format!(
                "align(): key column '{}' has unsupported type {:?}; \
                 supported key types are integers, floats, strings and \
                 dates/timestamps",
                key_col, other
            ))
            .into())
        }
    };

    // Narrow to the exact key type. safe=false: an out-of-range reference
    // value is an error, never a silent NULL (which would corrupt the join).
    if wide.data_type() == key_type {
        return Ok(wide);
    }
    datafusion::arrow::compute::kernels::cast::cast_with_options(
        &wide,
        key_type,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(|e| {
        LtseqError::Validation(format!(
            "align(): ref_sequence contains values outside key column '{}' \
             type {:?}: {}",
            key_col, key_type, e
        ))
        .into()
    })
}

/// Align table rows to a reference sequence
///
/// Creates a result table where:
/// - Rows are reordered to match ref_sequence order
/// - NULL rows inserted for keys not in the original table
/// - Rows with keys not in ref_sequence are excluded
///
/// Native plan: ref DataFrame (`__ref_key__`, `__pos__`) LEFT JOIN table
/// ON `__ref_key__ = key_col`, ORDER BY `__pos__`, project the table columns.
pub fn align_impl(
    table: &LTSeqTable,
    ref_sequence: Vec<Py<PyAny>>,
    key_col: &str,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    // Validate key column exists
    let key_field = schema.field_with_name(key_col).map_err(|_| {
        LtseqError::Validation(format!(
            "Key column '{}' not found in schema. Available: {:?}",
            key_col,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })?;

    // Build the reference DataFrame: typed __ref_key__ + __pos__ position.
    let ref_key_array = Python::attach(|py| {
        build_ref_key_array(py, &ref_sequence, key_field.data_type(), key_col)
    })?;
    let pos_array: ArrayRef = Arc::new(UInt64Array::from(
        (0..ref_sequence.len() as u64).collect::<Vec<_>>(),
    ));

    let ref_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("__ref_key__", ref_key_array.data_type().clone(), true),
        Field::new("__pos__", DataType::UInt64, false),
    ]));
    let ref_batch = RecordBatch::try_new(ref_schema, vec![ref_key_array, pos_array])
        .map_err(|e| LtseqError::Runtime(format!("Failed to build ref batch: {}", e)))?;

    let ref_df = table
        .session
        .read_batch(ref_batch)
        .map_err(|e| LtseqError::Runtime(format!("Failed to read ref batch: {}", e)))?;

    // Native LEFT JOIN + ORDER BY __pos__ + project the table's columns.
    let table_cols: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();

    let result_df = RUNTIME
        .block_on(async {
            ref_df
                .join(
                    (**df).clone(),
                    JoinType::Left,
                    &["__ref_key__"],
                    &[key_col],
                    None,
                )
                .and_then(|d| {
                    d.sort(vec![SortExpr::new(
                        Expr::Column(Column::new_unqualified("__pos__")),
                        true,
                        false,
                    )])
                })
                .and_then(|d| d.select(table_cols))
                .map_err(|e| format!("Align join failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    // Alignment defines a new row order driven by the reference sequence:
    // no sort metadata, no fast-path token.
    Ok(LTSeqTable::from_df(
        Arc::clone(&table.session),
        result_df,
        Vec::new(),
        None,
    ))
}
