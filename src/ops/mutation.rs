//! Row-level mutation operations: insert_row, delete_rows, conditional_update, modify_row

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::LTSeqTable;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::common::ScalarValue;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

// ============================================================================
// insert_row: Insert a row at a given position
// ============================================================================

pub fn insert_row_impl(
    table: &LTSeqTable,
    pos: i64,
    row_dict: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let batches = RUNTIME
        .block_on(async { (**df).clone().collect().await })
        .map_err(|e| LtseqError::with_context("Failed to collect data", e))?;

    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let pos = pos.clamp(0, num_rows as i64) as usize;

    let new_batch = row_dict_to_batch(row_dict, schema)?;

    if num_rows == 0 {
        return LTSeqTable::from_batches(
            Arc::clone(&table.session),
            vec![new_batch],
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        );
    }

    let mut result_batches = Vec::new();
    let mut row_offset = 0usize;
    let mut inserted = false;

    for batch in &batches {
        let batch_len = batch.num_rows();
        if inserted {
            result_batches.push(batch.clone());
            row_offset += batch_len;
            continue;
        }

        let insert_pos_in_batch = pos.saturating_sub(row_offset);
        if insert_pos_in_batch < batch_len {
            if insert_pos_in_batch > 0 {
                result_batches.push(batch.slice(0, insert_pos_in_batch));
            }
            result_batches.push(new_batch.clone());
            if insert_pos_in_batch < batch_len {
                result_batches.push(batch.slice(insert_pos_in_batch, batch_len - insert_pos_in_batch));
            }
            inserted = true;
        } else {
            result_batches.push(batch.clone());
        }
        row_offset += batch_len;
    }

    if !inserted {
        result_batches.push(new_batch);
    }

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        result_batches,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    )
}

// ============================================================================
// delete_rows: Delete row at a given position
// ============================================================================

pub fn delete_rows_impl(table: &LTSeqTable, pos: i64) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let batches = RUNTIME
        .block_on(async { (**df).clone().collect().await })
        .map_err(|e| LtseqError::with_context("Failed to collect data", e))?;

    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if pos < 0 || pos as usize >= num_rows {
        return Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&table.session),
            (**df).clone(),
            Arc::clone(schema),
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ));
    }

    let pos = pos as usize;
    let mut result_batches = Vec::new();
    let mut row_offset = 0usize;

    for batch in &batches {
        let batch_len = batch.num_rows();
        let delete_pos_in_batch = pos.saturating_sub(row_offset);

        if delete_pos_in_batch >= batch_len {
            result_batches.push(batch.clone());
        } else {
            if delete_pos_in_batch > 0 {
                result_batches.push(batch.slice(0, delete_pos_in_batch));
            }
            let after_start = delete_pos_in_batch + 1;
            if after_start < batch_len {
                result_batches.push(batch.slice(after_start, batch_len - after_start));
            }
        }
        row_offset += batch_len;
    }

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        result_batches,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    )
}

// ============================================================================
// conditional_update: Update columns where predicate is True
// ============================================================================

pub fn conditional_update_impl(
    table: &LTSeqTable,
    expr_dict: &Bound<'_, PyDict>,
    updates: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    if updates.is_empty() {
        return Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&table.session),
            (**df).clone(),
            Arc::clone(schema),
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ));
    }

    let predicate_expr = crate::transpiler::pyexpr_to_datafusion(
        crate::types::dict_to_py_expr(expr_dict)?,
        schema,
    )
    .map_err(LtseqError::Validation)?;

    let mut update_map = std::collections::HashMap::<String, ScalarValue>::new();
    for (key, val) in updates.iter() {
        let col_name: String = key
            .extract()
            .map_err(|e| LtseqError::with_context("Invalid column name", e))?;
        let field = schema.field_with_name(&col_name).map_err(|e| {
            LtseqError::Validation(format!("Column '{}' not found: {}", col_name, e))
        })?;
        let dt = field.data_type();
        let scalar = python_value_to_scalar(&val, dt).ok_or_else(|| {
            LtseqError::Validation(format!("Cannot convert value for column '{}'", col_name))
        })?;
        update_map.insert(col_name, scalar);
    }

    let result_df = RUNTIME
        .block_on(async {
            use datafusion::common::Column;
            use datafusion::logical_expr::case;
            use datafusion::prelude::lit;

            let mut select_exprs = Vec::new();

            for field in schema.fields() {
                let col_name = field.name();
                let col_expr = datafusion::prelude::Expr::Column(Column::new_unqualified(col_name));

                if let Some(scalar) = update_map.get(col_name) {
                    let updated_expr = case(predicate_expr.clone())
                        .when(lit(true), lit(scalar.clone()))
                        .otherwise(col_expr.clone())
                        .map_err(|e| format!("Failed to build CASE for '{}': {}", col_name, e))?
                        .alias(col_name);
                    select_exprs.push(updated_expr);
                } else {
                    select_exprs.push(col_expr);
                }
            }

            (**df)
                .clone()
                .select(select_exprs)
                .map_err(|e| format!("Conditional update execution failed: {}", e))
        })
        .map_err(LtseqError::Runtime)?;

    Ok(LTSeqTable::from_df_with_schema(
        Arc::clone(&table.session),
        result_df,
        Arc::clone(schema),
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    ))
}

// ============================================================================
// modify_row: Modify specific columns at a row position
// ============================================================================

pub fn modify_row_impl(
    table: &LTSeqTable,
    pos: i64,
    updates: &Bound<'_, PyDict>,
) -> PyResult<LTSeqTable> {
    let (df, schema) = table.require_df_and_schema()?;

    let batches = RUNTIME
        .block_on(async { (**df).clone().collect().await })
        .map_err(|e| LtseqError::with_context("Failed to collect data", e))?;

    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if pos < 0 || pos as usize >= num_rows || updates.is_empty() {
        return Ok(LTSeqTable::from_df_with_schema(
            Arc::clone(&table.session),
            (**df).clone(),
            Arc::clone(schema),
            table.sort_exprs.clone(),
            table.source_parquet_path.clone(),
        ));
    }

    let pos = pos as usize;

    let update_map: Vec<(String, ScalarValue)> = updates
        .iter()
        .filter_map(|(key, val)| {
            let col_name: String = key.extract().ok()?;
            let field = schema.field_with_name(&col_name).ok()?;
            let dt = field.data_type();
            let scalar = python_value_to_scalar(&val, dt)?;
            Some((col_name, scalar))
        })
        .collect();

    let mut result_batches = Vec::new();
    let mut row_offset = 0usize;

    for batch in &batches {
        let batch_len = batch.num_rows();
        let row_start = row_offset;
        let row_end = row_offset + batch_len;

        if pos < row_start || pos >= row_end {
            result_batches.push(batch.clone());
        } else {
            let local_pos = pos - row_start;

            let mut new_columns: Vec<ArrayRef> = Vec::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let col = batch.column(i).clone();
                let col_name = field.name().as_str();

                if let Some((_, scalar)) = update_map.iter().find(|(c, _)| c == col_name) {
                    let new_val: ArrayRef = scalar.to_array_of_size(1)
                        .map_err(|e| LtseqError::with_context("Failed to create scalar array", e))?;
                    let updated_col = compute_replace_at(&col, local_pos, &new_val.slice(0, 1))
                        .map_err(|e| LtseqError::with_context("replace_at failed", e))?;
                    new_columns.push(updated_col);
                } else {
                    new_columns.push(col);
                }
            }
            let new_batch = RecordBatch::try_new(Arc::clone(schema), new_columns)
                .map_err(|e| LtseqError::with_context("Failed to create batch", e))?;
            result_batches.push(new_batch);
        }
        row_offset += batch_len;
    }

    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        result_batches,
        table.sort_exprs.clone(),
        table.source_parquet_path.clone(),
    )
}

// ============================================================================
// Helpers
// ============================================================================

fn row_dict_to_batch(
    row_dict: &Bound<'_, PyDict>,
    schema: &Arc<Schema>,
) -> PyResult<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    for field in schema.fields() {
        let col_name = field.name().as_str();
        let dt = field.data_type();
        let val_opt = row_dict.get_item(col_name)?;
        let scalar = match val_opt {
            None => ScalarValue::try_from(dt)
                .map_err(|e| LtseqError::with_context("Failed to create null scalar", e))?,
            Some(val) => python_value_to_scalar(&val, dt).ok_or_else(|| LtseqError::Validation(
                format!("Cannot convert Python value for column '{}'", col_name)
            ))?,
        };
        let arr: ArrayRef = scalar.to_array_of_size(1)
            .map_err(|e| LtseqError::with_context("Failed to create array from scalar", e))?;
        arrays.push(arr);
        fields.push((**field).clone());
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| LtseqError::with_context("Failed to create record batch for insert", e).into())
}

fn python_value_to_scalar(val: &Bound<'_, PyAny>, dt: &DataType) -> Option<ScalarValue> {
    if val.is_none() {
        return ScalarValue::try_from(dt).ok();
    }
    match dt {
        DataType::Int64 => val.extract::<i64>().ok().map(|v| ScalarValue::Int64(Some(v))),
        DataType::Int32 => val.extract::<i32>().ok().map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int16 => val.extract::<i16>().ok().map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int8 => val.extract::<i8>().ok().map(|v| ScalarValue::Int8(Some(v))),
        DataType::Float64 => val.extract::<f64>().ok().map(|v| ScalarValue::Float64(Some(v))),
        DataType::Float32 => val.extract::<f32>().ok().map(|v| ScalarValue::Float32(Some(v))),
        DataType::Utf8 | DataType::LargeUtf8 => {
            val.extract::<String>().ok().map(|v| ScalarValue::Utf8(Some(v)))
        }
        DataType::Boolean => val.extract::<bool>().ok().map(|v| ScalarValue::Boolean(Some(v))),
        DataType::Date32 => val.extract::<i32>().ok().map(|v| ScalarValue::Date32(Some(v))),
        DataType::Date64 => val.extract::<i64>().ok().map(|v| ScalarValue::Date64(Some(v))),
        _ => None,
    }
}

fn compute_replace_at(
    col: &ArrayRef,
    pos: usize,
    new_val: &ArrayRef,
) -> Result<ArrayRef, datafusion::arrow::error::ArrowError> {
    use datafusion::arrow::compute::cast;

    let col_type = col.data_type();
    let cast_new_val = if new_val.data_type() != col_type {
        cast(new_val, col_type)?
    } else {
        new_val.clone()
    };

    let len = col.len();

    let mut new_arrays: Vec<ArrayRef> = Vec::new();

    if pos > 0 {
        new_arrays.push(col.slice(0, pos));
    }
    new_arrays.push(cast_new_val.slice(0, 1));
    if pos + 1 < len {
        new_arrays.push(col.slice(pos + 1, len - pos - 1));
    }

    let refs: Vec<&dyn Array> = new_arrays.iter().map(|a| a.as_ref()).collect();
    datafusion::arrow::compute::concat(&refs)
}
