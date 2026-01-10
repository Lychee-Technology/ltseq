//! As-of Join operations for LTSeqTable
//!
//! Time-series join matching each left row with nearest right row based on time/key column.
//!
//! # Algorithm
//!
//! Uses binary search for O(N log M) complexity where N = left rows, M = right rows.
//! Both tables must be sorted by their respective time columns.
//!
//! # Direction Semantics
//!
//! * `backward`: Find largest right.time where right.time <= left.time
//! * `forward`: Find smallest right.time where right.time >= left.time  
//! * `nearest`: Find closest right.time (backward bias on ties)

use crate::engine::RUNTIME;
use crate::LTSeqTable;
use datafusion::arrow::array;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::datasource::MemTable;
use pyo3::prelude::*;
use std::sync::Arc;

/// As-of Join: Time-series join matching each left row with nearest right row
///
/// For each row in the left table, finds the "nearest" matching row in the right table
/// based on a time/key column. This is commonly used in financial applications.
pub fn asof_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_time_col: &str,
    right_time_col: &str,
    direction: &str,
    alias: &str,
) -> PyResult<LTSeqTable> {
    // 1. Validate both tables have data
    let df_left = table.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Left table has no data. Call read_csv() first.",
        )
    })?;

    let df_right = other.dataframe.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Right table has no data. Call read_csv() first.",
        )
    })?;

    let stored_schema_left = table.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Left table schema not available.")
    })?;

    let stored_schema_right = other.schema.as_ref().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Right table schema not available.")
    })?;

    // 2. Validate direction
    match direction {
        "backward" | "forward" | "nearest" => {}
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid direction '{}'. Must be 'backward', 'forward', or 'nearest'",
                direction
            )))
        }
    }

    // 3. Collect both DataFrames to batches
    let (left_batches, right_batches) = RUNTIME
        .block_on(async {
            let left_future = (**df_left).clone().collect();
            let right_future = (**df_right).clone().collect();

            let left_result = left_future
                .await
                .map_err(|e| format!("Failed to collect left table: {}", e))?;
            let right_result = right_future
                .await
                .map_err(|e| format!("Failed to collect right table: {}", e))?;

            Ok::<_, String>((left_result, right_result))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // 4. Get actual schemas from batches
    let left_schema = left_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema_left).clone()));

    let right_schema = right_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new((**stored_schema_right).clone()));

    // 5. Find time column indices
    let left_time_idx = left_schema
        .fields()
        .iter()
        .position(|f| f.name() == left_time_col)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Time column '{}' not found in left table. Available: {:?}",
                left_time_col,
                left_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ))
        })?;

    let right_time_idx = right_schema
        .fields()
        .iter()
        .position(|f| f.name() == right_time_col)
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Time column '{}' not found in right table. Available: {:?}",
                right_time_col,
                right_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ))
        })?;

    // 6. Flatten batches and extract time values
    let mut left_rows: Vec<(usize, usize)> = Vec::new();
    let mut left_times: Vec<i64> = Vec::new();

    for (batch_idx, batch) in left_batches.iter().enumerate() {
        let time_col = batch.column(left_time_idx);
        let times = extract_time_values(time_col)?;
        for (row_idx, time) in times.into_iter().enumerate() {
            left_rows.push((batch_idx, row_idx));
            left_times.push(time);
        }
    }

    let mut right_times: Vec<i64> = Vec::new();
    let mut right_row_indices: Vec<(usize, usize)> = Vec::new();

    for (batch_idx, batch) in right_batches.iter().enumerate() {
        let time_col = batch.column(right_time_idx);
        let times = extract_time_values(time_col)?;
        for (row_idx, time) in times.into_iter().enumerate() {
            right_row_indices.push((batch_idx, row_idx));
            right_times.push(time);
        }
    }

    // 7. For each left row, find matching right row using binary search
    let mut matched_right_indices: Vec<Option<usize>> = Vec::with_capacity(left_times.len());

    for &left_time in &left_times {
        let match_idx = match direction {
            "backward" => find_asof_backward(left_time, &right_times),
            "forward" => find_asof_forward(left_time, &right_times),
            "nearest" => find_asof_nearest(left_time, &right_times),
            _ => unreachable!(),
        };
        matched_right_indices.push(match_idx);
    }

    // 8. Build result schema: left columns + aliased right columns
    let mut result_fields: Vec<Field> = Vec::new();

    for field in left_schema.fields() {
        result_fields.push((**field).clone());
    }

    for field in right_schema.fields() {
        result_fields.push(Field::new(
            format!("{}_{}", alias, field.name()),
            field.data_type().clone(),
            true, // Always nullable for asof join
        ));
    }

    let result_schema = Arc::new(ArrowSchema::new(result_fields));

    // 9. Build result arrays
    let num_result_rows = left_times.len();
    let mut result_columns: Vec<Arc<dyn Array>> = Vec::new();

    // Copy left columns
    for col_idx in 0..left_schema.fields().len() {
        let result_array = copy_column_from_batches(&left_batches, col_idx, &left_rows)?;
        result_columns.push(result_array);
    }

    // Build right columns with matches or NULLs
    for col_idx in 0..right_schema.fields().len() {
        let result_array = build_matched_column(
            &right_batches,
            col_idx,
            &right_row_indices,
            &matched_right_indices,
            num_result_rows,
        )?;
        result_columns.push(result_array);
    }

    // 10. Create result RecordBatch
    let result_batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&result_schema),
        result_columns,
    )
    .map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to create result batch: {}",
            e
        ))
    })?;

    // 11. Create result LTSeqTable
    let result_mem = MemTable::try_new(Arc::clone(&result_schema), vec![vec![result_batch]])
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create result table: {}",
                e
            ))
        })?;

    let result_df = table
        .session
        .read_table(Arc::new(result_mem))
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to read result table: {}",
                e
            ))
        })?;

    Ok(LTSeqTable {
        session: Arc::clone(&table.session),
        dataframe: Some(Arc::new(result_df)),
        schema: Some(result_schema),
        sort_exprs: Vec::new(),
    })
}

/// Find the largest index where right_times[idx] <= target (backward match)
fn find_asof_backward(target: i64, right_times: &[i64]) -> Option<usize> {
    if right_times.is_empty() {
        return None;
    }

    let idx = right_times.partition_point(|&t| t <= target);

    if idx == 0 {
        None
    } else {
        Some(idx - 1)
    }
}

/// Find the smallest index where right_times[idx] >= target (forward match)
fn find_asof_forward(target: i64, right_times: &[i64]) -> Option<usize> {
    if right_times.is_empty() {
        return None;
    }

    let idx = right_times.partition_point(|&t| t < target);

    if idx >= right_times.len() {
        None
    } else {
        Some(idx)
    }
}

/// Find the nearest index (backward bias on ties)
fn find_asof_nearest(target: i64, right_times: &[i64]) -> Option<usize> {
    let backward = find_asof_backward(target, right_times);
    let forward = find_asof_forward(target, right_times);

    match (backward, forward) {
        (None, None) => None,
        (Some(b), None) => Some(b),
        (None, Some(f)) => Some(f),
        (Some(b), Some(f)) => {
            let diff_back = target - right_times[b];
            let diff_fwd = right_times[f] - target;
            if diff_back <= diff_fwd {
                Some(b)
            } else {
                Some(f)
            }
        }
    }
}

/// Extract time values from an Arrow array column as i64
fn extract_time_values(col: &Arc<dyn Array>) -> PyResult<Vec<i64>> {
    let len = col.len();
    let mut values = Vec::with_capacity(len);

    match col.data_type() {
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Int64Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to cast to Int64Array")
                })?;
            for i in 0..len {
                values.push(if arr.is_null(i) {
                    i64::MIN
                } else {
                    arr.value(i)
                });
            }
        }
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Int32Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to cast to Int32Array")
                })?;
            for i in 0..len {
                values.push(if arr.is_null(i) {
                    i64::MIN
                } else {
                    arr.value(i) as i64
                });
            }
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Float64Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Failed to cast to Float64Array",
                    )
                })?;
            for i in 0..len {
                values.push(if arr.is_null(i) {
                    i64::MIN
                } else {
                    arr.value(i) as i64
                });
            }
        }
        DataType::Timestamp(_, _) => {
            // Try various timestamp types
            if let Some(arr) = col
                .as_any()
                .downcast_ref::<array::TimestampMicrosecondArray>()
            {
                for i in 0..len {
                    values.push(if arr.is_null(i) {
                        i64::MIN
                    } else {
                        arr.value(i)
                    });
                }
            } else if let Some(arr) = col
                .as_any()
                .downcast_ref::<array::TimestampNanosecondArray>()
            {
                for i in 0..len {
                    values.push(if arr.is_null(i) {
                        i64::MIN
                    } else {
                        arr.value(i)
                    });
                }
            } else if let Some(arr) = col
                .as_any()
                .downcast_ref::<array::TimestampMillisecondArray>()
            {
                for i in 0..len {
                    values.push(if arr.is_null(i) {
                        i64::MIN
                    } else {
                        arr.value(i)
                    });
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<array::TimestampSecondArray>() {
                for i in 0..len {
                    values.push(if arr.is_null(i) {
                        i64::MIN
                    } else {
                        arr.value(i)
                    });
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Unsupported timestamp type for asof join time column",
                ));
            }
        }
        DataType::Date32 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Date32Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to cast to Date32Array")
                })?;
            for i in 0..len {
                values.push(if arr.is_null(i) {
                    i64::MIN
                } else {
                    arr.value(i) as i64
                });
            }
        }
        DataType::Date64 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Date64Array>()
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to cast to Date64Array")
                })?;
            for i in 0..len {
                values.push(if arr.is_null(i) {
                    i64::MIN
                } else {
                    arr.value(i)
                });
            }
        }
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported data type '{}' for asof join time column",
                other
            )));
        }
    }

    Ok(values)
}

/// Copy a column from source batches to result, selecting rows by index
fn copy_column_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    col_idx: usize,
    row_indices: &[(usize, usize)],
) -> PyResult<Arc<dyn Array>> {
    if batches.is_empty() || row_indices.is_empty() {
        let dtype = if let Some(batch) = batches.first() {
            batch.column(col_idx).data_type().clone()
        } else {
            DataType::Null
        };
        return Ok(datafusion::arrow::array::new_empty_array(&dtype));
    }

    // Concatenate all batches for this column
    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(col_idx).as_ref()).collect();

    let concatenated = datafusion::arrow::compute::concat(&arrays).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to concatenate arrays: {}",
            e
        ))
    })?;

    // Calculate batch offsets for global indices
    let batch_offsets: Vec<usize> = batches
        .iter()
        .scan(0usize, |acc, b| {
            let prev = *acc;
            *acc += b.num_rows();
            Some(prev)
        })
        .collect();

    let global_indices: Vec<u32> = row_indices
        .iter()
        .map(|&(batch_idx, row_idx)| (batch_offsets[batch_idx] + row_idx) as u32)
        .collect();

    let indices = array::UInt32Array::from(global_indices);
    let result = datafusion::arrow::compute::take(&concatenated, &indices, None).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to take values: {}", e))
    })?;

    Ok(result)
}

/// Build a column for matched right rows, with NULLs for unmatched
fn build_matched_column(
    right_batches: &[datafusion::arrow::record_batch::RecordBatch],
    col_idx: usize,
    _right_row_indices: &[(usize, usize)],
    matched_indices: &[Option<usize>],
    num_rows: usize,
) -> PyResult<Arc<dyn Array>> {
    if right_batches.is_empty() {
        let dtype = DataType::Null;
        let null_arr = datafusion::arrow::array::new_null_array(&dtype, num_rows);
        return Ok(null_arr);
    }

    // Concatenate all right batches for this column
    let arrays: Vec<&dyn Array> = right_batches
        .iter()
        .map(|b| b.column(col_idx).as_ref())
        .collect();

    let concatenated = datafusion::arrow::compute::concat(&arrays).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to concatenate right arrays: {}",
            e
        ))
    })?;

    // Build nullable indices for take operation
    let indices: Vec<Option<u32>> = matched_indices
        .iter()
        .map(|opt| opt.map(|idx| idx as u32))
        .collect();

    let indices_array = array::UInt32Array::from(indices);

    let result =
        datafusion::arrow::compute::take(&concatenated, &indices_array, None).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to take right values: {}",
                e
            ))
        })?;

    Ok(result)
}
