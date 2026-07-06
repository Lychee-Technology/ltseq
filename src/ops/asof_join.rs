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
use crate::error::LtseqError;
use crate::ops::common::right_rename_map;
use crate::LTSeqTable;
use datafusion::arrow::array;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Encode the `by` grouping columns of every row into hashable owned keys via
/// Arrow's RowConverter. One converter is shared between left and right so the
/// encodings are comparable. Returns `None` when there are no `by` columns
/// (the ungrouped fast path). `by_indices` are column positions in `batches`.
fn extract_group_keys(
    converter: &RowConverter,
    batches: &[RecordBatch],
    by_indices: &[usize],
) -> PyResult<Vec<OwnedRow>> {
    let mut keys: Vec<OwnedRow> = Vec::new();
    for batch in batches {
        let cols: Vec<ArrayRef> =
            by_indices.iter().map(|&i| Arc::clone(batch.column(i))).collect();
        let rows = converter
            .convert_columns(&cols)
            .map_err(|e| LtseqError::Runtime(format!("Failed to encode by= keys: {}", e)))?;
        for i in 0..batch.num_rows() {
            keys.push(rows.row(i).owned());
        }
    }
    Ok(keys)
}

/// Resolve `by` column names to indices in `schema`, validating existence.
fn by_col_indices(
    schema: &ArrowSchema,
    by_cols: &[String],
    side: &str,
) -> PyResult<Vec<usize>> {
    by_cols
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    PyErr::from(LtseqError::Validation(format!(
                        "by= column '{}' not found in {} table",
                        name, side
                    )))
                })
        })
        .collect()
}

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
    suffix: &str,
    left_by_cols: Vec<String>,
    right_by_cols: Vec<String>,
) -> PyResult<LTSeqTable> {
    // 1. Validate both tables have data
    let (df_left, stored_schema_left) = table.require_df_and_schema()?;
    let (df_right, stored_schema_right) = other.require_df_and_schema()?;

    // 2. Validate direction
    match direction {
        "backward" | "forward" | "nearest" => {}
        _ => {
            return Err(LtseqError::Validation(format!(
                "Invalid direction '{}'. Must be 'backward', 'forward', or 'nearest'",
                direction
            ))
            .into())
        }
    }

    // 3. Collect both DataFrames to batches
    let (left_batches, right_batches) = RUNTIME
        .block_on(async {
            let left_future = (**df_left).clone().collect();
            let right_future = (**df_right).clone().collect();

            let left_result = left_future
                .await
                .map_err(LtseqError::collect)?;
            let right_result = right_future
                .await
                .map_err(LtseqError::collect)?;

            Ok::<_, LtseqError>((left_result, right_result))
        })
        .map_err(PyErr::from)?;

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
            LtseqError::Validation(format!(
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
            LtseqError::Validation(format!(
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
    let mut left_times: Vec<i64> = Vec::new();

    for batch in left_batches.iter() {
        let time_col = batch.column(left_time_idx);
        let times = extract_time_values(time_col)?;
        left_times.extend(times);
    }

    let mut right_times: Vec<i64> = Vec::new();

    for batch in right_batches.iter() {
        let time_col = batch.column(right_time_idx);
        let times = extract_time_values(time_col)?;
        right_times.extend(times);
    }

    // 7. For each left row, find the matching right row via binary search.
    //
    // Without `by=`, the search runs over the whole (time-sorted) right table.
    // With `by=`, each left row only matches right rows sharing its group key:
    // we bucket right row indices by group (times stay ascending because the
    // table is time-sorted) and search within the left row's own bucket.
    let mut matched_right_indices: Vec<Option<usize>> = Vec::with_capacity(left_times.len());

    let find = |target: i64, times: &[i64]| match direction {
        "backward" => find_asof_backward(target, times),
        "forward" => find_asof_forward(target, times),
        "nearest" => find_asof_nearest(target, times),
        _ => unreachable!(),
    };

    if left_by_cols.is_empty() && right_by_cols.is_empty() {
        for &left_time in &left_times {
            matched_right_indices.push(find(left_time, &right_times));
        }
    } else {
        if left_by_cols.len() != right_by_cols.len() {
            return Err(LtseqError::Validation(
                "asof_join by= must name the same number of columns on both sides".into(),
            )
            .into());
        }
        let left_by_idx = by_col_indices(&left_schema, &left_by_cols, "left")?;
        let right_by_idx = by_col_indices(&right_schema, &right_by_cols, "right")?;

        // Build one shared RowConverter over the by-column types (must match).
        let sort_fields: Vec<SortField> = right_by_idx
            .iter()
            .map(|&i| SortField::new(right_schema.field(i).data_type().clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)
            .map_err(|e| LtseqError::Runtime(format!("Failed to build by= encoder: {}", e)))?;

        let left_keys = extract_group_keys(&converter, &left_batches, &left_by_idx)?;
        let right_keys = extract_group_keys(&converter, &right_batches, &right_by_idx)?;

        // Bucket right rows by group: key -> (ascending times, original indices).
        let mut groups: HashMap<OwnedRow, (Vec<i64>, Vec<usize>)> = HashMap::new();
        for (idx, key) in right_keys.iter().enumerate() {
            let entry = groups.entry(key.clone()).or_default();
            entry.0.push(right_times[idx]);
            entry.1.push(idx);
        }

        for (i, &left_time) in left_times.iter().enumerate() {
            match groups.get(&left_keys[i]) {
                Some((times, orig)) => {
                    matched_right_indices.push(find(left_time, times).map(|g| orig[g]));
                }
                None => matched_right_indices.push(None),
            }
        }
    }

    // 8. Build result schema: left columns + suffix-renamed right columns.
    // Only right columns that collide with the left get the suffix (Polars
    // semantics). Unlike an equi-join, we KEEP the right time column: an asof
    // match is approximate, so the matched timestamp is real information.
    let rename_map = right_rename_map(&left_schema, &right_schema, suffix)?;
    let mut result_fields: Vec<Field> = Vec::new();

    for field in left_schema.fields() {
        result_fields.push((**field).clone());
    }

    for (field, (_, new_name)) in right_schema.fields().iter().zip(rename_map.iter()) {
        result_fields.push(Field::new(
            new_name,
            field.data_type().clone(),
            true, // Always nullable for asof join
        ));
    }

    let result_schema = Arc::new(ArrowSchema::new(result_fields));

    // 9. Build result arrays
    //
    // Optimization: consolidate all batches into single RecordBatches once,
    // then extract columns directly. This avoids per-column concat (which
    // would redundantly concatenate the same batches N+M times).
    let num_result_rows = left_times.len();
    let mut result_columns: Vec<Arc<dyn Array>> = Vec::new();

    // Consolidate left batches into a single RecordBatch
    let consolidated_left = if left_batches.len() <= 1 {
        left_batches.into_iter().next()
    } else {
        Some(
            datafusion::arrow::compute::concat_batches(&left_schema, &left_batches).map_err(
                |e| {
                    LtseqError::Runtime(format!(
                        "Failed to concatenate left batches: {}",
                        e
                    ))
                },
            )?,
        )
    };

    // Consolidate right batches into a single RecordBatch
    let consolidated_right = if right_batches.len() <= 1 {
        right_batches.into_iter().next()
    } else {
        Some(
            datafusion::arrow::compute::concat_batches(&right_schema, &right_batches).map_err(
                |e| {
                    LtseqError::Runtime(format!(
                        "Failed to concatenate right batches: {}",
                        e
                    ))
                },
            )?,
        )
    };

    // Copy left columns directly (all rows kept in order after concat)
    if let Some(ref left_batch) = consolidated_left {
        for col_idx in 0..left_schema.fields().len() {
            result_columns.push(Arc::clone(left_batch.column(col_idx)));
        }
    }

    // Build right columns with matches or NULLs via take()
    if let Some(ref right_batch) = consolidated_right {
        let indices: Vec<Option<u32>> = matched_right_indices
            .iter()
            .map(|opt| opt.map(|idx| idx as u32))
            .collect();
        let indices_array = array::UInt32Array::from(indices);

        for col_idx in 0..right_schema.fields().len() {
            let col = right_batch.column(col_idx);
            let result =
                datafusion::arrow::compute::take(col, &indices_array, None).map_err(|e| {
                    LtseqError::Runtime(format!(
                        "Failed to take right values: {}",
                        e
                    ))
                })?;
            result_columns.push(result);
        }
    } else {
        // No right data — add null columns
        for field in right_schema.fields() {
            let null_arr =
                datafusion::arrow::array::new_null_array(field.data_type(), num_result_rows);
            result_columns.push(null_arr);
        }
    }

    // 10. Create result RecordBatch
    let result_batch = datafusion::arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&result_schema),
        result_columns,
    )
    .map_err(|e| {
        LtseqError::Runtime(format!(
            "Failed to create result batch: {}",
            e
        ))
    })?;

    // 11. Create result LTSeqTable
    LTSeqTable::from_batches(
        Arc::clone(&table.session),
        vec![result_batch],
        Vec::new(),
        None, // row set / columns diverge from the raw file: drop fast-path token
    )
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
                    LtseqError::Validation("Failed to cast to Int64Array".into())
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
                    LtseqError::Validation("Failed to cast to Int32Array".into())
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
                    LtseqError::Validation(
                        "Failed to cast to Float64Array".into(),
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
                return Err(LtseqError::Validation(
                    "Unsupported timestamp type for asof join time column".into(),
                )
                .into());
            }
        }
        DataType::Date32 => {
            let arr = col
                .as_any()
                .downcast_ref::<array::Date32Array>()
                .ok_or_else(|| {
                    LtseqError::Validation("Failed to cast to Date32Array".into())
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
                    LtseqError::Validation("Failed to cast to Date64Array".into())
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
            return Err(LtseqError::Validation(format!(
                "Unsupported data type '{}' for asof join time column",
                other
            ))
            .into());
        }
    }

    Ok(values)
}


