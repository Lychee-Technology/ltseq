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
//!
//! A NULL or NaN time on either side never matches. Float times are searched
//! as floats, so their fractions and infinities count. An integer time next to
//! a float one is compared by exact value, so neither is rounded.

use crate::engine::RUNTIME;
use crate::error::LtseqError;
use crate::ops::common::{equal_floats_as_one, right_rename_map};
use crate::LTSeqTable;
use datafusion::arrow::array;
use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Float64Array, Int64Array, PrimitiveArray,
    RecordBatch,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, Int32Type, Int64Type, Schema as ArrowSchema,
    SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::row::{RowConverter, SortField};
use num_bigint::BigInt;
use num_traits::Float;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

/// Call `f` with the index of each row of `batch`, in order, and its `by`
/// columns encoded by Arrow's RowConverter, or `None` when one of them is
/// NULL. One converter is shared between left and right, so two keys are
/// equal exactly when their bytes are. `by_indices` are column positions in
/// `batch`. The rows are encoded `chunk_rows` at a time, so the encoder
/// holds at most that many keys however many rows `batch` has.
///
/// `by` keys follow `join`'s equality (contract §12.1, #370), which the
/// encoding alone does not: it gives NULL a key of its own, and a float the
/// key of its bits. So a row with a NULL key gets none, as NULL matches
/// nothing, NULL included; and floats are encoded through
/// `equal_floats_as_one`, so that `-0.0` meets `0.0` and NaN meets NaN.
fn for_each_group_key(
    converter: &RowConverter,
    batch: &RecordBatch,
    by_indices: &[usize],
    chunk_rows: usize,
    mut f: impl FnMut(usize, Option<&[u8]>),
) -> Result<(), LtseqError> {
    for start in (0..batch.num_rows()).step_by(chunk_rows) {
        let len = chunk_rows.min(batch.num_rows() - start);
        let cols: Vec<ArrayRef> = by_indices
            .iter()
            .map(|&i| equal_floats_as_one(batch.column(i).slice(start, len)))
            .collect();
        // `logical_nulls`, not `nulls`: a `null`-typed column has no
        // validity buffer, and a dictionary's NULLs can sit in its values.
        let nulls = cols.iter().fold(None, |acc: Option<NullBuffer>, col| {
            NullBuffer::union(acc.as_ref(), col.logical_nulls().as_ref())
        });
        let keys = converter
            .convert_columns(&cols)
            .map_err(|e| LtseqError::Runtime(format!("Failed to encode by= keys: {}", e)))?;
        for (i, key) in keys.iter().enumerate() {
            let valid = nulls.as_ref().is_none_or(|n| n.is_valid(i));
            f(start + i, valid.then(|| key.data()));
        }
    }
    Ok(())
}

/// Resolve `by` column names to indices in `schema`, validating existence.
fn by_col_indices(
    schema: &ArrowSchema,
    by_cols: &[String],
    side: &str,
) -> Result<Vec<usize>, LtseqError> {
    by_cols
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    LtseqError::Validation(format!(
                        "by= column '{}' not found in {} table",
                        name, side
                    ))
                })
        })
        .collect()
}

/// As-of Join: Time-series join matching each left row with nearest right row
///
/// For each row in the left table, finds the "nearest" matching row in the right table
/// based on a time/key column. This is commonly used in financial applications.
///
/// Runs with the GIL released (`lib.rs::asof_join` wraps it in `gil::detached`):
/// both collects and the binary-search matching take only plain Rust values.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the flat parameter list of the PyO3 `asof_join` binding in lib.rs; \
              packing into a struct is a behavior-neutral refactor out of scope for #150"
)]
pub fn asof_join_impl(
    table: &LTSeqTable,
    other: &LTSeqTable,
    left_time_col: &str,
    right_time_col: &str,
    direction: &str,
    suffix: &str,
    left_by_cols: Vec<String>,
    right_by_cols: Vec<String>,
) -> Result<LTSeqTable, LtseqError> {
    // 1. Validate both tables have data
    let (_, stored_schema_left) = table.require_df_and_schema()?;
    let (_, stored_schema_right) = other.require_df_and_schema()?;

    // 2. Validate direction
    match direction {
        "backward" | "forward" | "nearest" => {}
        _ => {
            return Err(LtseqError::Validation(format!(
                "Invalid direction '{}'. Must be 'backward', 'forward', or 'nearest'",
                direction
            )))
        }
    }

    // 3. Collect both DataFrames to batches, in table order: the search
    // relies on each side's sort by its time column.
    let (left_ordered, right_ordered) = (table.ordered_df()?, other.ordered_df()?);
    let (left_batches, right_batches) = RUNTIME
        .block_on(async {
            let left_future = left_ordered.collect();
            let right_future = right_ordered.collect();

            let left_result = left_future
                .await
                .map_err(LtseqError::collect)?;
            let right_result = right_future
                .await
                .map_err(LtseqError::collect)?;

            Ok::<_, LtseqError>((left_result, right_result))
        })?;

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

    // 6. Consolidate each side into one batch, and read its time column. A
    // 64-bit time column is read in place, not copied.
    let left_batch = consolidate(&left_schema, left_batches, "left")?;
    let right_batch = consolidate(&right_schema, right_batches, "right")?;
    let left_times = time_values(left_batch.column(left_time_idx))?;
    let right_times = time_values(right_batch.column(right_time_idx))?;

    // 7. For each left row, find the matching right row via binary search.
    let by = if left_by_cols.is_empty() && right_by_cols.is_empty() {
        None
    } else {
        if left_by_cols.len() != right_by_cols.len() {
            return Err(LtseqError::Validation(
                "asof_join by= must name the same number of columns on both sides".into(),
            ));
        }
        let left_cols = by_col_indices(&left_schema, &left_by_cols, "left")?;
        let right_cols = by_col_indices(&right_schema, &right_by_cols, "right")?;

        // Build one shared RowConverter over the by-column types (must match).
        let sort_fields: Vec<SortField> = right_cols
            .iter()
            .map(|&i| SortField::new(right_schema.field(i).data_type().clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)
            .map_err(|e| LtseqError::Runtime(format!("Failed to build by= encoder: {}", e)))?;

        Some(ByKeys {
            converter,
            left: &left_batch,
            left_cols,
            right: &right_batch,
            right_cols,
            // Keys are encoded a batch of the session's size at a time, and
            // only a group's first key is kept, so neither side holds a key
            // per row.
            chunk_rows: table.session.copied_config().batch_size(),
        })
    };
    let by = by.as_ref();
    let matched_right_indices = match (&left_times, &right_times) {
        (Times::Int(left), Times::Int(right)) => matched_rows(left, right, direction, by)?,
        (Times::Int(left), Times::Float(right)) => matched_rows(left, right, direction, by)?,
        (Times::Float(left), Times::Int(right)) => matched_rows(left, right, direction, by)?,
        (Times::Float(left), Times::Float(right)) => matched_rows(left, right, direction, by)?,
    };

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
    let num_result_rows = left_batch.num_rows();
    let mut result_columns: Vec<Arc<dyn Array>> = Vec::new();

    // Copy left columns directly (all rows kept in order after concat)
    for col_idx in 0..left_schema.fields().len() {
        result_columns.push(Arc::clone(left_batch.column(col_idx)));
    }

    // Build right columns with matches or NULLs via take()
    if right_batch.num_rows() > 0 {
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
        result_schema,
        Vec::new(),
    )
}

/// The `by=` columns of both sides, and what encodes them.
struct ByKeys<'a> {
    converter: RowConverter,
    left: &'a RecordBatch,
    left_cols: Vec<usize>,
    right: &'a RecordBatch,
    right_cols: Vec<usize>,
    chunk_rows: usize,
}

/// A bucket: the non-NULL times of some right rows, and their row indices.
type Bucket<N> = (Vec<N>, Vec<usize>);

/// The right row each left row matches, by left row.
///
/// A NULL time never matches (#263), and neither does a NaN one, which
/// `time_values` reads as NULL (#369): a left row with one has no match, and
/// the search runs over right times that leave them out, so they stay
/// ascending wherever the sort put them. With `by=`, each left row only
/// matches right rows sharing its group key: we bucket right rows by group
/// and search within the left row's own bucket.
fn matched_rows<L, R>(
    left_times: &PrimitiveArray<L>,
    right_times: &PrimitiveArray<R>,
    direction: &str,
    by: Option<&ByKeys>,
) -> Result<Vec<Option<usize>>, LtseqError>
where
    L: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    L::Native: AsofTime<R::Native>,
{
    let mut matched: Vec<Option<usize>> = Vec::with_capacity(left_times.len());

    let find = |target: L::Native, times: &[R::Native]| match direction {
        "backward" => find_asof_backward(target, times),
        "forward" => find_asof_forward(target, times),
        "nearest" => find_asof_nearest(target, times),
        _ => unreachable!(),
    };
    let search =
        |left_time: Option<L::Native>, times: &[R::Native]| left_time.and_then(|t| find(t, times));
    let add = |(times, rows): &mut Bucket<R::Native>, row: usize| {
        if right_times.is_valid(row) {
            times.push(right_times.value(row));
            rows.push(row);
        }
    };

    let Some(by) = by else {
        // A sort by time puts the column's NULLs first or last, and its NaNs
        // at the ends too, so the right rows left to search are consecutive,
        // and their times are searched in place. Only a right table declared
        // sorted without being so, as `is_sorted=True` or `assume_sorted`
        // allow, can hold a NULL between them; a bucket that leaves the NULLs
        // out handles it.
        if let Some(run) = non_null_run(right_times) {
            let times = &right_times.values()[run.clone()];
            matched.extend(
                left_times
                    .iter()
                    .map(|t| search(t, times).map(|i| run.start + i)),
            );
        } else {
            let mut all = Bucket::default();
            for row in 0..right_times.len() {
                add(&mut all, row);
            }
            matched.extend(
                left_times
                    .iter()
                    .map(|t| search(t, &all.0).map(|i| all.1[i])),
            );
        }
        return Ok(matched);
    };

    // Bucket right rows by group. A right row with a NULL key is in no
    // bucket, and a left row with one finds none (#370).
    let mut groups: HashMap<Box<[u8]>, Bucket<R::Native>> = HashMap::new();
    for_each_group_key(
        &by.converter,
        by.right,
        &by.right_cols,
        by.chunk_rows,
        |row, key| {
            let Some(key) = key else { return };
            match groups.get_mut(key) {
                Some(bucket) => add(bucket, row),
                None => {
                    let mut bucket = Bucket::default();
                    add(&mut bucket, row);
                    groups.insert(key.into(), bucket);
                }
            }
        },
    )?;

    for_each_group_key(
        &by.converter,
        by.left,
        &by.left_cols,
        by.chunk_rows,
        |row, key| {
            let left_time = left_times.is_valid(row).then(|| left_times.value(row));
            matched.push(
                key.and_then(|key| groups.get(key))
                    .and_then(|(times, rows)| search(left_time, times).map(|i| rows[i])),
            );
        },
    )?;
    Ok(matched)
}

/// A left time, searched among right times of type `R` by exact value: an
/// integer and a float are compared as they are, and neither is converted to
/// the other's type, where an integer above `2**53` would round and land on
/// the wrong side of the left time. No value here is a NaN: `time_values`
/// reads one as NULL.
trait AsofTime<R>: Copy {
    /// How this time orders against the right time `right`.
    fn cmp_right(self, right: R) -> Ordering;

    /// Whether `back`, at or before this time, is at most as far from it as
    /// `fwd`, at or after it. The two distances are compared exactly.
    fn back_is_nearest(self, back: R, fwd: R) -> bool;
}

impl AsofTime<i64> for i64 {
    fn cmp_right(self, right: i64) -> Ordering {
        self.cmp(&right)
    }

    fn back_is_nearest(self, back: i64, fwd: i64) -> bool {
        // As u64, a distance between any two i64 times fits.
        self.abs_diff(back) <= fwd.abs_diff(self)
    }
}

impl AsofTime<f64> for f64 {
    fn cmp_right(self, right: f64) -> Ordering {
        self.partial_cmp(&right).unwrap_or(Ordering::Equal)
    }

    fn back_is_nearest(self, back: f64, fwd: f64) -> bool {
        // An exact match is at distance 0, also at an infinity, where the
        // subtraction below has no answer.
        if back == self {
            return true;
        }
        if fwd == self {
            return false;
        }
        // This time lies strictly between the other two, so it is finite, and
        // an infinity is farther from it than any finite time: a distance
        // that overflows below is not an infinite one.
        if back.is_infinite() || fwd.is_infinite() {
            return fwd.is_infinite();
        }
        let (to_back, back_error) = difference(self, back);
        let (to_fwd, fwd_error) = difference(fwd, self);
        // Rounding keeps order, so rounded distances that differ are in the
        // order of the exact ones, and one that overflowed is the greater;
        // rounded ones that agree are told apart by their errors. Finite times
        // are at most twice the largest float apart, so at most one distance
        // overflows and the errors compared are those of finite distances.
        if to_back != to_fwd {
            return to_back < to_fwd;
        }
        back_error <= fwd_error
    }
}

impl AsofTime<f64> for i64 {
    fn cmp_right(self, right: f64) -> Ordering {
        cmp_int_float(i128::from(self), right)
    }

    fn back_is_nearest(self, back: f64, fwd: f64) -> bool {
        // A time a float holds exactly compares as floats, without big numbers.
        let target = self as f64;
        if self.cmp_right(target).is_eq() {
            return target.back_is_nearest(back, fwd);
        }
        if back.is_infinite() || fwd.is_infinite() {
            return fwd.is_infinite();
        }
        // `self - back <= fwd - self`, with no distance to round.
        BigInt::from(self) << (SCALE_BITS + 1) <= scaled_float(back) + scaled_float(fwd)
    }
}

impl AsofTime<i64> for f64 {
    fn cmp_right(self, right: i64) -> Ordering {
        cmp_int_float(i128::from(right), self).reverse()
    }

    fn back_is_nearest(self, back: i64, fwd: i64) -> bool {
        // `self - back <= fwd - self`, with no distance to round: this time
        // lies between two i64, so doubling it neither rounds nor overflows.
        cmp_int_float(i128::from(back) + i128::from(fwd), 2.0 * self).is_ge()
    }
}

/// How `int`, at most a sum of two i64, orders against `float`, by exact value.
fn cmp_int_float(int: i128, float: f64) -> Ordering {
    // A whole float converts exactly. One past i128's range saturates, which
    // is still past `int`, so the fraction is only reached for a finite float.
    let whole = float.trunc();
    int.cmp(&(whole as i128))
        .then_with(|| 0.0.cmp_right(float - whole))
}

/// `2**SCALE_BITS` times any finite float is an integer.
const SCALE_BITS: i16 = 1075;

/// A finite float times `2**SCALE_BITS`.
fn scaled_float(value: f64) -> BigInt {
    let (mantissa, exponent, sign) = Float::integer_decode(value);
    (BigInt::from(sign) * BigInt::from(mantissa)) << (exponent + SCALE_BITS) as usize
}

/// `a - b` rounded, and what the rounding lost: when the result is finite,
/// the two add up to the exact difference. This is Fast2Sum, which needs the
/// addend of greater magnitude first, and whose intermediate results do not
/// overflow unless the sum itself does.
fn difference(a: f64, b: f64) -> (f64, f64) {
    let b = -b;
    let (larger, smaller) = if a.abs() >= b.abs() { (a, b) } else { (b, a) };
    let sum = larger + smaller;
    (sum, smaller - (sum - larger))
}

/// Find the largest index where right_times[idx] <= target (backward match)
fn find_asof_backward<L: AsofTime<R>, R: Copy>(target: L, right_times: &[R]) -> Option<usize> {
    if right_times.is_empty() {
        return None;
    }

    let idx = right_times.partition_point(|&t| target.cmp_right(t).is_ge());

    if idx == 0 {
        None
    } else {
        Some(idx - 1)
    }
}

/// Find the smallest index where right_times[idx] >= target (forward match)
fn find_asof_forward<L: AsofTime<R>, R: Copy>(target: L, right_times: &[R]) -> Option<usize> {
    if right_times.is_empty() {
        return None;
    }

    let idx = right_times.partition_point(|&t| target.cmp_right(t).is_gt());

    if idx >= right_times.len() {
        None
    } else {
        Some(idx)
    }
}

/// Find the nearest index (backward bias on ties)
fn find_asof_nearest<L: AsofTime<R>, R: Copy>(target: L, right_times: &[R]) -> Option<usize> {
    let backward = find_asof_backward(target, right_times);
    let forward = find_asof_forward(target, right_times);

    match (backward, forward) {
        (None, None) => None,
        (Some(b), None) => Some(b),
        (None, Some(f)) => Some(f),
        (Some(b), Some(f)) => {
            if target.back_is_nearest(right_times[b], right_times[f]) {
                Some(b)
            } else {
                Some(f)
            }
        }
    }
}

/// `batches` as one batch: empty when there are none, and not copied when
/// there is one.
fn consolidate(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    side: &str,
) -> Result<RecordBatch, LtseqError> {
    concat_batches(schema, &batches)
        .map_err(|e| LtseqError::Runtime(format!("Failed to concatenate {} batches: {}", side, e)))
}

/// A time column read for the search, NULL where the time is NULL or NaN.
enum Times {
    Int(Int64Array),
    Float(Float64Array),
}

/// A time column's values. A 64-bit column is read in place, floats included;
/// a 32-bit one is converted. A column without rows has no values to read, so
/// its type is not checked: a filter that keeps no row of an all-NULL column,
/// typed `null`, still joins.
fn time_values(col: &ArrayRef) -> Result<Times, LtseqError> {
    if col.is_empty() {
        return Ok(Times::Int(Int64Array::from(Vec::<i64>::new())));
    }
    let values = match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().clone(),
        DataType::Int32 => col.as_primitive::<Int32Type>().unary(i64::from),
        DataType::Float64 => return Ok(Times::Float(nan_as_null(col.as_primitive()))),
        DataType::Timestamp(TimeUnit::Second, _) => {
            col.as_primitive::<TimestampSecondType>().reinterpret_cast()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => col
            .as_primitive::<TimestampMillisecondType>()
            .reinterpret_cast(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col
            .as_primitive::<TimestampMicrosecondType>()
            .reinterpret_cast(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_primitive::<TimestampNanosecondType>()
            .reinterpret_cast(),
        DataType::Date32 => col.as_primitive::<Date32Type>().unary(i64::from),
        DataType::Date64 => col.as_primitive::<Date64Type>().reinterpret_cast(),
        other => {
            return Err(LtseqError::Validation(format!(
                "Unsupported data type '{}' for asof join time column",
                other
            )));
        }
    };

    Ok(Times::Int(values))
}

/// `times` with each NaN, whatever its sign and payload, read as NULL. Only
/// the validity changes; the values are not copied.
fn nan_as_null(times: &Float64Array) -> Float64Array {
    let values = times.values();
    let not_nan = NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |i| {
        !values[i].is_nan()
    }));
    let nulls = NullBuffer::union(times.nulls(), Some(&not_nan));
    Float64Array::new(values.clone(), nulls)
}

/// The rows of `times` that are not NULL, when they are consecutive: there
/// is no NULL between them.
fn non_null_run(times: &dyn Array) -> Option<Range<usize>> {
    let len = times.len();
    let start = (0..len).take_while(|&i| times.is_null(i)).count();
    let end = len - (start..len).rev().take_while(|&i| times.is_null(i)).count();
    (len - (end - start) == times.null_count()).then_some(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        DictionaryArray, Float32Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };

    #[test]
    fn a_64_bit_time_column_is_read_in_place() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let stamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1),
            None,
            Some(3),
        ]));
        for col in [ints, stamps] {
            let Ok(Times::Int(times)) = time_values(&col) else {
                panic!("an integer time column");
            };
            assert_eq!(
                times.values().inner().as_ptr(),
                col.to_data().buffers()[0].as_ptr()
            );
            assert_eq!(times, Int64Array::from(vec![Some(1), None, Some(3)]));
        }
    }

    #[test]
    fn a_float_time_column_is_read_in_place_with_its_nans_as_nulls() {
        let col: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(-f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(1.5),
            None,
            Some(f64::INFINITY),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
        ]));
        let Ok(Times::Float(times)) = time_values(&col) else {
            panic!("a float time column");
        };
        assert_eq!(
            times.values().inner().as_ptr(),
            col.to_data().buffers()[0].as_ptr()
        );
        let valid: Vec<Option<f64>> = times.iter().collect();
        assert_eq!(
            valid,
            [
                None,
                None,
                Some(f64::NEG_INFINITY),
                Some(1.5),
                None,
                Some(f64::INFINITY),
                None
            ]
        );
        assert_eq!(non_null_run(&times), None);
    }

    #[test]
    fn float_times_are_searched_with_their_fractions() {
        let times = [4.0, 5.5];
        assert_eq!(find_asof_backward(5.2, &times), Some(0));
        assert_eq!(find_asof_forward(5.2, &times), Some(1));
        assert_eq!(find_asof_nearest(5.2, &times), Some(1));
        assert_eq!(find_asof_nearest(4.7, &times), Some(0));
        assert_eq!(find_asof_nearest(4.75, &times), Some(0), "a tie goes back");
    }

    #[test]
    fn nearest_handles_infinite_times() {
        let (inf, big) = (f64::INFINITY, f64::MAX);
        // An infinity is farther than every finite time, from a finite time.
        assert_eq!(find_asof_nearest(0.0, &[-inf, 1.0]), Some(1));
        assert_eq!(find_asof_nearest(0.0, &[-1.0, inf]), Some(0));
        assert_eq!(find_asof_nearest(0.0, &[-inf, inf]), Some(0), "a tie");
        // An infinity is at distance 0 from itself, and the last of equal
        // times is the backward one.
        assert_eq!(find_asof_nearest(inf, &[1.0, inf, inf]), Some(2));
        assert_eq!(find_asof_nearest(-inf, &[-inf, -inf, 1.0]), Some(1));
        // No right time is at or past the left one.
        assert_eq!(find_asof_nearest(inf, &[1.0]), Some(0));
        assert_eq!(find_asof_nearest(-inf, &[1.0]), Some(0));
        // A distance that overflows is still the greater one.
        assert_eq!(find_asof_nearest(big / 2.0, &[-big, big]), Some(1));
        assert_eq!(find_asof_nearest(-big / 2.0, &[-big, big]), Some(0));
        // A distance that overflows is finite, so an infinity is farther.
        assert_eq!(find_asof_nearest(-big / 2.0, &[-inf, big]), Some(1));
        assert_eq!(find_asof_nearest(big / 2.0, &[-big, inf]), Some(0));
    }

    #[test]
    fn an_integer_and_a_float_compare_by_exact_value() {
        let edge = 2i64.pow(53);
        let (edge_f, two_63) = (edge as f64, 2f64.powi(63));
        for (int, float, expected) in [
            (edge + 1, edge_f, Ordering::Greater),
            (edge - 1, edge_f, Ordering::Less),
            (edge, edge_f, Ordering::Equal),
            (edge + 1, edge_f + 2.0, Ordering::Less),
            (5, 5.25, Ordering::Less),
            (-5, -5.25, Ordering::Greater),
            (0, -0.0, Ordering::Equal),
            (0, f64::from_bits(1), Ordering::Less),
            (i64::MAX, two_63, Ordering::Less),
            (i64::MIN, -two_63, Ordering::Equal),
            (i64::MIN, -two_63 * (1.0 + f64::EPSILON), Ordering::Greater),
            (i64::MAX, f64::MAX, Ordering::Less),
            (i64::MIN, f64::MIN, Ordering::Greater),
            (i64::MAX, f64::INFINITY, Ordering::Less),
            (i64::MIN, f64::NEG_INFINITY, Ordering::Greater),
        ] {
            assert_eq!(int.cmp_right(float), expected, "{int} and {float}");
            assert_eq!(
                float.cmp_right(int),
                expected.reverse(),
                "{float} and {int}"
            );
        }
    }

    #[test]
    fn a_mixed_pair_keeps_each_time_on_its_side() {
        let edge = 2i64.pow(53);
        let edge_f = edge as f64;
        // As floats, edge + 1 rounds to edge, and edge + 3 to edge + 4.
        assert_eq!(find_asof_backward(edge_f, &[edge + 1]), None);
        assert_eq!(find_asof_forward(edge_f, &[edge + 1]), Some(0));
        assert_eq!(find_asof_forward(edge_f + 4.0, &[edge + 3]), None);
        assert_eq!(find_asof_backward(edge_f + 4.0, &[edge + 3]), Some(0));
        assert_eq!(find_asof_backward(edge + 1, &[edge_f + 2.0]), None);
        assert_eq!(find_asof_forward(edge + 1, &[edge_f]), None);
        assert_eq!(find_asof_forward(edge + 3, &[edge_f + 4.0]), Some(0));
    }

    #[test]
    fn nearest_compares_exact_distances_across_a_mixed_pair() {
        let edge = 2i64.pow(53);
        let edge_f = edge as f64;
        let (inf, big) = (f64::INFINITY, f64::MAX);
        // edge + 3 is a tie between edge and edge + 6; as a float, edge + 4 is
        // closer to edge + 6.
        assert_eq!(
            find_asof_nearest(edge + 3, &[edge_f, edge_f + 6.0]),
            Some(0)
        );
        // edge + 1 is closer to edge + 2 than to edge - 2; as a float, edge is
        // a tie between them.
        assert_eq!(
            find_asof_nearest(edge + 1, &[edge_f - 2.0, edge_f + 2.0]),
            Some(1)
        );
        assert_eq!(find_asof_nearest(5, &[4.5, 5.5]), Some(0), "a tie");
        assert_eq!(find_asof_nearest(i64::MAX, &[-big, big]), Some(1));
        assert_eq!(find_asof_nearest(i64::MIN + 1, &[-big, big]), Some(0));
        assert_eq!(find_asof_nearest(edge + 1, &[-inf, big]), Some(1));
        assert_eq!(find_asof_nearest(edge + 1, &[-big, inf]), Some(0));
        assert_eq!(find_asof_nearest(edge + 1, &[-inf, inf]), Some(0), "a tie");
        assert_eq!(find_asof_nearest(1, &[-inf, big]), Some(1));
        // A float between integers, and one that is the later of them.
        assert_eq!(find_asof_nearest(5.25, &[4i64, 6]), Some(1));
        assert_eq!(find_asof_nearest(5.0, &[4i64, 6]), Some(0), "a tie");
        assert_eq!(find_asof_nearest(edge_f, &[edge, edge + 1]), Some(0));
        assert_eq!(
            find_asof_nearest(edge_f + 2.0, &[edge + 1, edge + 2]),
            Some(1)
        );
        // edge + 8 is closer to edge + 11 than to edge + 3; as floats, those
        // are edge + 12 and edge + 4, a tie.
        assert_eq!(
            find_asof_nearest(edge_f + 8.0, &[edge + 3, edge + 11]),
            Some(1)
        );
        // The sum of the two neighbours does not fit an i64, nor the distance.
        assert_eq!(
            find_asof_nearest(2f64.powi(62), &[i64::MIN, i64::MAX]),
            Some(1)
        );
        assert_eq!(find_asof_nearest(0.0, &[i64::MIN, i64::MAX]), Some(1));
        assert_eq!(
            find_asof_nearest(-0.5, &[i64::MIN, i64::MAX]),
            Some(0),
            "a tie"
        );
    }

    #[test]
    fn scaled_floats_keep_every_bit() {
        let scale = BigInt::from(1) << SCALE_BITS as usize;
        assert_eq!(scaled_float(0.0), BigInt::from(0));
        assert_eq!(scaled_float(1.5), &scale * 3 / 2);
        assert_eq!(scaled_float(-1.5), -&scale * 3 / 2);
        // The smallest float is 2**-1074.
        assert_eq!(scaled_float(f64::from_bits(1)), BigInt::from(2));
        assert_eq!(scaled_float(f64::MIN_POSITIVE), BigInt::from(1) << 53);
    }

    #[test]
    fn nearest_tells_apart_distances_that_round_to_the_same_float() {
        // Both distances from +-0.5 round to 2**53, as 2**53 +- 0.5.
        let edge = 2f64.powi(53);
        assert_eq!(0.5 - -edge, edge - 0.5);
        assert_eq!(find_asof_nearest(0.5, &[-edge, edge]), Some(1));
        assert_eq!(find_asof_nearest(-0.5, &[-edge, edge]), Some(0));
        assert_eq!(find_asof_nearest(0.0, &[-edge, edge]), Some(0), "a tie");
    }

    #[test]
    fn the_difference_and_its_error_sum_to_the_exact_value() {
        // 2**53 + 0.5 and 2**53 - 0.5 both round to 2**53.
        let edge = 2f64.powi(53);
        assert_eq!(difference(0.5, -edge), (edge, 0.5));
        assert_eq!(difference(edge, 0.5), (edge, -0.5));
        assert_eq!(difference(3.0, 1.0), (2.0, 0.0));
        assert_eq!(difference(f64::MAX, -f64::MAX).0, f64::INFINITY);
    }

    #[test]
    fn the_non_null_run_is_found_unless_a_null_lies_inside_it() {
        let run = |times: &[Option<i64>]| non_null_run(&Int64Array::from(times.to_vec()));
        assert_eq!(run(&[Some(1), Some(2)]), Some(0..2));
        assert_eq!(run(&[None, None, Some(1), Some(2)]), Some(2..4));
        assert_eq!(run(&[Some(1), Some(2), None]), Some(0..2));
        assert_eq!(run(&[None, Some(1), None]), Some(1..2));
        assert_eq!(run(&[None, None]), Some(2..2));
        assert_eq!(run(&[]), Some(0..0));
        assert_eq!(run(&[Some(1), None, Some(2)]), None);
    }

    #[test]
    fn group_keys_are_visited_in_order_and_compare_equal_across_chunks() {
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b"]));
        let batch = RecordBatch::try_from_iter([("k", keys)]).expect("a batch");
        let converter =
            RowConverter::new(vec![SortField::new(DataType::Utf8)]).expect("an encoder");
        for chunk_rows in 1..=6 {
            let mut rows = Vec::new();
            let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
            for_each_group_key(&converter, &batch, &[0], chunk_rows, |row, key| {
                rows.push(row);
                groups
                    .entry(key.expect("a key that is not NULL").to_vec())
                    .or_default()
                    .push(row);
            })
            .expect("encoded keys");
            let mut groups: Vec<Vec<usize>> = groups.into_values().collect();
            groups.sort();
            assert_eq!(rows, [0, 1, 2, 3, 4], "chunk_rows={chunk_rows}");
            assert_eq!(
                groups,
                [vec![0, 2], vec![1, 4], vec![3]],
                "chunk_rows={chunk_rows}"
            );
        }
    }

    /// The rows of `cols` grouped as `for_each_group_key` keys them, and the
    /// rows it gives no key.
    fn group_rows(cols: Vec<ArrayRef>) -> (Vec<Vec<usize>>, Vec<usize>) {
        let fields = cols
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).expect("an encoder");
        let names = (0..cols.len()).map(|i| format!("k{i}"));
        let batch = RecordBatch::try_from_iter(names.zip(cols)).expect("a batch");
        let indices: Vec<usize> = (0..batch.num_columns()).collect();
        let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let mut keyless = Vec::new();
        for_each_group_key(&converter, &batch, &indices, 2, |row, key| match key {
            Some(key) => groups.entry(key.to_vec()).or_default().push(row),
            None => keyless.push(row),
        })
        .expect("encoded keys");
        let mut groups: Vec<Vec<usize>> = groups.into_values().collect();
        groups.sort();
        (groups, keyless)
    }

    #[test]
    fn a_row_with_a_null_in_any_by_column_has_no_key() {
        let s: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            Some("a"),
            None,
            Some("a"),
            Some("a"),
        ]));
        let k: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            None,
            Some(1),
            Some(1),
        ]));
        assert_eq!(group_rows(vec![s, k]), (vec![vec![3, 4]], vec![0, 1, 2]));
        let all_null: ArrayRef = Arc::new(datafusion::arrow::array::NullArray::new(3));
        assert_eq!(group_rows(vec![all_null]), (vec![], vec![0, 1, 2]));
    }

    #[test]
    fn float_keys_follow_join_equality() {
        let nans = [f64::NAN, -f64::NAN, f64::from_bits(0x7ff8_0000_0000_0001)];
        let keys = vec![
            Some(0.0),
            Some(-0.0),
            Some(nans[0]),
            Some(nans[1]),
            Some(nans[2]),
            Some(1.5),
            None,
        ];
        let f64s: ArrayRef = Arc::new(Float64Array::from(keys.clone()));
        let f32s: ArrayRef = Arc::new(Float32Array::from(
            keys.iter().map(|k| k.map(|v| v as f32)).collect::<Vec<_>>(),
        ));
        for col in [f64s, f32s] {
            assert_eq!(
                group_rows(vec![col]),
                (vec![vec![0, 1], vec![2, 3, 4], vec![5]], vec![6])
            );
        }
    }

    #[test]
    fn dictionary_encoded_float_keys_follow_join_equality() {
        // Rows 0..5 point at -0.0, 0.0, three NaNs of different bits and 1.5;
        // row 6 has a NULL key and row 7 points at a NULL value.
        let values = vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(-f64::NAN),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(1.5),
            None,
        ];
        let keys = Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            None,
            Some(6),
        ]);
        let f64s: ArrayRef = Arc::new(Float64Array::from(values.clone()));
        let f32s: ArrayRef = Arc::new(Float32Array::from(
            values
                .iter()
                .map(|k| k.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ));
        for values in [f64s, f32s] {
            let dict: ArrayRef =
                Arc::new(DictionaryArray::try_new(keys.clone(), values).expect("a dictionary"));
            assert_eq!(
                group_rows(vec![dict]),
                (vec![vec![0, 1], vec![2, 3, 4], vec![5]], vec![6, 7])
            );
        }
    }
}
