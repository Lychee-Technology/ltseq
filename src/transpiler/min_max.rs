//! `min` and `max` in the contract's float order (§14.2, §9.4, §18; #253).
//!
//! DataFusion 55's float `min` and `max` give different answers on its three
//! paths:
//!
//! - The grouped accumulators compare with `partial_cmp` and let a NaN replace
//!   the running value, and the next value replace the NaN, so the result
//!   depends on where the NaNs fall in each batch: the grouped `min` of
//!   `[1.0, NaN, 2.0]` is `2.0`. They also start a group at the largest finite
//!   value, so the grouped `max` of `[-inf, -inf]` is `-f64::MAX`
//!   (apache/datafusion#24432; the fix is not in 55.x).
//! - The ungrouped and window accumulators order by IEEE totalOrder, which
//!   puts a NaN with the sign bit set, the NaN `inf - inf` gives on x86-64,
//!   below `-inf`.
//! - The `AggregateStatistics` rule answers an ungrouped `min` or `max` from
//!   Parquet statistics, which leave NaN out.
//!
//! In the contract's order every NaN, whatever its sign bit and payload, is
//! greater than `+inf`, and `-0.0` equals `0.0`. So `max` is NaN when a NaN
//! takes part and `min` is NaN only when every value is NaN. When the values
//! equal to the result differ in representation, the result is §18's
//! representative: `0.0` for the zeros, Python's `float("nan")` for the NaNs.
//! Otherwise it is that value, bit for bit. Either way the result is a
//! function of the values taking part, whatever the batches and partitions.
//!
//! [`min`] and [`max`] are the aggregates every plan uses, in `agg`,
//! `group_by`, `pivot`, the group dialect and windows. For a float argument
//! they compare values through their representatives, on which IEEE
//! totalOrder is the contract's order. They are never answered from
//! statistics, and a running one claims no order of its output. Every other
//! type keeps DataFusion's accumulators, its answers from statistics and its
//! claims. The session turns off the scan filter DataFusion plans for any
//! aggregate named `min` or `max` (`engine.rs`).

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray, PrimitiveArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Float16Type, Float32Type, Float64Type,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, Expr, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};

static MIN: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| udaf(Extreme::Min, min_udaf()));
static MAX: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| udaf(Extreme::Max, max_udaf()));

/// `min(expr)` as an aggregate expression.
pub(crate) fn min(expr: Expr) -> Expr {
    MIN.call(vec![expr])
}

/// `max(expr)` as an aggregate expression.
pub(crate) fn max(expr: Expr) -> Expr {
    MAX.call(vec![expr])
}

fn udaf(extreme: Extreme, datafusion: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::new_from_impl(MinMax {
        extreme,
        datafusion,
    }))
}

/// An Arrow float type, with the values the contract's order needs.
trait Float: ArrowPrimitiveType + Debug {
    /// §18's representative of the NaNs: the bits of Python's `float("nan")`.
    const NAN: Self::Native;
}

type Half = <Float16Type as ArrowPrimitiveType>::Native;

impl Float for Float16Type {
    const NAN: Half = Half::from_bits(0x7e00);
}

impl Float for Float32Type {
    const NAN: f32 = f32::from_bits(0x7fc0_0000);
}

impl Float for Float64Type {
    const NAN: f64 = f64::from_bits(0x7ff8_0000_0000_0000);
}

/// `value`'s representative (§18): `0.0` for both zeros, `T::NAN` for every
/// NaN, and `value` itself otherwise. Two values are equal in the contract
/// exactly when their representatives have the same bits.
fn representative<T: Float>(value: T::Native) -> T::Native {
    // A NaN is the one value with no order to itself; `-0.0 == 0.0`.
    if value.partial_cmp(&value).is_none() {
        T::NAN
    } else if value == T::Native::ZERO {
        T::Native::ZERO
    } else {
        value
    }
}

/// `a` against `b` in the contract's order. On representatives, which hold
/// neither `-0.0` nor a NaN other than `T::NAN`, IEEE totalOrder is that order.
fn contract_cmp<T: Float>(a: T::Native, b: T::Native) -> Ordering {
    representative::<T>(a).compare(representative::<T>(b))
}

fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Extreme {
    Min,
    Max,
}

impl Extreme {
    /// The side of the order this extreme takes.
    fn better(self) -> Ordering {
        match self {
            Self::Max => Ordering::Greater,
            Self::Min => Ordering::Less,
        }
    }

    /// The result over the values behind `current` and `value`. A value
    /// equal to `current` with other bits makes the result the
    /// representative, so the fold gives the same bits in any order and
    /// grouping of its values.
    fn pick<T: Float>(self, current: T::Native, value: T::Native) -> T::Native {
        match contract_cmp::<T>(value, current) {
            Ordering::Equal if !value.is_eq(current) => representative::<T>(value),
            ordering if ordering == self.better() => value,
            _ => current,
        }
    }
}

/// DataFusion's `min` or `max` with float arguments taken over.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MinMax {
    extreme: Extreme,
    /// DataFusion's aggregate of the same name, which keeps every other type.
    datafusion: Arc<AggregateUDF>,
}

impl MinMax {
    fn datafusion(&self) -> &dyn AggregateUDFImpl {
        self.datafusion.inner().as_ref()
    }
}

impl AggregateUDFImpl for MinMax {
    fn name(&self) -> &str {
        self.datafusion().name()
    }

    fn signature(&self) -> &Signature {
        self.datafusion().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.datafusion().return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.datafusion().coerce_types(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.datafusion().state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let (extreme, data_type) = (self.extreme, args.return_field.data_type());
        Ok(match data_type {
            DataType::Float16 => Box::new(FloatAccumulator::<Float16Type>::new(extreme, data_type)),
            DataType::Float32 => Box::new(FloatAccumulator::<Float32Type>::new(extreme, data_type)),
            DataType::Float64 => Box::new(FloatAccumulator::<Float64Type>::new(extreme, data_type)),
            _ => return self.datafusion().accumulator(args),
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        is_float(args.return_field.data_type())
            || self.datafusion().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let (extreme, data_type) = (self.extreme, args.return_field.data_type());
        Ok(match data_type {
            DataType::Float16 => Box::new(FloatGroupsAccumulator::<Float16Type>::new(
                extreme, data_type,
            )),
            DataType::Float32 => Box::new(FloatGroupsAccumulator::<Float32Type>::new(
                extreme, data_type,
            )),
            DataType::Float64 => Box::new(FloatGroupsAccumulator::<Float64Type>::new(
                extreme, data_type,
            )),
            _ => return self.datafusion().create_groups_accumulator(args),
        })
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let (extreme, data_type) = (self.extreme, args.return_field.data_type());
        Ok(match data_type {
            DataType::Float16 => Box::new(SlidingFloatAccumulator::<Float16Type>::new(
                extreme, data_type,
            )),
            DataType::Float32 => Box::new(SlidingFloatAccumulator::<Float32Type>::new(
                extreme, data_type,
            )),
            DataType::Float64 => Box::new(SlidingFloatAccumulator::<Float64Type>::new(
                extreme, data_type,
            )),
            _ => return self.datafusion().create_sliding_accumulator(args),
        })
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.datafusion().order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.datafusion().reverse_expr()
    }

    /// `None`, so DataFusion never swaps in a plan of its own for this
    /// aggregate. DataFusion 55 has one, the TopK aggregation of `GROUP BY ..
    /// ORDER BY max(..) LIMIT n`, which compares floats in its own order;
    /// other types lose that rewrite with it.
    fn is_descending(&self) -> Option<bool> {
        None
    }

    fn value_from_stats(&self, args: &StatisticsArgs) -> Option<ScalarValue> {
        // Parquet statistics leave NaN out of a float column's min and max.
        if is_float(args.return_type) {
            return None;
        }
        self.datafusion().value_from_stats(args)
    }

    /// DataFusion reads this as an order of the window's output in its own
    /// sort order, IEEE totalOrder for floats, and drops a later sort it
    /// satisfies. A float running extreme is monotonic only in the
    /// contract's order: a running `max` of `[1.0, -NaN]` is `-NaN`, below
    /// `1.0` in totalOrder, and a representative can sort below the bits it
    /// replaces. So floats claim none.
    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        if is_float(data_type) {
            return SetMonotonicity::NotMonotonic;
        }
        self.datafusion().set_monotonicity(data_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.datafusion().documentation()
    }
}

/// The rows of `values` that take part: non-NULL, and TRUE in `filter`.
fn taking_part(values: &dyn Array, filter: Option<&BooleanArray>) -> Option<NullBuffer> {
    let filter = filter.map(|filter| {
        let (bits, nulls) = filter.clone().into_parts();
        NullBuffer::new(match nulls {
            Some(nulls) => nulls.inner() & &bits,
            None => bits,
        })
    });
    NullBuffer::union(filter.as_ref(), values.nulls())
}

/// `min` or `max` of a float argument without groups, or over a window frame
/// that only grows.
#[derive(Debug)]
struct FloatAccumulator<T: Float> {
    extreme: Extreme,
    data_type: DataType,
    value: Option<T::Native>,
}

impl<T: Float> FloatAccumulator<T> {
    fn new(extreme: Extreme, data_type: &DataType) -> Self {
        Self {
            extreme,
            data_type: data_type.clone(),
            value: None,
        }
    }
}

impl<T: Float> Accumulator for FloatAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for value in values[0].as_primitive::<T>().iter().flatten() {
            self.value = Some(match self.value {
                Some(current) => self.extreme.pick::<T>(current, value),
                None => value,
            });
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// `min` or `max` of a float argument for every group. A group with no value
/// taking part is NULL.
#[derive(Debug)]
struct FloatGroupsAccumulator<T: Float> {
    extreme: Extreme,
    data_type: DataType,
    values: Vec<T::Native>,
    seen: Vec<bool>,
}

impl<T: Float> FloatGroupsAccumulator<T> {
    fn new(extreme: Extreme, data_type: &DataType) -> Self {
        Self {
            extreme,
            data_type: data_type.clone(),
            values: Vec::new(),
            seen: Vec::new(),
        }
    }
}

impl<T: Float> GroupsAccumulator for FloatGroupsAccumulator<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.values.resize(total_num_groups, T::Native::default());
        self.seen.resize(total_num_groups, false);
        let values = values[0].as_primitive::<T>();
        let taking_part = taking_part(values, opt_filter);
        for (row, &group) in group_indices.iter().enumerate() {
            if taking_part.as_ref().is_some_and(|rows| rows.is_null(row)) {
                continue;
            }
            let value = values.value(row);
            self.values[group] = if self.seen[group] {
                self.extreme.pick::<T>(self.values[group], value)
            } else {
                value
            };
            self.seen[group] = true;
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let seen = NullBuffer::from(emit_to.take_needed(&mut self.seen));
        let values = PrimitiveArray::<T>::new(values.into(), Some(seen));
        Ok(Arc::new(values.with_data_type(self.data_type.clone())))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    /// A row's state is its value, or NULL where it does not take part.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T>();
        let nulls = taking_part(values, opt_filter);
        let (data_type, buffer, _) = values.clone().into_parts();
        let state = PrimitiveArray::<T>::new(buffer, nulls).with_data_type(data_type);
        Ok(vec![Arc::new(state)])
    }

    fn size(&self) -> usize {
        self.values.capacity() * size_of::<T::Native>() + self.seen.capacity()
    }
}

/// `min` or `max` of a float argument over a sliding window frame
/// (`rolling`). Values leave the frame in the order they entered it, so the
/// frame's candidates are a monotonic deque of runs: a value ends every run
/// behind it that it is better than, and joins the last run if it has the
/// same bits. Each run holds the positions, among the non-NULL values, of
/// the values it stands for. The first run is the frame's extreme; the values
/// equal to it differ in representation exactly when the second run is equal
/// to it too, since adjacent runs differ in bits.
#[derive(Debug)]
struct SlidingFloatAccumulator<T: Float> {
    extreme: Extreme,
    data_type: DataType,
    runs: VecDeque<Run<T::Native>>,
    entered: u64,
    left: u64,
}

#[derive(Debug)]
struct Run<N> {
    first: u64,
    last: u64,
    value: N,
}

impl<T: Float> SlidingFloatAccumulator<T> {
    fn new(extreme: Extreme, data_type: &DataType) -> Self {
        Self {
            extreme,
            data_type: data_type.clone(),
            runs: VecDeque::new(),
            entered: 0,
            left: 0,
        }
    }
}

impl<T: Float> Accumulator for SlidingFloatAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let better = self.extreme.better();
        for value in values[0].as_primitive::<T>().iter().flatten() {
            while self
                .runs
                .back()
                .is_some_and(|run| contract_cmp::<T>(value, run.value) == better)
            {
                self.runs.pop_back();
            }
            match self.runs.back_mut() {
                Some(run) if run.value.is_eq(value) => run.last = self.entered,
                _ => self.runs.push_back(Run {
                    first: self.entered,
                    last: self.entered,
                    value,
                }),
            }
            self.entered += 1;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // NULLs never entered, so only the non-NULL values leave.
        let leaving = values[0].len() - values[0].logical_null_count();
        for _ in 0..leaving {
            if let Some(run) = self.runs.front_mut() {
                if run.first == self.left {
                    if run.first == run.last {
                        self.runs.pop_front();
                    } else {
                        run.first += 1;
                    }
                }
            }
            self.left += 1;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.runs.front().map(|extreme| match self.runs.get(1) {
            Some(next) if contract_cmp::<T>(next.value, extreme.value).is_eq() => {
                representative::<T>(extreme.value)
            }
            _ => extreme.value,
        });
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.runs.capacity() * size_of::<Run<T::Native>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float32Array, Float64Array};

    const NEG_NAN: f64 = f64::from_bits(0xfff8_0000_0000_0000);
    const PAYLOAD_NAN: f64 = f64::from_bits(0x7ff8_0000_0000_0001);
    const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

    /// The contract's answer, computed without representatives or
    /// totalOrder. `max` is NaN when a NaN takes part, `min` only when every
    /// value is NaN; the values equal to the answer keep their bits when they
    /// all have the same bits, and are `0.0` or `float("nan")` otherwise.
    fn expected(extreme: Extreme, values: &[Option<f64>]) -> Option<u64> {
        let present: Vec<f64> = values.iter().flatten().copied().collect();
        let (nans, numbers): (Vec<f64>, Vec<f64>) = present.iter().partition(|v| v.is_nan());
        let equal = match extreme {
            Extreme::Max if !nans.is_empty() => nans,
            Extreme::Min if numbers.is_empty() => nans,
            _ => {
                let pick = match extreme {
                    Extreme::Max => f64::max,
                    Extreme::Min => f64::min,
                };
                let value = numbers.iter().copied().reduce(pick)?;
                numbers.into_iter().filter(|v| *v == value).collect()
            }
        };
        let first = equal.first()?.to_bits();
        if equal.iter().all(|v| v.to_bits() == first) {
            Some(first)
        } else if equal[0].is_nan() {
            Some(CANONICAL_NAN)
        } else {
            Some(0.0_f64.to_bits())
        }
    }

    fn bits(value: ScalarValue) -> Option<u64> {
        match value {
            ScalarValue::Float64(v) => v.map(f64::to_bits),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    fn array(values: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    /// Inputs where the NaNs, the zeros and the infinities meet each other
    /// and NULL, in several orders.
    fn cases() -> Vec<Vec<Option<f64>>> {
        let mut cases = vec![
            vec![],
            vec![None, None],
            vec![Some(f64::NAN), Some(NEG_NAN)],
            vec![Some(NEG_NAN), Some(NEG_NAN)],
            vec![Some(f64::NEG_INFINITY), Some(f64::NEG_INFINITY)],
            vec![Some(f64::INFINITY)],
            vec![Some(-0.0), Some(0.0), Some(-0.0)],
            vec![Some(-0.0), None, Some(-0.0)],
            vec![Some(PAYLOAD_NAN), None, Some(f64::INFINITY), Some(-1.0)],
        ];
        let values = [Some(1.0), Some(f64::NAN), Some(2.0)];
        for nan in [f64::NAN, NEG_NAN, PAYLOAD_NAN] {
            for order in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                cases.push(
                    order
                        .iter()
                        .map(|&i| if i == 1 { Some(nan) } else { values[i] })
                        .collect(),
                );
            }
        }
        cases
    }

    #[test]
    fn the_reference_reads_the_contract() {
        let max = |values: &[Option<f64>]| expected(Extreme::Max, values);
        let min = |values: &[Option<f64>]| expected(Extreme::Min, values);
        assert_eq!(
            max(&[Some(1.0), Some(NEG_NAN), Some(2.0)]),
            Some(NEG_NAN.to_bits())
        );
        assert_eq!(
            min(&[Some(1.0), Some(NEG_NAN), Some(2.0)]),
            Some(1.0_f64.to_bits())
        );
        assert_eq!(min(&[Some(NEG_NAN), Some(f64::NAN)]), Some(CANONICAL_NAN));
        assert_eq!(max(&[Some(-0.0), Some(0.0)]), Some(0));
        assert_eq!(max(&[Some(-0.0), Some(-0.0)]), Some((-0.0_f64).to_bits()));
        assert_eq!(max(&[None]), None);
    }

    #[test]
    fn representative_is_one_zero_and_one_nan() {
        assert_eq!(representative::<Float64Type>(-0.0).to_bits(), 0);
        assert_eq!(
            representative::<Float64Type>(NEG_NAN).to_bits(),
            CANONICAL_NAN
        );
        assert_eq!(
            representative::<Float64Type>(PAYLOAD_NAN).to_bits(),
            CANONICAL_NAN
        );
        assert_eq!(representative::<Float64Type>(-2.5), -2.5);
        assert_eq!(representative::<Float32Type>(-0.0_f32).to_bits(), 0);
        assert_eq!(
            representative::<Float32Type>(f32::from_bits(0xffc0_0001)).to_bits(),
            0x7fc0_0000
        );
        assert_eq!(
            representative::<Float16Type>(Half::from_bits(0x8000)).to_bits(),
            0
        );
        assert_eq!(
            representative::<Float16Type>(Half::from_bits(0xfe01)).to_bits(),
            0x7e00
        );
    }

    #[test]
    fn ungrouped_follows_the_contract_in_any_batching() {
        for extreme in [Extreme::Min, Extreme::Max] {
            for case in cases() {
                // One batch, then one batch per value merged from separate states.
                let mut one = FloatAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
                one.update_batch(&[array(&case)]).unwrap();
                assert_eq!(
                    bits(one.evaluate().unwrap()),
                    expected(extreme, &case),
                    "{extreme:?} {case:?}"
                );

                let mut merged = FloatAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
                for value in case.iter().rev() {
                    let mut part =
                        FloatAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
                    part.update_batch(&[array(&[*value])]).unwrap();
                    let state = part.state().unwrap()[0].to_array().unwrap();
                    merged.merge_batch(&[state]).unwrap();
                }
                assert_eq!(
                    bits(merged.evaluate().unwrap()),
                    expected(extreme, &case),
                    "{extreme:?} {case:?} merged"
                );
            }
        }
    }

    #[test]
    fn grouped_follows_the_contract_in_any_batching() {
        let cases = cases();
        let longest = cases.iter().map(Vec::len).max().unwrap();
        let all: Vec<usize> = (0..cases.len()).collect();
        for extreme in [Extreme::Min, Extreme::Max] {
            // Every case is a group whose rows arrive interleaved, one batch
            // per row position. A second accumulator merges the first one's
            // state; a third merges each row's state from `convert_to_state`.
            let mut grouped =
                FloatGroupsAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
            let mut from_rows =
                FloatGroupsAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
            for position in 0..longest {
                let (values, groups): (Vec<Option<f64>>, Vec<usize>) = cases
                    .iter()
                    .enumerate()
                    .filter_map(|(group, case)| case.get(position).map(|v| (*v, group)))
                    .unzip();
                grouped
                    .update_batch(&[array(&values)], &groups, None, cases.len())
                    .unwrap();
                let rows = from_rows.convert_to_state(&[array(&values)], None).unwrap();
                from_rows.merge_batch(&rows, &groups, cases.len()).unwrap();
            }
            let state = grouped.state(EmitTo::All).unwrap();
            let mut merged =
                FloatGroupsAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
            merged.merge_batch(&state, &all, cases.len()).unwrap();
            for accumulator in [&mut merged, &mut from_rows] {
                let result = accumulator.evaluate(EmitTo::All).unwrap();
                let result = result.as_primitive::<Float64Type>();
                for (group, case) in cases.iter().enumerate() {
                    let actual = result
                        .is_valid(group)
                        .then(|| result.value(group).to_bits());
                    assert_eq!(actual, expected(extreme, case), "{extreme:?} {case:?}");
                }
            }
        }
    }

    #[test]
    fn grouped_rows_outside_the_filter_take_no_part() {
        let values = array(&[Some(1.0), Some(f64::NAN), Some(2.0), Some(3.0)]);
        let filter = BooleanArray::from(vec![Some(true), Some(false), Some(true), None]);
        let groups = [0, 0, 0, 1];
        let mut grouped =
            FloatGroupsAccumulator::<Float64Type>::new(Extreme::Max, &DataType::Float64);
        grouped
            .update_batch(&[Arc::clone(&values)], &groups, Some(&filter), 2)
            .unwrap();
        let rows = grouped.convert_to_state(&[values], Some(&filter)).unwrap();
        let mut from_rows =
            FloatGroupsAccumulator::<Float64Type>::new(Extreme::Max, &DataType::Float64);
        from_rows.merge_batch(&rows, &groups, 2).unwrap();
        for accumulator in [&mut grouped, &mut from_rows] {
            let result = accumulator.evaluate(EmitTo::All).unwrap();
            let result = result.as_primitive::<Float64Type>();
            assert_eq!(result.value(0), 2.0);
            assert!(result.is_null(1));
        }
    }

    #[test]
    fn sliding_follows_the_contract_in_every_frame() {
        let rows = [
            Some(1.0),
            Some(NEG_NAN),
            Some(2.0),
            None,
            Some(-0.0),
            Some(0.0),
            Some(-0.0),
            Some(-0.0),
            Some(f64::NAN),
            Some(3.0),
            None,
            Some(NEG_NAN),
            Some(NEG_NAN),
            Some(f64::NEG_INFINITY),
            Some(PAYLOAD_NAN),
            Some(-0.0),
        ];
        for extreme in [Extreme::Min, Extreme::Max] {
            for width in 1..=6 {
                let mut sliding =
                    SlidingFloatAccumulator::<Float64Type>::new(extreme, &DataType::Float64);
                for (row, value) in rows.iter().enumerate() {
                    sliding.update_batch(&[array(&[*value])]).unwrap();
                    if row >= width {
                        sliding
                            .retract_batch(&[array(&[rows[row - width]])])
                            .unwrap();
                    }
                    let frame = &rows[(row + 1).saturating_sub(width)..=row];
                    assert_eq!(
                        bits(sliding.evaluate().unwrap()),
                        expected(extreme, frame),
                        "{extreme:?} width {width} row {row}"
                    );
                }
            }
        }
    }

    #[test]
    fn only_non_floats_claim_an_order_of_a_running_extreme() {
        for (udaf, claim) in [
            (&*MIN, SetMonotonicity::Decreasing),
            (&*MAX, SetMonotonicity::Increasing),
        ] {
            let monotonicity = |data_type| udaf.inner().set_monotonicity(&data_type);
            for float in [DataType::Float16, DataType::Float32, DataType::Float64] {
                assert_eq!(monotonicity(float), SetMonotonicity::NotMonotonic);
            }
            assert_eq!(monotonicity(DataType::Int64), claim);
            assert_eq!(monotonicity(DataType::Utf8), claim);
        }
    }

    #[test]
    fn float32_keeps_its_type() {
        let values: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(1.0),
            Some(f32::from_bits(0xffc0_0000)),
            None,
        ]));
        let mut max = FloatAccumulator::<Float32Type>::new(Extreme::Max, &DataType::Float32);
        max.update_batch(&[Arc::clone(&values)]).unwrap();
        let mut min = FloatAccumulator::<Float32Type>::new(Extreme::Min, &DataType::Float32);
        min.update_batch(&[values]).unwrap();
        match (max.evaluate().unwrap(), min.evaluate().unwrap()) {
            (ScalarValue::Float32(Some(max)), ScalarValue::Float32(Some(min))) => {
                assert_eq!(max.to_bits(), 0xffc0_0000);
                assert_eq!(min, 1.0);
            }
            other => panic!("expected two Float32 values, got {other:?}"),
        }
    }
}
